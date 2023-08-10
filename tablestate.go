// Copyright 2023 Rivian Automotive, Inc.
// Licensed under the Apache License, Version 2.0 (the “License”);
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an “AS IS” BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package delta

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"time"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/apache/arrow/go/v13/parquet/file"
	"github.com/apache/arrow/go/v13/parquet/pqarrow"
	"github.com/rivian/delta-go/internal/perf"
	"github.com/rivian/delta-go/storage"
)

type DeltaTableState[RowType any, PartitionType any] struct {
	// current table version represented by this table state
	Version int64
	// A remove action should remain in the state of the table as a tombstone until it has expired.
	// A tombstone expires when the creation timestamp of the delta file exceeds the expiration
	Tombstones map[string]Remove
	// active files for table state
	Files map[string]AddPartitioned[RowType, PartitionType]
	// Information added to individual commits
	CommitInfos           []CommitInfo
	AppTransactionVersion map[string]int64
	MinReaderVersion      int32
	MinWriterVersion      int32
	// table metadata corresponding to current version
	CurrentMetadata *DeltaTableMetaData
	// retention period for tombstones as time.Duration (nanoseconds)
	TombstoneRetention time.Duration
	// retention period for log entries as time.Duration (nanoseconds)
	LogRetention            time.Duration
	EnableExpiredLogCleanup bool
	// Additional state for on-disk optimizations for large checkpoints
	onDiskOptimization bool
	OnDiskTableState
}

var (
	ErrorMissingMetadata          error = errors.New("missing metadata")
	ErrorConvertingCheckpointAdd  error = errors.New("unable to generate checkpoint add")
	ErrorCDCNotSupported          error = errors.New("cdc is not supported")
	ErrorDeleteVectorNotSupported error = errors.New("delete vectors are not supported")
	ErrorGeneratingCheckpoint     error = errors.New("unable to write checkpoint to buffer")
	ErrorReadingCheckpoint        error = errors.New("unable to read checkpoint")
	ErrorVersionOutOfOrder        error = errors.New("versions out of order during update")
	ErrorUnexpectedSchemaFailure  error = errors.New("unexpected error converting schema")
)

// / Create an empty table state for the given version
func NewDeltaTableState[RowType any, PartitionType any](version int64) *DeltaTableState[RowType, PartitionType] {
	tableState := new(DeltaTableState[RowType, PartitionType])
	tableState.Version = version
	tableState.Files = make(map[string]AddPartitioned[RowType, PartitionType])
	tableState.Tombstones = make(map[string]Remove)
	tableState.AppTransactionVersion = make(map[string]int64)
	// Default 7 days
	tableState.TombstoneRetention = time.Hour * 24 * 7
	// Default 30 days
	tableState.LogRetention = time.Hour * 24 * 30
	tableState.EnableExpiredLogCleanup = false
	return tableState
}

func (tableState *DeltaTableState[RowType, PartitionType]) FileCount() int {
	if tableState.onDiskOptimization {
		return tableState.onDiskFileCount
	}
	return len(tableState.Files)
}

func (tableState *DeltaTableState[RowType, PartitionType]) TombstoneCount() int {
	if tableState.onDiskOptimization {
		return tableState.onDiskTombstoneCount
	}
	return len(tableState.Tombstones)
}

// / Get a configuration value from the table state, or return the default value if the configuration option is not present
func (tableState *DeltaTableState[RowType, PartitionType]) ConfigurationOrDefault(configKey DeltaConfigKey, defaultValue string) string {
	if tableState.CurrentMetadata == nil || tableState.CurrentMetadata.Configuration == nil {
		return defaultValue
	}
	value, ok := tableState.CurrentMetadata.Configuration[string(configKey)]
	if !ok {
		return defaultValue
	}
	return value
}

// / Generate a table state from a specific commit version
func NewDeltaTableStateFromCommit[RowType any, PartitionType any](table *DeltaTable[RowType, PartitionType], version int64) (*DeltaTableState[RowType, PartitionType], error) {
	actions, err := table.ReadCommitVersion(version)
	if err != nil {
		return nil, err
	}
	return NewDeltaTableStateFromActions[RowType, PartitionType](actions, version)
}

// / Generate a table state from a list of actions
func NewDeltaTableStateFromActions[RowType any, PartitionType any](actions []Action, version int64) (*DeltaTableState[RowType, PartitionType], error) {
	tableState := NewDeltaTableState[RowType, PartitionType](version)
	for _, action := range actions {
		err := tableState.processAction(action)
		if err != nil {
			return nil, err
		}
	}
	return tableState, nil
}

// / Update the table state by applying a single action
func (tableState *DeltaTableState[RowType, PartitionType]) processAction(actionInterface Action) error {
	switch action := actionInterface.(type) {
	case *AddPartitioned[RowType, PartitionType]:
		tableState.Files[action.Path] = *action
	case *Add[RowType]:
		// We're using the AddPartitioned type for storing our list of added files, so need to translate the type here
		add := new(AddPartitioned[RowType, PartitionType])
		// Copy details
		add.fromAdd(action)
		tableState.Files[action.Path] = *add
	case *Remove:
		// TODO - do we need to decode as in delta-rs?
		tableState.Tombstones[action.Path] = *action
	case *MetaData:
		if action.Configuration != nil {
			// Parse the configuration options that we make use of
			option, ok := action.Configuration[string(DeletedFileRetentionDurationDeltaConfigKey)]
			if ok {
				duration, err := ParseInterval(option)
				if err != nil {
					return err
				}
				tableState.TombstoneRetention = duration
			}
			option, ok = action.Configuration[string(LogRetentionDurationDeltaConfigKey)]
			if ok {
				duration, err := ParseInterval(option)
				if err != nil {
					return err
				}
				tableState.LogRetention = duration
			}
			option, ok = action.Configuration[string(EnableExpiredLogCleanupDeltaConfigKey)]
			if ok {
				boolOption, err := strconv.ParseBool(option)
				if err != nil {
					return err
				}
				tableState.EnableExpiredLogCleanup = boolOption
			}
		}
		deltaTableMetadata, err := action.ToDeltaTableMetaData()
		if err != nil {
			return err
		}
		tableState.CurrentMetadata = &deltaTableMetadata
	case *Txn:
		tableState.AppTransactionVersion[action.AppId] = action.Version
	case *Protocol:
		tableState.MinReaderVersion = action.MinReaderVersion
		tableState.MinWriterVersion = action.MinWriterVersion
	case *CommitInfo:
		tableState.CommitInfos = append(tableState.CommitInfos, *action)
	case *Cdc:
		return ErrorCDCNotSupported
	default:
		return errors.Join(ErrorActionUnknown, fmt.Errorf("unknown %v", action))
	}
	return nil
}

// / Merges new state information into our state
func (tableState *DeltaTableState[RowType, PartitionType]) merge(newTableState *DeltaTableState[RowType, PartitionType], maxRowsPerPart int, config *ReadWriteCheckpointConfiguration, finalMerge bool) error {
	var err error

	if tableState.onDiskOptimization {
		err = tableState.mergeOnDiskState(newTableState, maxRowsPerPart, config, finalMerge)
		if err != nil {
			return err
		}
		// the final merge is to resolve pending adds/tombstones and does not include a new table state
		if finalMerge {
			return nil
		}
	}

	// In memory file updates
	for k, v := range newTableState.Tombstones {
		// Remove deleted files from existing added files
		delete(tableState.Files, k)
		// Add deleted file tombstones to state so they're available for vacuum
		tableState.Tombstones[k] = v
	}
	for k, v := range newTableState.Files {
		// If files were deleted and then re-added, remove from updated tombstones
		delete(tableState.Tombstones, k)
		tableState.Files[k] = v
	}

	if newTableState.MinReaderVersion > 0 {
		tableState.MinReaderVersion = newTableState.MinReaderVersion
		tableState.MinWriterVersion = newTableState.MinWriterVersion
	}

	if newTableState.CurrentMetadata != nil {
		tableState.TombstoneRetention = newTableState.TombstoneRetention
		tableState.LogRetention = newTableState.LogRetention
		tableState.EnableExpiredLogCleanup = newTableState.EnableExpiredLogCleanup
		tableState.CurrentMetadata = newTableState.CurrentMetadata
	}

	for k, v := range newTableState.AppTransactionVersion {
		tableState.AppTransactionVersion[k] = v
	}

	tableState.CommitInfos = append(tableState.CommitInfos, newTableState.CommitInfos...)

	if newTableState.Version <= tableState.Version {
		return ErrorVersionOutOfOrder
	}
	tableState.Version = newTableState.Version

	return nil
}

func stateFromCheckpoint[RowType any, PartitionType any](table *DeltaTable[RowType, PartitionType], checkpoint *CheckPoint, config *ReadWriteCheckpointConfiguration) (*DeltaTableState[RowType, PartitionType], error) {
	newState := NewDeltaTableState[RowType, PartitionType](checkpoint.Version)
	checkpointDataPaths := table.GetCheckpointDataPaths(checkpoint)
	if config != nil && config.WorkingStore != nil {
		newState.onDiskOptimization = true
		newState.onDiskTempFiles = make([]storage.Path, 0, len(checkpointDataPaths))
		newState.ctx = config.Ctx
		newState.redisClient = config.RedisClient
		if newState.redisClient == nil {
			return nil, errors.New("need a redis client")
		}
	}

	// Optional concurrency support
	if newState.onDiskOptimization && config.ConcurrentCheckpointRead > 1 {
		err := newState.applyCheckpointConcurrently(table.Store, checkpointDataPaths, config)
		if err != nil {
			return nil, err
		}
	} else {
		// No concurrency
		for i, location := range checkpointDataPaths {
			task := checkpointProcessingTask[RowType, PartitionType]{location: location, state: newState, part: i, config: config, store: table.Store}
			err := stateFromCheckpointPart(task)
			if err != nil {
				return nil, err
			}
		}
	}

	return newState, nil
}

type checkpointProcessingTask[RowType any, PartitionType any] struct {
	location storage.Path
	state    *DeltaTableState[RowType, PartitionType]
	part     int
	config   *ReadWriteCheckpointConfiguration
	store    storage.ObjectStore
}

func stateFromCheckpointPart[RowType any, PartitionType any](task checkpointProcessingTask[RowType, PartitionType]) error {
	checkpointBytes, err := task.store.Get(&task.location)
	if err != nil {
		return err
	}
	if len(checkpointBytes) > 0 {
		err = task.state.processCheckpointBytes(checkpointBytes, task.part, task.config)
		if err != nil {
			return err
		}
	} else {
		return errors.Join(ErrorCheckpointIncomplete, fmt.Errorf("zero size checkpoint at %s", task.location.Raw))
	}
	return nil
}

func isPartitionTypeEmpty[PartitionType any]() bool {
	testPartitionItem := new(PartitionType)
	structType := reflect.TypeOf(*testPartitionItem)
	return structType.NumField() == 0
}

func (tableState *DeltaTableState[RowType, PartitionType]) processCheckpointBytes(checkpointBytes []byte, part int, config *ReadWriteCheckpointConfiguration) (returnErr error) {
	// Determine whether partitioned
	isPartitioned := !isPartitionTypeEmpty[PartitionType]()
	if isPartitioned {
		return processCheckpointBytesWithAddSpecified[RowType, PartitionType, AddPartitioned[RowType, PartitionType]](checkpointBytes, tableState, part, config)
	} else {
		return processCheckpointBytesWithAddSpecified[RowType, PartitionType, Add[RowType]](checkpointBytes, tableState, part, config)
	}
}

// / Update a table state with the contents of a checkpoint file
func processCheckpointBytesWithAddSpecified[RowType any, PartitionType any, AddType AddPartitioned[RowType, PartitionType] | Add[RowType]](checkpointBytes []byte, tableState *DeltaTableState[RowType, PartitionType], part int, config *ReadWriteCheckpointConfiguration) error {
	defer perf.TrackTime(time.Now(), "processCheckpointBytesWithAddSpecified")
	perf.SnapshotMemory("processCheckpointBytesWithAddSpecified beginning")

	concurrentCheckpointRead := tableState.onDiskOptimization && config.ConcurrentCheckpointRead > 1
	var processFunc = func(checkpointEntry *CheckpointEntry[RowType, PartitionType, AddType]) error {
		var action Action
		// if checkpointEntry.Add != nil {
		// 	action = checkpointEntry.Add
		// }
		// if checkpointEntry.Remove != nil {
		// 	action = checkpointEntry.Remove
		// }
		if checkpointEntry.MetaData != nil {
			action = checkpointEntry.MetaData
		}
		if checkpointEntry.Protocol != nil {
			action = checkpointEntry.Protocol
		}
		if checkpointEntry.Txn != nil {
			action = checkpointEntry.Txn
		}

		if action != nil {
			if concurrentCheckpointRead {
				tableState.concurrentUpdateMutex.Lock()
				defer tableState.concurrentUpdateMutex.Unlock()
			}
			err := tableState.processAction(action)
			if err != nil {
				return err
			}
		} else {
			if !tableState.onDiskOptimization {
				// This is expected during optimized on-disk reading but not otherwise
				return errors.New("no action found in checkpoint record")
			}
		}
		return nil
	}

	bytesReader := bytes.NewReader(checkpointBytes)
	parquetReader, err := file.NewParquetReader(bytesReader)
	if err != nil {
		return err
	}

	defaultValue := new(CheckpointEntry[RowType, PartitionType, AddType])
	// parquetSchema := parquetReader.MetaData().Schema
	fileReader, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{BatchSize: 10, Parallel: true}, memory.DefaultAllocator)
	if err != nil {
		return err
	}
	arrowSchema, err := fileReader.Schema()
	if err != nil {
		return err
	}
	arrowFieldList := arrowSchema.Fields()

	// Get mappings between struct member names and parquet/arrow names so we don't have to look them up repeatedly
	// during record assignments
	inMemoryIndexMappings := make(map[string]int, 100)
	defaultType := reflect.TypeOf(defaultValue)
	var fieldExclusions []string
	if tableState.onDiskOptimization {
		fieldExclusions = []string{"Root.Add", "Root.Remove"}
	}
	err = getStructFieldNameToArrowIndexMappings(defaultType, "Root", arrowFieldList, fieldExclusions, inMemoryIndexMappings)
	if err != nil {
		return err
	}

	tbl, err := fileReader.ReadTable(context.TODO())
	if err != nil {
		return err
	}
	defer tbl.Release()
	tableReader := array.NewTableReader(tbl, 0)
	defer tableReader.Release()

	recordCount := 0
	for tableReader.Next() {
		// the record contains a batch of rows
		record := tableReader.Record()

		if tableState.onDiskOptimization {
			err = initializeOnDiskRecord(tableState, record, recordCount, int32(part), arrowSchema)
			if err != nil {
				return err
			}
		}

		entries := make([]*CheckpointEntry[RowType, PartitionType, AddType], record.NumRows())
		entryValues := make([]reflect.Value, record.NumRows())
		for j := int64(0); j < record.NumRows(); j++ {
			t := new(CheckpointEntry[RowType, PartitionType, AddType])
			entries[j] = t
			entryValues[j] = reflect.ValueOf(t)
		}

		inMemoryColumns := make([]arrow.Array, 0, 3)
		for i := 0; i < int(record.NumCols()); i++ {
			c := record.ColumnName(i)
			if c == "txn" || c == "metaData" || c == "protocol" {
				inMemoryColumns = append(inMemoryColumns, record.Column(i))
			}
		}
		err = func() error {
			defer perf.TrackTime(time.Now(), "reading goStructFromArrowArrays")
			return goStructFromArrowArrays(entryValues, inMemoryColumns, "Root", inMemoryIndexMappings, 0)
		}()
		if err != nil {
			return err
		}
		err = func() error {
			defer perf.TrackTime(time.Now(), "reading processFunc")
			for j := int64(0); j < record.NumRows(); j++ {
				err := processFunc(entries[j])
				if err != nil {
					return err
				}
			}
			return nil
		}()
		if err != nil {
			return err
		}
		recordCount++
	}

	if tableState.onDiskOptimization {
		// The non-add, non-remove columns will be almost entirely nulls, so picking out just add and remove
		// slows us down here for a very minimal improvement in file size.
		// Instead we just write out the entire file.
		onDiskFile := storage.PathFromIter([]string{config.WorkingFolder.Raw, fmt.Sprintf("intermediate.%d.parquet", part)})
		perf.TrackOperationThreadSafe("finalization.processCheckpointBytesWithAddSpecified.write")
		config.WorkingStore.Put(&onDiskFile, checkpointBytes)
		tableState.onDiskTempFiles = append(tableState.onDiskTempFiles, onDiskFile)
	}
	perf.SnapshotMemory("processCheckpointBytesWithAddSpecified end")

	return nil
}

// / Prepare the table state for checkpointing by updating tombstones
func (tableState *DeltaTableState[RowType, PartitionType]) prepareStateForCheckpoint(config *ReadWriteCheckpointConfiguration) error {
	defer perf.TrackTime(time.Now(), "prepareStateForCheckpoint")
	perf.SnapshotMemory("prepareStateForCheckpoint beginning")

	if tableState.CurrentMetadata == nil {
		return ErrorMissingMetadata
	}

	retentionTimestamp := time.Now().UnixMilli() - tableState.TombstoneRetention.Milliseconds()

	if tableState.onDiskOptimization {
		return tableState.prepareOnDiskStateForCheckpoint(retentionTimestamp, config)

	}

	// Don't keep expired tombstones
	// Also check if any of the non-expired Remove actions had ExtendedFileMetadata = false
	doNotUseExtendedFileMetadata := false
	unexpiredTombstones := make(map[string]Remove, len(tableState.Tombstones))
	for path, remove := range tableState.Tombstones {
		if remove.DeletionTimestamp == nil || *remove.DeletionTimestamp > retentionTimestamp {
			unexpiredTombstones[path] = remove
			doNotUseExtendedFileMetadata = doNotUseExtendedFileMetadata && !remove.ExtendedFileMetadata
		}
	}

	tableState.Tombstones = unexpiredTombstones

	// If any Remove has ExtendedFileMetadata = false, set all to false
	if doNotUseExtendedFileMetadata {
		for path, remove := range tableState.Tombstones {
			remove.ExtendedFileMetadata = false
			tableState.Tombstones[path] = remove
			// TODO - do we need to remove the extra settings if it was true?
		}
	}
	perf.SnapshotMemory("prepareStateForCheckpoint end")
	return nil
}

// / Retrieve the next batch of checkpoint entries to write to Parquet
func checkpointRows[RowType any, PartitionType any, AddType AddPartitioned[RowType, PartitionType] | Add[RowType]](
	tableState *DeltaTableState[RowType, PartitionType], startOffset int, config *CheckpointConfiguration) ([]CheckpointEntry[RowType, PartitionType, AddType], error) {
	var maxRowCount int
	defer perf.TrackTime(time.Now(), "checkpointRows")
	perf.SnapshotMemory("checkpointRows beginning")
	maxRowCount = 2 + len(tableState.AppTransactionVersion) + tableState.FileCount() + tableState.TombstoneCount()
	if config.MaxRowsPerPart < maxRowCount {
		maxRowCount = config.MaxRowsPerPart
	}
	checkpointRows := make([]CheckpointEntry[RowType, PartitionType, AddType], 0, maxRowCount)
	perf.SnapshotMemory("checkpointRows beginning")

	currentOffset := 0

	// Row 1: protocol
	if startOffset <= currentOffset {
		protocol := new(Protocol)
		protocol.MinReaderVersion = tableState.MinReaderVersion
		protocol.MinWriterVersion = tableState.MinWriterVersion
		checkpointRows = append(checkpointRows, CheckpointEntry[RowType, PartitionType, AddType]{Protocol: protocol})
	}

	currentOffset++

	// Row 2: metadata
	if startOffset <= currentOffset && len(checkpointRows) < config.MaxRowsPerPart {
		metadata := tableState.CurrentMetadata.ToMetaData()
		checkpointRows = append(checkpointRows, CheckpointEntry[RowType, PartitionType, AddType]{MetaData: &metadata})
	}

	currentOffset++

	// Next, optional Txn entries per app id
	if startOffset < currentOffset+len(tableState.AppTransactionVersion) && len(tableState.AppTransactionVersion) > 0 && len(checkpointRows) < config.MaxRowsPerPart {
		keys := make([]string, 0, len(tableState.AppTransactionVersion))
		for k := range tableState.AppTransactionVersion {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for i, appId := range keys {
			if startOffset < currentOffset+i {
				txn := new(Txn)
				txn.AppId = appId
				version := tableState.AppTransactionVersion[appId]
				txn.Version = version
				checkpointRows = append(checkpointRows, CheckpointEntry[RowType, PartitionType, AddType]{Txn: txn})

				if len(checkpointRows) >= config.MaxRowsPerPart {
					break
				}
			}
		}
	}

	currentOffset += len(tableState.AppTransactionVersion)

	// Tombstone / Remove entries
	tombstoneCount := tableState.TombstoneCount()
	if startOffset < currentOffset+tombstoneCount && tombstoneCount > 0 && len(checkpointRows) < config.MaxRowsPerPart {
		if tableState.onDiskOptimization {
			initialOffset := startOffset - currentOffset
			if initialOffset < 0 {
				initialOffset = 0
			}
			onDiskTombstoneCheckpointRows(tableState, initialOffset, &checkpointRows, config)
		} else {
			keys := make([]string, 0, tombstoneCount)
			for k := range tableState.Tombstones {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for i, path := range keys {
				if startOffset <= currentOffset+i {
					checkpointRemove := new(Remove)
					*checkpointRemove = tableState.Tombstones[path]
					checkpointRows = append(checkpointRows, CheckpointEntry[RowType, PartitionType, AddType]{Remove: checkpointRemove})

					if len(checkpointRows) >= config.MaxRowsPerPart {
						break
					}
				}
			}
		}
	}
	currentOffset += tombstoneCount

	// Add entries
	fileCount := tableState.FileCount()
	if startOffset < currentOffset+fileCount && fileCount > 0 && len(checkpointRows) < config.MaxRowsPerPart {
		if tableState.onDiskOptimization {
			initialOffset := startOffset - currentOffset
			if initialOffset < 0 {
				initialOffset = 0
			}
			onDiskAddCheckpointRows(tableState, initialOffset, &checkpointRows, config)
		} else {
			keys := make([]string, 0, fileCount)
			for k := range tableState.Files {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for i, path := range keys {
				if startOffset <= currentOffset+i {
					add := tableState.Files[path]
					checkpointAdd, err := checkpointAdd[RowType, PartitionType, AddType](&add)
					if err != nil {
						return nil, errors.Join(ErrorConvertingCheckpointAdd, err)
					}
					checkpointRows = append(checkpointRows, CheckpointEntry[RowType, PartitionType, AddType]{Add: checkpointAdd})

					if len(checkpointRows) >= config.MaxRowsPerPart {
						break
					}
				}
			}
		}
	}
	perf.SnapshotMemory("checkpointRows end")

	return checkpointRows, nil
}
