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
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/apache/arrow/go/v13/parquet"
	"github.com/apache/arrow/go/v13/parquet/compress"
	"github.com/apache/arrow/go/v13/parquet/file"
	"github.com/apache/arrow/go/v13/parquet/pqarrow"
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
	// Add and remove actions that have been written to disk
	filesOnDisk []storage.Path
	// Keep track of whether there are pending updates while doing a disk with on-disk temp files
	pendingUpdates bool
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
func (tableState *DeltaTableState[RowType, PartitionType]) merge(newTableState *DeltaTableState[RowType, PartitionType], maxRowsPerPart int, config *ReadWriteTableConfiguration) error {
	useDisk := config != nil && config.WorkingStore != nil
	var err error

	if useDisk {
		// Try to batch file updates before applying them to the on-disk files as that process can be slow
		// If we have incoming adds and existing removes, or vice versa, or if we have too many pending updates, then process the pending updates
		if (len(tableState.Files) > 0 && len(newTableState.Tombstones) > 0) ||
			(len(tableState.Tombstones) > 0 && len(newTableState.Files) > 0) ||
			(len(tableState.Files)+len(tableState.Tombstones) > 2) { // TODO small number while testing. put in config and add unit tests
			// TODO we could parallelize calls to updateOnDiskState
			appended := false
			schemaDetails := new(*intermediateSchemaDetails)
			for i, f := range tableState.filesOnDisk {
				// Try to append if it's the last file
				tryAppend := i == len(tableState.filesOnDisk)-1
				appended, err = updateOnDiskState(config.WorkingStore, &f, schemaDetails, tableState.Files, tableState.Tombstones, maxRowsPerPart, tryAppend)
				if err != nil {
					return err
				}
			}
			if !appended {
				// Didn't append so create a new file instead
				newRecord, err := newRecordForAddsAndRemoves(tableState.Files, tableState.Tombstones, (**schemaDetails).addFieldIndex, (**schemaDetails).removeFieldIndex)
				if err != nil {
					return err
				}
				defer newRecord.Release()

				onDiskFile := storage.PathFromIter([]string{config.WorkingFolder.Raw, fmt.Sprintf("intermediate.%d.parquet", len(tableState.filesOnDisk))})
				err = writeRecords(config.WorkingStore, &onDiskFile, *schemaDetails, []arrow.Record{newRecord})
				if err != nil {
					return err
				}
				tableState.filesOnDisk = append(tableState.filesOnDisk, onDiskFile)
			}
			// Reset the pending files and tombstones
			tableState.Files = make(map[string]AddPartitioned[RowType, PartitionType], 10000)
			tableState.Tombstones = make(map[string]Remove, 10000)
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

func (details *intermediateSchemaDetails) setFromReader(reader *pqarrow.FileReader) error {
	arrowSchema, err := reader.Schema()
	if err != nil {
		return err
	}
	details.schema = arrowSchema

	indices := arrowSchema.FieldIndices("add")
	if indices == nil || len(indices) != 1 {
		return errors.Join(ErrorReadingCheckpoint, errors.New("intermediate checkpoint file schema has invalid add column index"))
	}
	details.addFieldIndex = indices[0]
	indices = arrowSchema.FieldIndices("remove")
	if indices == nil || len(indices) != 1 {
		return errors.Join(ErrorReadingCheckpoint, errors.New("intermediate checkpoint file schema has invalid remove column index"))
	}
	details.removeFieldIndex = indices[0]
	// Locate the add.Path and remove.Path field locations
	addPathFieldIndex, ok := arrowSchema.Field(details.addFieldIndex).Type.(*arrow.StructType).FieldIdx("path")
	if !ok {
		return errors.Join(ErrorReadingCheckpoint, errors.New("intermediate checkpoint file schema has invalid add.path column index"))
	}
	details.addPathFieldIndex = addPathFieldIndex
	removePathFieldIndex, ok := arrowSchema.Field(details.removeFieldIndex).Type.(*arrow.StructType).FieldIdx("path")
	if !ok {
		return errors.Join(ErrorReadingCheckpoint, errors.New("intermediate checkpoint file schema has invalid remove.path column index"))
	}
	details.removePathFieldIndex = removePathFieldIndex

	return nil
}

// TODO this function and related ones should probably be in a different file
func updateOnDiskState[RowType any, PartitionType any](store storage.ObjectStore, path *storage.Path, schemaDetails **intermediateSchemaDetails, newAdds map[string]AddPartitioned[RowType, PartitionType], newRemoves map[string]Remove, maxRowsPerPart int, tryAppend bool) (bool, error) {
	changed := false
	appended := false

	checkpointBytes, err := store.Get(path)
	if err != nil {
		return false, err
	}
	bytesReader := bytes.NewReader(checkpointBytes)
	parquetReader, err := file.NewParquetReader(bytesReader)
	if err != nil {
		return false, err
	}
	defer parquetReader.Close()
	arrowRdr, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{Parallel: true, BatchSize: 10}, memory.DefaultAllocator)
	if err != nil {
		return false, err
	}

	// schema details can be re-used for each part
	if *schemaDetails == nil {
		*schemaDetails = new(intermediateSchemaDetails)
		err = (*schemaDetails).setFromReader(arrowRdr)
		if err != nil {
			return false, err
		}
	}
	arrowSchemaDetails := *schemaDetails

	// TODO profile whether it's worth picking out the columns we want here
	tbl, err := arrowRdr.ReadTable(context.TODO())
	if err != nil {
		return false, err
	}
	defer tbl.Release()

	tableReader := array.NewTableReader(tbl, 0)
	defer tableReader.Release()

	// The initial tables will have a single record each
	// As we start appending new records while iterating the commit logs, we can expect multiple chunks per table
	rowCount := 0
	records := make([]arrow.Record, 0, 10)
	for tableReader.Next() {
		record := tableReader.Record()
		rowCount += int(record.NumRows())
		addPathArray := record.Column(arrowSchemaDetails.addFieldIndex).(*array.Struct).Field(arrowSchemaDetails.addPathFieldIndex).(*array.String)
		removePathArray := record.Column(arrowSchemaDetails.removeFieldIndex).(*array.Struct).Field(arrowSchemaDetails.removePathFieldIndex).(*array.String)

		// Locate changes to the add and remove columns
		toChangeAddRows := make([]int64, 0, len(newRemoves))
		toChangeRemoveRows := make([]int64, 0, len(newAdds))
		// Note that although record.NumRows() returns an int64, both the IsNull() and Value() functions accept ints
		for row := 0; row < int(record.NumRows()); row++ {
			// Is there an add action in this row
			if !addPathArray.IsNull(row) {
				// If the file is now in tombstones, it needs to be removed from the add file list
				_, ok := newRemoves[addPathArray.Value(row)]
				if ok {
					toChangeAddRows = append(toChangeAddRows, int64(row))
				}
			}
			// Is there a remove action in this row
			if !removePathArray.IsNull(row) {
				// If the file has been re-added, it needs to be removed from the tombstones
				_, ok := newAdds[removePathArray.Value(row)]
				if ok {
					toChangeRemoveRows = append(toChangeRemoveRows, int64(row))
				}
			}
		}

		// We need to copy the add and remove columns, including children, nulling the changed rows as we go
		var changedAdd arrow.Array
		var changedRemove arrow.Array
		if len(toChangeAddRows) > 0 {
			changedAdd, err = copyArrowArrayWithNulls(record.Column(arrowSchemaDetails.addFieldIndex), toChangeAddRows)
			if err != nil {
				return false, err
			}
			record.SetColumn(arrowSchemaDetails.addFieldIndex, changedAdd)
			changed = true
		}
		if len(toChangeRemoveRows) > 0 {
			changedRemove, err = copyArrowArrayWithNulls(record.Column(arrowSchemaDetails.removeFieldIndex), toChangeRemoveRows)
			if err != nil {
				return false, err
			}
			record.SetColumn(arrowSchemaDetails.removeFieldIndex, changedRemove)
			changed = true
		}

		records = append(records, record)
	}

	// If we want to write out the new values, see if they fit in this file
	if tryAppend && (rowCount+len(newAdds)+len(newRemoves) < maxRowsPerPart) {
		newRecord, err := newRecordForAddsAndRemoves(newAdds, newRemoves, arrowSchemaDetails.addFieldIndex, arrowSchemaDetails.removeFieldIndex)
		if err != nil {
			return false, err
		}
		defer newRecord.Release()

		records = append(records, newRecord)
		appended = true
		changed = true
	}

	if changed {
		err := writeRecords(store, path, arrowSchemaDetails, records)
		if err != nil {
			return false, err
		}
	}
	return appended, nil
}

func writeRecords(store storage.ObjectStore, path *storage.Path, arrowSchemaDetails *intermediateSchemaDetails, records []arrow.Record) error {
	props := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Zstd),
	)
	w, closeFunc, err := store.Writer(path, os.O_CREATE|os.O_TRUNC)
	if err != nil {
		return err
	}
	defer closeFunc()
	writer, err := pqarrow.NewFileWriter(arrowSchemaDetails.schema, w, props, pqarrow.DefaultWriterProps())
	if err != nil {
		return err
	}
	defer writer.Close()
	for _, record := range records {
		err = writer.Write(record)
		if err != nil {
			return err
		}
	}
	return nil
}

type intermediateSchemaDetails struct {
	schema               *arrow.Schema
	addFieldIndex        int
	addPathFieldIndex    int
	removeFieldIndex     int
	removePathFieldIndex int
}

// Get a new record containing the adds and removes
// The record needs to be released
func newRecordForAddsAndRemoves[RowType any, PartitionType any](newAdds map[string]AddPartitioned[RowType, PartitionType], newRemoves map[string]Remove, addFieldIndex int, removeFieldIndex int) (arrow.Record, error) {
	newRecord, err := newCheckpointEntryRecord[RowType, PartitionType](len(newAdds) + len(newRemoves))
	if err != nil {
		return nil, err
	}
	if len(newAdds) > 0 {
		addsSlice := make([]AddPartitioned[RowType, PartitionType], len(newAdds))
		i := 0
		for _, ap := range newAdds {
			addsSlice[i] = ap
			i++
		}
		newAddsArray, err := newColumnArray(addsSlice, 0, len(newRemoves))
		defer newAddsArray.Release()
		if err != nil {
			return nil, err
		}
		newRecord.SetColumn(addFieldIndex, newAddsArray)
	}
	if len(newRemoves) > 0 {
		removesSlice := make([]Remove, len(newRemoves))
		i := 0
		for _, r := range newRemoves {
			removesSlice[i] = r
			i++
		}
		newRemovesArray, err := newColumnArray(removesSlice, len(newAdds), 0)
		defer newRemovesArray.Release()
		if err != nil {
			return nil, err
		}
		newRecord.SetColumn(removeFieldIndex, newRemovesArray)
	}
	return newRecord, nil
}

// The returned Array needs to be released
func newColumnArray[T any](newColumn []T, nullsBefore int, nullsAfter int) (arrow.Array, error) {
	columnBuilder, _, err := newStructBuilderFromStructsPrependNulls(newColumn, nullsBefore)
	if err != nil {
		return nil, err
	}
	defer columnBuilder.Release()
	columnBuilder.AppendNulls(nullsAfter)
	columnArray := columnBuilder.NewArray()
	return columnArray, nil
}

// The returned record needs to be released
func newCheckpointEntryRecord[RowType any, PartitionType any](count int) (arrow.Record, error) {
	isPartitioned := !isPartitionTypeEmpty[PartitionType]()
	if isPartitioned {
		return newTypedCheckpointEntryRecord[RowType, PartitionType, AddPartitioned[RowType, PartitionType]](count)
	} else {
		return newTypedCheckpointEntryRecord[RowType, PartitionType, Add[RowType]](count)
	}
}

// The returned record needs to be released
func newTypedCheckpointEntryRecord[RowType any, PartitionType any, AddType AddPartitioned[RowType, PartitionType] | Add[RowType]](count int) (arrow.Record, error) {
	checkpointEntryBuilder, _, err := newRecordBuilder[CheckpointEntry[RowType, PartitionType, AddType]]()
	defer checkpointEntryBuilder.Release()
	if err != nil {
		return nil, err
	}
	for _, field := range checkpointEntryBuilder.Fields() {
		field.AppendNulls(count)
	}
	record := checkpointEntryBuilder.NewRecord()
	return record, nil
}

// Copy arrow array data, setting any rows in nullRows to null
func copyArrowArrayWithNulls(in arrow.Array, nullRows []int64) (arrow.Array, error) {
	recordBuilder := array.NewStructBuilder(memory.DefaultAllocator, in.DataType().(*arrow.StructType))
	var copiedComponents []arrow.Array = make([]arrow.Array, 0, len(nullRows)+1)
	lastIndexCopied := int64(-1)
	for _, nullIndex := range nullRows {
		// Copy any uncopied records before this null record
		if nullIndex > lastIndexCopied+1 {
			copiedSlice := array.NewSlice(in, lastIndexCopied+1, nullIndex)
			defer copiedSlice.Release()
			copiedComponents = append(copiedComponents, copiedSlice)
		}
		// Create a single length array that's all null
		recordBuilder.AppendNull()
		// NewStructArray() resets the StructBuilder so we don't have to
		nullArray := recordBuilder.NewStructArray()
		defer nullArray.Release()
		copiedComponents = append(copiedComponents, nullArray)
		lastIndexCopied = nullIndex
	}
	// Copy any uncopied records after the last null
	if in.Len() > int(lastIndexCopied)+1 {
		copiedSlice := array.NewSlice(in, lastIndexCopied+1, int64(in.Len()))
		defer copiedSlice.Release()
		copiedComponents = append(copiedComponents, copiedSlice)
	}
	return array.Concatenate(copiedComponents, memory.DefaultAllocator)
}

func stateFromCheckpoint[RowType any, PartitionType any](table *DeltaTable[RowType, PartitionType], checkpoint *CheckPoint, config *ReadWriteTableConfiguration) (*DeltaTableState[RowType, PartitionType], error) {
	newState := NewDeltaTableState[RowType, PartitionType](checkpoint.Version)
	checkpointDataPaths := table.GetCheckpointDataPaths(checkpoint)
	if config != nil && config.WorkingStore != nil {
		newState.filesOnDisk = make([]storage.Path, 0, len(checkpointDataPaths))
	}
	for i, location := range checkpointDataPaths {
		checkpointBytes, err := table.Store.Get(&location)
		if err != nil {
			return nil, err
		}
		if len(checkpointBytes) > 0 {
			err = processCheckpointBytes(checkpointBytes, newState, table, i, config)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, errors.Join(ErrorCheckpointIncomplete, fmt.Errorf("zero size checkpoint at %s", location.Raw))
		}
	}
	return newState, nil
}

func isPartitionTypeEmpty[PartitionType any]() bool {
	testPartitionItem := new(PartitionType)
	structType := reflect.TypeOf(*testPartitionItem)
	return structType.NumField() == 0
}

func processCheckpointBytes[RowType any, PartitionType any](checkpointBytes []byte, tableState *DeltaTableState[RowType, PartitionType], table *DeltaTable[RowType, PartitionType], part int, config *ReadWriteTableConfiguration) (returnErr error) {
	// Determine whether partitioned
	isPartitioned := !isPartitionTypeEmpty[PartitionType]()
	if isPartitioned {
		return processCheckpointBytesWithAddSpecified[RowType, PartitionType, AddPartitioned[RowType, PartitionType]](checkpointBytes, tableState, table, part, config)
	} else {
		return processCheckpointBytesWithAddSpecified[RowType, PartitionType, Add[RowType]](checkpointBytes, tableState, table, part, config)
	}
}

// / Update a table state with the contents of a checkpoint file
func processCheckpointBytesWithAddSpecified[RowType any, PartitionType any, AddType AddPartitioned[RowType, PartitionType] | Add[RowType]](checkpointBytes []byte, tableState *DeltaTableState[RowType, PartitionType], table *DeltaTable[RowType, PartitionType], part int, config *ReadWriteTableConfiguration) error {
	useDisk := config != nil && config.WorkingFolder != nil
	var processFunc = func(checkpointEntry *CheckpointEntry[RowType, PartitionType, AddType]) error {
		var action Action
		if checkpointEntry.Add != nil {
			action = checkpointEntry.Add
		}
		if checkpointEntry.Remove != nil {
			action = checkpointEntry.Remove
		}
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
			err := tableState.processAction(action)
			if err != nil {
				return err
			}
		} else {
			if !useDisk {
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
	parquetSchema := parquetReader.MetaData().Schema
	fileReader, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{BatchSize: 10, Parallel: true}, memory.DefaultAllocator)
	if err != nil {
		return err
	}
	arrowSchema, err := fileReader.Schema()
	if err != nil {
		return err
	}
	arrowFieldList := arrowSchema.Fields()
	inMemoryCols := make([]int, 0, 150)
	onDiskCols := make([]int, 0, 150)

	for i := 0; i < parquetSchema.NumColumns(); i++ {
		columnPath := parquetSchema.Column(i).ColumnPath().String()
		if useDisk && (strings.HasPrefix(columnPath, "add") || strings.HasPrefix(columnPath, "remove")) {
			onDiskCols = append(onDiskCols, i)
		} else {
			inMemoryCols = append(inMemoryCols, i)
		}
	}

	// Get mappings between struct member names and parquet/arrow names so we don't have to look them up repeatedly
	// during record assignments
	inMemoryIndexMappings := make(map[string]int, 100)
	defaultType := reflect.TypeOf(defaultValue)
	var fieldExclusions []string
	if useDisk {
		fieldExclusions = []string{"Root.Add", "Root.Remove"}
	}
	err = getStructFieldNameToArrowIndexMappings(defaultType, "Root", arrowFieldList, fieldExclusions, inMemoryIndexMappings)
	if err != nil {
		return err
	}
	var onDiskIndexMappings map[string]int
	if useDisk {
		onDiskIndexMappings = make(map[string]int, 100)
		err = getStructFieldNameToArrowIndexMappings(defaultType, "Root", arrowFieldList, []string{"Root.Txn", "Root.MetaData", "Root.Protocol"}, onDiskIndexMappings)
		if err != nil {
			return err
		}
	}

	testingRead := true
	// Read a row group at a time; process in-memory actions
	if testingRead {
		for i := 0; i < parquetReader.NumRowGroups(); i++ {
			tbl, err := fileReader.ReadRowGroups(context.TODO(), inMemoryCols, []int{i})
			if err != nil {
				return err
			}
			defer tbl.Release()

			tableReader := array.NewTableReader(tbl, 0)
			defer tableReader.Release()

			for tableReader.Next() {
				// the record contains a batch of rows
				record := tableReader.Record()

				entries := make([]*CheckpointEntry[RowType, PartitionType, AddType], record.NumRows())
				entryValues := make([]reflect.Value, record.NumRows())
				for j := int64(0); j < record.NumRows(); j++ {
					t := new(CheckpointEntry[RowType, PartitionType, AddType])
					entries[j] = t
					entryValues[j] = reflect.ValueOf(t)
				}

				goStructFromArrowArrays(entryValues, record.Columns(), "Root", inMemoryIndexMappings)
				for j := int64(0); j < record.NumRows(); j++ {
					err = processFunc(entries[j])
					if err != nil {
						return err
					}
				}
			}
		}
	}

	if useDisk {
		// The non-add, non-remove columns will be almost entirely nulls, so picking out just add and remove
		// slows us down here for a very minimal improvement in file size.
		// Instead we just write out the entire file.
		onDiskFile := storage.PathFromIter([]string{config.WorkingFolder.Raw, fmt.Sprintf("intermediate.%d.parquet", part)})
		config.WorkingStore.Put(&onDiskFile, checkpointBytes)
		tableState.filesOnDisk = append(tableState.filesOnDisk, onDiskFile)
	}

	return nil
}

// / Prepare the table state for checkpointing by updating tombstones
func (tableState *DeltaTableState[RowType, PartitionType]) prepareStateForCheckpoint() error {
	if tableState.CurrentMetadata == nil {
		return ErrorMissingMetadata
	}

	// Don't keep expired tombstones
	// Also check if any of the non-expired Remove actions had ExtendedFileMetadata = false
	doNotUseExtendedFileMetadata := false
	retentionTimestamp := time.Now().UnixMilli() - tableState.TombstoneRetention.Milliseconds()
	unexpiredTombstones := make(map[string]Remove, len(tableState.Tombstones))
	for path, remove := range tableState.Tombstones {
		if remove.DeletionTimestamp == nil || *remove.DeletionTimestamp > retentionTimestamp {
			unexpiredTombstones[path] = remove
			doNotUseExtendedFileMetadata = doNotUseExtendedFileMetadata && (remove.ExtendedFileMetadata == nil || !*remove.ExtendedFileMetadata)
		}
	}

	tableState.Tombstones = unexpiredTombstones

	// If any Remove has ExtendedFileMetadata = false, set all to false
	removeExtendedFileMetadata := false
	if doNotUseExtendedFileMetadata {
		for path, remove := range tableState.Tombstones {
			remove.ExtendedFileMetadata = &removeExtendedFileMetadata
			tableState.Tombstones[path] = remove
			// TODO - do we need to remove the extra settings if it was true?
		}
	}
	return nil
}

// / Retrieve the next batch of checkpoint entries to write to Parquet
func checkpointRows[RowType any, PartitionType any, AddType AddPartitioned[RowType, PartitionType] | Add[RowType]](tableState *DeltaTableState[RowType, PartitionType], startOffset int, maxRows int) ([]CheckpointEntry[RowType, PartitionType, AddType], error) {
	maxRowCount := 2 + len(tableState.AppTransactionVersion) + len(tableState.Tombstones) + len(tableState.Files)
	if maxRows < maxRowCount {
		maxRowCount = maxRows
	}
	checkpointRows := make([]CheckpointEntry[RowType, PartitionType, AddType], 0, maxRowCount)

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
	if startOffset <= currentOffset && len(checkpointRows) < maxRows {
		metadata := tableState.CurrentMetadata.ToMetaData()
		checkpointRows = append(checkpointRows, CheckpointEntry[RowType, PartitionType, AddType]{MetaData: &metadata})
	}

	currentOffset++

	// Next, optional Txn entries per app id
	if startOffset < currentOffset+len(tableState.AppTransactionVersion) && len(tableState.AppTransactionVersion) > 0 && len(checkpointRows) < maxRows {
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

				if len(checkpointRows) >= maxRows {
					break
				}
			}
		}
	}

	currentOffset += len(tableState.AppTransactionVersion)

	// Tombstone / Remove entries
	if startOffset < currentOffset+len(tableState.Tombstones) && len(tableState.Tombstones) > 0 && len(checkpointRows) < maxRows {
		keys := make([]string, 0, len(tableState.Tombstones))
		for k := range tableState.Tombstones {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for i, path := range keys {
			if startOffset <= currentOffset+i {
				checkpointRemove := new(Remove)
				*checkpointRemove = tableState.Tombstones[path]
				checkpointRows = append(checkpointRows, CheckpointEntry[RowType, PartitionType, AddType]{Remove: checkpointRemove})

				if len(checkpointRows) >= maxRows {
					break
				}
			}
		}
	}

	currentOffset += len(tableState.Tombstones)

	// Add entries
	if startOffset < currentOffset+len(tableState.Files) && len(tableState.Files) > 0 && len(checkpointRows) < maxRows {
		keys := make([]string, 0, len(tableState.Files))
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

				if len(checkpointRows) >= maxRows {
					break
				}
			}
		}
	}

	return checkpointRows, nil
}
