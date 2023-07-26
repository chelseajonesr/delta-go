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
	"strings"
	"sync"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/apache/arrow/go/v13/parquet/file"
	"github.com/apache/arrow/go/v13/parquet/pqarrow"
	"github.com/rivian/delta-go/storage"
	"golang.org/x/sync/errgroup"
)

// This file manages on-disk optimization of table state during checkpoint
// read and generation

type OnDiskTableState struct {
	// Add and remove actions that have been written to disk
	onDiskTempFiles []storage.Path
	// Mutexes for concurrent table state updates
	concurrentUpdateMutex sync.Mutex
	// Count of adds and removes
	onDiskFileCount      int
	onDiskTombstoneCount int
	// And per-part counts of adds and removes, needed for checkpoint generation
	onDiskFileCountsPerPart      []int
	onDiskTombstoneCountsPerPart []int
	// Whether to remove extended file metadata from all tombstones in checkpoint generation
	onDiskRemoveExtendedFileMetadata bool
}

// Merge the incoming state with existing on-disk state
func (tableState *DeltaTableState[RowType, PartitionType]) mergeOnDiskState(newTableState *DeltaTableState[RowType, PartitionType], maxRowsPerPart int, config *ReadWriteCheckpointConfiguration, finalMerge bool) error {
	// Try to batch file updates before applying them to the on-disk files as that process can be slow
	// If we have incoming adds and existing removes, or vice versa, or if we have too many pending updates, then process the pending updates
	if finalMerge ||
		(len(tableState.Files) > 0 && len(newTableState.Tombstones) > 0) ||
		(len(tableState.Tombstones) > 0 && len(newTableState.Files) > 0) ||
		(len(tableState.Files)+len(tableState.Tombstones) > maxRowsPerPart) {
		appended := false

		mergeSinglePart := func(part int) error {
			tryAppend := part == len(tableState.onDiskTempFiles)-1
			didAppend, addsDiff, tombstonesDiff, err := mergeNewAddsAndRemovesToOnDiskPartState(config.WorkingStore, &tableState.onDiskTempFiles[part], tableState.Files, tableState.Tombstones, maxRowsPerPart, tryAppend)
			if err != nil {
				return err
			}
			// Only one call to updateOnDiskState() will try to append, so only one (at most) goroutine will set appended here
			if didAppend {
				appended = true
			}
			// This is threadsafe
			if addsDiff != 0 || tombstonesDiff != 0 {
				tableState.updateOnDiskCounts(addsDiff, tombstonesDiff)
			}
			return nil
		}
		// Optional concurrency support
		if config.ConcurrentCheckpointRead > 1 {
			g, ctx := errgroup.WithContext(context.Background())
			fileIndexChannel := make(chan int)

			for i := 0; i < config.ConcurrentCheckpointRead; i++ {
				g.Go(func() error {
					for part := range fileIndexChannel {
						err := mergeSinglePart(part)
						if err != nil {
							return err
						}
					}
					return nil
				})
			}
			g.Go(func() error {
				defer close(fileIndexChannel)
				done := ctx.Done()
				for i := range tableState.onDiskTempFiles {
					if err := ctx.Err(); err != nil {
						return err
					}
					select {
					case fileIndexChannel <- i:
						continue
					case <-done:
						break
					}
				}
				return ctx.Err()
			})
			err := g.Wait()
			if err != nil {
				return err
			}
		} else {
			// non-concurrent
			for part := range tableState.onDiskTempFiles {
				err := mergeSinglePart(part)
				if err != nil {
					return err
				}
			}
		}

		if !appended {
			// Didn't append so create a new file instead
			exampleRecord, err := newCheckpointEntryRecord[RowType, PartitionType](0)
			if err != nil {
				return err
			}
			schemaDetails := new(tempFileSchemaDetails)
			err = schemaDetails.setFromArrowSchema(exampleRecord.Schema(), nil)
			if err != nil {
				return err
			}
			newRecord, err := newRecordForAddsAndRemoves(tableState.Files, tableState.Tombstones, schemaDetails.addFieldIndex, schemaDetails.removeFieldIndex)
			if err != nil {
				return err
			}
			defer newRecord.Release()
			(*schemaDetails).schema = newRecord.Schema()

			onDiskFile := storage.PathFromIter([]string{config.WorkingFolder.Raw, fmt.Sprintf("intermediate.%d.parquet", len(tableState.onDiskTempFiles))})
			err = writeRecords(config.WorkingStore, &onDiskFile, schemaDetails.schema, []arrow.Record{newRecord})
			if err != nil {
				return err
			}
			tableState.onDiskTempFiles = append(tableState.onDiskTempFiles, onDiskFile)
			tableState.updateOnDiskCounts(len(tableState.Files), len(tableState.Tombstones))
		}
		// Reset the pending files and tombstones
		tableState.Files = make(map[string]AddPartitioned[RowType, PartitionType], 10000)
		tableState.Tombstones = make(map[string]Remove, 10000)
	}
	return nil
}

// Merge new adds and removes with a single on-disk temp file
func mergeNewAddsAndRemovesToOnDiskPartState[RowType any, PartitionType any](
	store storage.ObjectStore, path *storage.Path,
	newAdds map[string]AddPartitioned[RowType, PartitionType], newRemoves map[string]Remove,
	maxRowsPerPart int, tryAppend bool) (bool, int, int, error) {
	var addDiffCount, tombstoneDiffCount int
	appended := false

	getRowsToNull := func(record arrow.Record, arrowSchemaDetails *tempFileSchemaDetails, addRowsToNull *[]int64, removeRowsToNull *[]int64) {
		addPathArray := record.Column(arrowSchemaDetails.addFieldIndex).(*array.Struct).Field(arrowSchemaDetails.addPathFieldIndex).(*array.String)
		removePathArray := record.Column(arrowSchemaDetails.removeFieldIndex).(*array.Struct).Field(arrowSchemaDetails.removePathFieldIndex).(*array.String)
		// Note that although record.NumRows() returns an int64, both the IsNull() and Value() functions accept ints
		for row := 0; row < int(record.NumRows()); row++ {
			// Is there an add action in this row
			if !addPathArray.IsNull(row) {
				// If the file is now in tombstones, it needs to be removed from the add file list
				_, ok := newRemoves[addPathArray.Value(row)]
				if ok {
					*addRowsToNull = append(*addRowsToNull, int64(row))
				}
			}
			// Is there a remove action in this row
			if !removePathArray.IsNull(row) {
				// If the file has been re-added, it needs to be removed from the tombstones
				_, ok := newAdds[removePathArray.Value(row)]
				if ok {
					*removeRowsToNull = append(*removeRowsToNull, int64(row))
				}
			}
		}
		addDiffCount -= len(*addRowsToNull)
		tombstoneDiffCount -= len(*removeRowsToNull)
	}

	appendRows := func(records *[]arrow.Record, arrowSchemaDetails *tempFileSchemaDetails, rowCount int) (bool, error) {
		// If we want to write out the new values, see if they fit in this file
		if tryAppend && (rowCount+len(newAdds)+len(newRemoves) < maxRowsPerPart) {
			// Existing checkpoint file schema may not match if generated from a different client; check first
			exampleRecord, err := newCheckpointEntryRecord[RowType, PartitionType](0)
			if err != nil {
				return false, err
			}
			if exampleRecord.Schema().Equal(arrowSchemaDetails.schema) {
				// newRecordForAddsAndRemoves returns a retained record, so we don't need to retain it again here
				newRecord, err := newRecordForAddsAndRemoves(newAdds, newRemoves, arrowSchemaDetails.addFieldIndex, arrowSchemaDetails.removeFieldIndex)
				if err != nil {
					return false, err
				}
				*records = append(*records, newRecord)
				addDiffCount += len(newAdds)
				tombstoneDiffCount += len(newRemoves)
				appended = true
				return true, nil
			}
		}
		return false, nil
	}

	err := updateOnDiskPartState[RowType, PartitionType](store, path, getRowsToNull, appendRows)
	return appended, addDiffCount, tombstoneDiffCount, err
}

// Update the on-disk state in the given file with the new adds and removes
// This may be called concurrently on multiple checkpoint parts
func updateOnDiskPartState[RowType any, PartitionType any](
	store storage.ObjectStore, path *storage.Path,
	getRowsToNull func(record arrow.Record, arrowSchemaDetails *tempFileSchemaDetails, addRowsToNull *[]int64, removeRowsToNull *[]int64),
	appendRows func(records *[]arrow.Record, arrowSchemaDetails *tempFileSchemaDetails, rowCount int) (bool, error)) error {

	changed := false

	tableReader, _, arrowSchemaDetails, deferFuncs, err := openFileForTableReader(store, path, nil)
	for _, d := range deferFuncs {
		defer d()
	}
	if err != nil {
		return err
	}

	// The initial tables will have a single record each
	// As we start appending new records while iterating the commit logs, we can expect multiple chunks per table
	rowCount := 0
	records := make([]arrow.Record, 0, 10)
	for tableReader.Next() {
		record := tableReader.Record()
		record.Retain()
		rowCount += int(record.NumRows())
		// Locate changes to the add and remove columns
		addRowsToNull := make([]int64, 0, rowCount)
		removeRowsToNull := make([]int64, 0, rowCount)
		getRowsToNull(record, arrowSchemaDetails, &addRowsToNull, &removeRowsToNull)

		// Copy the add and remove columns, including children, nulling the changed rows as we go
		var changedAdd arrow.Array
		var changedRemove arrow.Array
		if len(addRowsToNull) > 0 {
			changedAdd, err = copyArrowArrayWithNulls(record.Column(arrowSchemaDetails.addFieldIndex), addRowsToNull)
			if err != nil {
				return err
			}
			record, err = record.SetColumn(arrowSchemaDetails.addFieldIndex, changedAdd)
			if err != nil {
				return err
			}
			changed = true
		}
		if len(removeRowsToNull) > 0 {
			changedRemove, err = copyArrowArrayWithNulls(record.Column(arrowSchemaDetails.removeFieldIndex), removeRowsToNull)
			if err != nil {
				return err
			}
			record, err = record.SetColumn(arrowSchemaDetails.removeFieldIndex, changedRemove)
			if err != nil {
				return err
			}
			changed = true
		}

		records = append(records, record)
	}

	// Callback to append additional rows
	appended, err := appendRows(&records, arrowSchemaDetails, rowCount)
	if err != nil {
		return err
	}
	changed = changed || appended

	if changed {
		err := writeRecords(store, path, arrowSchemaDetails.schema, records)
		if err != nil {
			return err
		}
	}

	for _, record := range records {
		record.Release()
	}
	return nil
}

// Count the adds and tombstones in a checkpoint file and add them to the state total
func countAddsAndTombstones[RowType any, PartitionType any](tableState *DeltaTableState[RowType, PartitionType], checkpointBytes []byte, arrowSchema *arrow.Schema, config *ReadWriteCheckpointConfiguration) error {
	arrowSchemaDetails := new(tempFileSchemaDetails)
	err := arrowSchemaDetails.setFromArrowSchema(arrowSchema, nil)
	if err != nil {
		return err
	}

	bytesReader := bytes.NewReader(checkpointBytes)
	parquetReader, err := file.NewParquetReader(bytesReader)
	if err != nil {
		return err
	}
	defer parquetReader.Close()
	arrowRdr, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{Parallel: true, BatchSize: 10}, memory.DefaultAllocator)
	if err != nil {
		return err
	}

	tbl, err := arrowRdr.ReadTable(context.TODO())
	if err != nil {
		return err
	}
	defer tbl.Release()

	tableReader := array.NewTableReader(tbl, 0)
	defer tableReader.Release()

	rowCount := 0
	for tableReader.Next() {
		record := tableReader.Record()
		rowCount += int(record.NumRows())
		addPathArray := record.Column(arrowSchemaDetails.addFieldIndex).(*array.Struct).Field(arrowSchemaDetails.addPathFieldIndex).(*array.String)
		removePathArray := record.Column(arrowSchemaDetails.removeFieldIndex).(*array.Struct).Field(arrowSchemaDetails.removePathFieldIndex).(*array.String)
		tableState.updateOnDiskCounts(addPathArray.Len()-addPathArray.NullN(), removePathArray.Len()-removePathArray.NullN())
	}
	return nil
}

// Additional information used in updating a temporary on-disk checkpoint file
type tempFileSchemaDetails struct {
	schema                          *arrow.Schema
	addFieldIndex                   int
	addPathFieldIndex               int
	removeFieldIndex                int
	removePathFieldIndex            int
	removeExtendedFileMetadataIndex int
	removeDeletionTimestampIndex    int
}

// / This only supports top level checkpoint column skipping, by using excludePrefixes
func (details *tempFileSchemaDetails) setFromArrowSchema(arrowSchema *arrow.Schema, excludePrefixes []string) error {
	details.schema = arrowSchema

	currentNonExcludedIdx := 0
	var addFieldIndexWithoutExclusions, removeFieldIndexWithoutExclusions int
	for i, field := range arrowSchema.Fields() {
		excluded := false
		for _, prefix := range excludePrefixes {
			if strings.HasPrefix(field.Name, prefix) {
				excluded = true
				break
			}
		}
		if field.Name == "add" {
			addFieldIndexWithoutExclusions = i
			details.addFieldIndex = currentNonExcludedIdx
		} else if field.Name == "remove" {
			removeFieldIndexWithoutExclusions = i
			details.removeFieldIndex = currentNonExcludedIdx
		}
		if !excluded {
			currentNonExcludedIdx++
		}
	}
	addStruct := arrowSchema.Field(addFieldIndexWithoutExclusions).Type.(*arrow.StructType)
	// Locate required field locations
	index, ok := addStruct.FieldIdx("path")
	if !ok {
		return errors.Join(ErrorReadingCheckpoint, errors.New("temporary checkpoint file schema has invalid add.path column index"))
	}
	details.addPathFieldIndex = index
	removeStruct := arrowSchema.Field(removeFieldIndexWithoutExclusions).Type.(*arrow.StructType)
	index, ok = removeStruct.FieldIdx("path")
	if !ok {
		return errors.Join(ErrorReadingCheckpoint, errors.New("temporary checkpoint file schema has invalid remove.path column index"))
	}
	details.removePathFieldIndex = index
	index, ok = removeStruct.FieldIdx("extendedFileMetadata")
	if !ok {
		return errors.Join(ErrorReadingCheckpoint, errors.New("temporary checkpoint file schema has invalid remove.extendedFileMetadata column index"))
	}
	details.removeExtendedFileMetadataIndex = index
	index, ok = removeStruct.FieldIdx("deletionTimestamp")
	if !ok {
		return errors.Join(ErrorReadingCheckpoint, errors.New("temporary checkpoint file schema has invalid remove.deletionTimestamp column index"))
	}
	details.removeDeletionTimestampIndex = index

	return nil
}

// Get a new record containing the adds and removes
// The record returned needs to be released
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
		newRecord, err = newRecord.SetColumn(addFieldIndex, newAddsArray)
		if err != nil {
			return nil, err
		}
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
		newRecord, err = newRecord.SetColumn(removeFieldIndex, newRemovesArray)
		if err != nil {
			return nil, err
		}
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

// Apply checkpoint parts to the table state concurrently
func (tableState *DeltaTableState[RowType, PartitionType]) applyCheckpointConcurrently(store storage.ObjectStore, checkpointDataPaths []storage.Path, config *ReadWriteCheckpointConfiguration) error {
	var taskChannel chan checkpointProcessingTask[RowType, PartitionType]
	g, ctx := errgroup.WithContext(context.Background())
	taskChannel = make(chan checkpointProcessingTask[RowType, PartitionType])

	for i := 0; i < config.ConcurrentCheckpointRead; i++ {
		g.Go(func() error {
			for t := range taskChannel {
				if err := stateFromCheckpointPart(t); err != nil {
					return err
				} else if err := ctx.Err(); err != nil {
					return err
				}
			}
			return nil
		})
	}
	g.Go(func() error {
		defer close(taskChannel)
		done := ctx.Done()
		for i, location := range checkpointDataPaths {
			if err := ctx.Err(); err != nil {
				return err
			}
			task := checkpointProcessingTask[RowType, PartitionType]{store: store, location: location, state: tableState, part: i, config: config}
			select {
			case taskChannel <- task:
				continue
			case <-done:
				break
			}
		}
		return ctx.Err()
	})
	err := g.Wait()
	if err != nil {
		return err
	}
	return nil
}

// Threadsafe: update state with changes to the number of adds/removes on disk
func (tableState *DeltaTableState[RowType, PartitionType]) updateOnDiskCounts(addsDiff int, tombstonesDiff int) {
	tableState.concurrentUpdateMutex.Lock()
	defer tableState.concurrentUpdateMutex.Unlock()
	tableState.onDiskFileCount += addsDiff
	tableState.onDiskTombstoneCount += tombstonesDiff
}

// Threadsafe: mark that tombstones were found without extended metadata
func (tableState *DeltaTableState[RowType, PartitionType]) setTombstoneWithoutExtendedMetadata() {
	tableState.concurrentUpdateMutex.Lock()
	defer tableState.concurrentUpdateMutex.Unlock()
	tableState.onDiskRemoveExtendedFileMetadata = true
}

// Find out whether there are any non-extended metadata tombstones and null out any expired tombstones
// Also count adds and removes in each on disk temp file
func (tableState *DeltaTableState[RowType, PartitionType]) prepareOnDiskStateForCheckpoint(retentionTimestamp int64, config *ReadWriteCheckpointConfiguration) error {
	tableState.onDiskFileCountsPerPart = make([]int, len(tableState.onDiskTempFiles))
	tableState.onDiskTombstoneCountsPerPart = make([]int, len(tableState.onDiskTempFiles))

	prepareSinglePart := func(part int) error {
		tombstoneWithoutExtendedMetadata, tombstonesDiff, partFileCount, partTombstoneCount, err := prepareOnDiskPartStateForCheckpoint[RowType, PartitionType](config.WorkingStore, &tableState.onDiskTempFiles[part], retentionTimestamp)
		if err != nil {
			return err
		}
		tableState.onDiskFileCountsPerPart[part] = partFileCount
		tableState.onDiskTombstoneCountsPerPart[part] = partTombstoneCount

		// These updates are threadsafe
		if tombstoneWithoutExtendedMetadata {
			tableState.setTombstoneWithoutExtendedMetadata()
		}
		if tombstonesDiff != 0 {
			tableState.updateOnDiskCounts(0, tombstonesDiff)
		}
		return nil
	}

	// Optional concurrency support
	if config.ConcurrentCheckpointRead > 1 {
		g, ctx := errgroup.WithContext(context.Background())
		fileIndexChannel := make(chan int)

		for i := 0; i < config.ConcurrentCheckpointRead; i++ {
			g.Go(func() error {
				for part := range fileIndexChannel {
					err := prepareSinglePart(part)
					if err != nil {
						return err
					}
				}
				return nil
			})
		}
		g.Go(func() error {
			defer close(fileIndexChannel)
			done := ctx.Done()
			for i := range tableState.onDiskTempFiles {
				if err := ctx.Err(); err != nil {
					return err
				}
				select {
				case fileIndexChannel <- i:
					continue
				case <-done:
					break
				}
			}
			return ctx.Err()
		})
		err := g.Wait()
		if err != nil {
			return err
		}
	} else {
		// non-concurrent
		for part := range tableState.onDiskTempFiles {
			err := prepareSinglePart(part)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Find out whether there are any non-extended metadata tombstones and null out any expired tombstones
// This may be called concurrently on multiple checkpoint parts
func prepareOnDiskPartStateForCheckpoint[RowType any, PartitionType any](store storage.ObjectStore, path *storage.Path, retentionTimestamp int64) (bool, int, int, int, error) {
	tombstoneDiffCount := 0
	tombstoneWithoutExtendedMetadata := false
	tombstoneCount := 0
	fileCount := 0
	getRowsToNull := func(record arrow.Record, arrowSchemaDetails *tempFileSchemaDetails, addRowsToNull *[]int64, removeRowsToNull *[]int64) {
		removeArray := record.Column(arrowSchemaDetails.removeFieldIndex).(*array.Struct)
		addArray := record.Column(arrowSchemaDetails.addFieldIndex).(*array.Struct)
		removeExtendedFileMetadataArray := removeArray.Field(arrowSchemaDetails.removeExtendedFileMetadataIndex).(*array.Boolean)
		removeDeletionTimestampArray := removeArray.Field(arrowSchemaDetails.removeDeletionTimestampIndex).(*array.Int64)
		// Note that although record.NumRows() returns an int64, both the IsNull() and Value() functions accept ints
		for row := 0; row < int(record.NumRows()); row++ {
			// Is there a remove action in this row
			if removeArray.IsValid(row) {
				efm := removeExtendedFileMetadataArray.IsValid(row) && removeExtendedFileMetadataArray.Value(row)
				tombstoneWithoutExtendedMetadata = tombstoneWithoutExtendedMetadata || !efm

				if removeDeletionTimestampArray.IsValid(row) && removeDeletionTimestampArray.Value(row) <= retentionTimestamp {
					*removeRowsToNull = append(*removeRowsToNull, int64(row))
				}
			}
		}
		// Number of valid tombstones in the file: length of the array, minus length of nulls, minus length of rows we're about to null
		tombstoneCount += (removeArray.Len() - removeArray.NullN()) - len(*removeRowsToNull)
		fileCount += addArray.Len() - addArray.NullN()
		tombstoneDiffCount -= len(*removeRowsToNull)
	}
	appendRows := func(records *[]arrow.Record, arrowSchemaDetails *tempFileSchemaDetails, rowCount int) (bool, error) {
		return false, nil
	}

	err := updateOnDiskPartState[RowType, PartitionType](store, path, getRowsToNull, appendRows)
	return tombstoneWithoutExtendedMetadata, tombstoneDiffCount, fileCount, tombstoneCount, err
}

func onDiskRows[RowType any, PartitionType any, AddType AddPartitioned[RowType, PartitionType] | Add[RowType]](
	tableState *DeltaTableState[RowType, PartitionType], initialOffset int, checkpointRows *[]CheckpointEntry[RowType, PartitionType, AddType], config *CheckpointConfiguration,
	partRowCountArray []int, structFieldExclusions []string, arrowFieldExclusions []string, validityIndex func(*tempFileSchemaDetails) int) error {

	// Figure out which part file
	partRowsProcessed := 0
	for part, f := range tableState.onDiskTempFiles {
		currentPartRows := partRowCountArray[part]
		// We want to skip past "initialOffset" rows
		if initialOffset <= partRowsProcessed+currentPartRows && currentPartRows > 0 {
			// Retrieve rows from this part

			// Use a function to make per-file defer cleanup simpler
			err := func() error {
				tableReader, arrowSchema, schemaDetails, deferFuncs, err := openFileForTableReader(config.ReadWriteConfiguration.WorkingStore, &f, arrowFieldExclusions)
				for _, d := range deferFuncs {
					defer d()
				}
				if err != nil {
					return err
				}

				// Which row to start on in this part
				partRowOffset := initialOffset - partRowsProcessed
				if partRowOffset < 0 {
					partRowOffset = 0
				}

				// How many rows to read in this part
				expectedRows := currentPartRows - partRowOffset
				if len(*checkpointRows)+expectedRows > config.MaxRowsPerPart {
					expectedRows = config.MaxRowsPerPart - len(*checkpointRows)
				}

				// Index mappings need to be generated each time because checkpoint temp files may not have the same schema
				defaultValue := new(CheckpointEntry[RowType, PartitionType, AddType])
				schemaIndexMappings := make(map[string]int, 100)
				defaultType := reflect.TypeOf(defaultValue)
				err = getStructFieldNameToArrowIndexMappings(defaultType, "Root", arrowSchema.Fields(), structFieldExclusions, schemaIndexMappings)
				if err != nil {
					return err
				}

				// Allocate our new checkpoint entry rows
				entries := make([]*CheckpointEntry[RowType, PartitionType, AddType], expectedRows)
				for j := 0; j < expectedRows; j++ {
					t := new(CheckpointEntry[RowType, PartitionType, AddType])
					entries[j] = t
				}
				entryCount := 0
				columnIdx := validityIndex(schemaDetails)

				for tableReader.Next() && entryCount < expectedRows {
					record := tableReader.Record()
					requiredStructArray := record.Column(columnIdx).(*array.Struct)
					for row := partRowOffset; row < int(record.NumRows()) && entryCount < expectedRows; row++ {
						// Is there a required action in this row
						if requiredStructArray.IsValid(row) {
							// Convert the action to a Go checkpoint entry
							// TODO - as a further optimization, skip converting to Go and back again.
							// However, the incoming checkpoint schema doesn't necessarily match our schema, so this will require
							// schema conversion inside Arrow.
							err = goStructFromArrowArrays([]reflect.Value{reflect.ValueOf(entries[entryCount])}, record.Columns(), "Root", schemaIndexMappings, row)
							if err != nil {
								return nil
							}
							entryCount++
						}
					}
				}

				// Append the new checkpoint rows
				for j := 0; j < expectedRows; j++ {
					*checkpointRows = append(*checkpointRows, *entries[j])
				}
				return nil
			}()
			if err != nil {
				return err
			}
			if len(*checkpointRows) >= config.MaxRowsPerPart {
				return nil
			}
		}
		partRowsProcessed += currentPartRows
	}
	return nil
}

func onDiskTombstoneCheckpointRows[RowType any, PartitionType any, AddType AddPartitioned[RowType, PartitionType] | Add[RowType]](
	tableState *DeltaTableState[RowType, PartitionType], initialOffset int, checkpointRows *[]CheckpointEntry[RowType, PartitionType, AddType],
	config *CheckpointConfiguration) error {
	initialLength := len(*checkpointRows)
	err := onDiskRows[RowType, PartitionType, AddType](
		tableState, initialOffset, checkpointRows, config, tableState.onDiskTombstoneCountsPerPart,
		[]string{"Root.Txn", "Root.Add", "Root.MetaData", "Root.Protocol"},
		[]string{"txn", "add", "metaData", "protocol"},
		func(t *tempFileSchemaDetails) int { return t.removeFieldIndex })
	if err != nil {
		return err
	}
	if tableState.onDiskRemoveExtendedFileMetadata {
		for i := initialLength; i < len(*checkpointRows); i++ {
			if (*checkpointRows)[i].Remove != nil {
				(*checkpointRows)[i].Remove.ExtendedFileMetadata = false
			}
		}
	}
	return nil
}

func onDiskAddCheckpointRows[RowType any, PartitionType any, AddType AddPartitioned[RowType, PartitionType] | Add[RowType]](
	tableState *DeltaTableState[RowType, PartitionType], initialOffset int, checkpointRows *[]CheckpointEntry[RowType, PartitionType, AddType],
	config *CheckpointConfiguration) error {
	return onDiskRows[RowType, PartitionType, AddType](
		tableState, initialOffset, checkpointRows, config, tableState.onDiskFileCountsPerPart,
		[]string{"Root.Txn", "Root.Remove", "Root.MetaData", "Root.Protocol"},
		[]string{"txn", "remove", "metaData", "protocol"},
		func(t *tempFileSchemaDetails) int { return t.addFieldIndex })
}

// / The slice of functions returned contains cleanup functions that should be immediately called with defer by the caller,
// / even if an error is also returned.
// / Also, the cleanup functions do cleanup for everything including the returned TableReader
// / If excludePrefixes is set, the parquet schema will be adjusted to skip reading any columns starting with that prefix
func openFileForTableReader(store storage.ObjectStore, path *storage.Path, excludePrefixes []string) (*array.TableReader, *arrow.Schema, *tempFileSchemaDetails, []func(), error) {
	deferFuncs := make([]func(), 0, 3)
	checkpointBytes, err := store.Get(path)
	if err != nil {
		return nil, nil, nil, deferFuncs, err
	}
	bytesReader := bytes.NewReader(checkpointBytes)
	parquetReader, err := file.NewParquetReader(bytesReader)
	if err != nil {
		return nil, nil, nil, deferFuncs, err
	}
	closeIgnoreErr := func() {
		parquetReader.Close()
	}
	deferFuncs = append(deferFuncs, closeIgnoreErr)

	arrowRdr, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{Parallel: true, BatchSize: 10}, memory.DefaultAllocator)
	if err != nil {
		return nil, nil, nil, deferFuncs, err
	}

	schemaDetails := new(tempFileSchemaDetails)
	arrowSchema, err := arrowRdr.Schema()
	if err != nil {
		return nil, nil, nil, deferFuncs, err
	}
	err = schemaDetails.setFromArrowSchema(arrowSchema, excludePrefixes)
	if err != nil {
		return nil, nil, nil, deferFuncs, err
	}

	var tbl arrow.Table

	if len(excludePrefixes) > 0 {
		parquetSchema := parquetReader.MetaData().Schema
		allowedCols := make([]int, 0, 150)
	checkColumn:
		for i := 0; i < parquetSchema.NumColumns(); i++ {
			columnPath := parquetSchema.Column(i).ColumnPath().String()
			for _, prefix := range excludePrefixes {
				if strings.HasPrefix(columnPath, prefix) {
					continue checkColumn
				}
			}
			allowedCols = append(allowedCols, i)
		}
		rowgroups := make([]int, arrowRdr.ParquetReader().NumRowGroups())
		for i := 0; i < arrowRdr.ParquetReader().NumRowGroups(); i++ {
			rowgroups[i] = i
		}
		tbl, err = arrowRdr.ReadRowGroups(context.TODO(), allowedCols, rowgroups)
	} else {
		tbl, err = arrowRdr.ReadTable(context.TODO())
	}
	if err != nil {
		return nil, nil, nil, deferFuncs, err
	}
	deferFuncs = append(deferFuncs, tbl.Release)

	tableReader := array.NewTableReader(tbl, 0)
	deferFuncs = append(deferFuncs, tableReader.Release)

	return tableReader, arrowSchema, schemaDetails, deferFuncs, nil
}
