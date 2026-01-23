// Copyright 2023 Rivian Automotive, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package delta

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestFilteredCheckpointSinglePart tests filtering a single-part checkpoint
func TestFilteredCheckpointSinglePart(t *testing.T) {
	store, state, tableLock, _ := setupCheckpointTest(t, "")

	// Create a partitioned table
	schema := SchemaTypeStruct{
		Fields: []SchemaField{
			{Name: "id", Type: Integer, Nullable: false, Metadata: make(map[string]any)},
			{Name: "date", Type: String, Nullable: false, Metadata: make(map[string]any)},
			{Name: "value", Type: String, Nullable: true, Metadata: make(map[string]any)},
		},
	}
	metadata := NewTableMetaData("test_table", "test", new(Format).Default(), schema, []string{"date"}, make(map[string]string))
	protocol := new(Protocol).Default()

	table := NewTable(store, tableLock, state)
	err := table.Create(*metadata, protocol, make(CommitInfo), []Add{})
	if err != nil {
		t.Fatal(err)
	}

	// Add files with different partition values (dates)
	tx := table.CreateTransaction(NewTransactionOptions())
	for i := 0; i < 5; i++ {
		dateValue := fmt.Sprintf("2024-01-%02d", i+1)
		add := &Add{
			Path:             fmt.Sprintf("part-%s-%s.parquet", dateValue, uuid.NewString()),
			PartitionValues:  map[string]string{"date": dateValue},
			Size:             100 + int64(i),
			ModificationTime: time.Now().UnixMilli(),
			DataChange:       true,
		}
		tx.AddAction(add)
	}
	// Add a file with a later partition value
	for i := 5; i < 10; i++ {
		dateValue := fmt.Sprintf("2024-01-%02d", i+1)
		add := &Add{
			Path:             fmt.Sprintf("part-%s-%s.parquet", dateValue, uuid.NewString()),
			PartitionValues:  map[string]string{"date": dateValue},
			Size:             100 + int64(i),
			ModificationTime: time.Now().UnixMilli(),
			DataChange:       true,
		}
		tx.AddAction(add)
	}
	version, err := tx.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if version != 1 {
		t.Errorf("Expected version 1, got %d", version)
	}

	// Create a checkpoint at version 1
	checkpointConfig := NewCheckpointConfiguration()
	_, err = CreateCheckpoint(store, tableLock, checkpointConfig, 1)
	if err != nil {
		t.Fatal(err)
	}

	// Verify checkpoint exists
	exists, err := DoesCheckpointVersionExist(store, 1, true)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("Checkpoint at version 1 should exist")
	}

	// Now filter the checkpoint to keep only dates >= "2024-01-06"
	err = table.CreateFilteredCheckpoint(1, "date", "2024-01-06")
	if err != nil {
		t.Fatal(err)
	}

	// Verify new checkpoint at version 2 exists
	exists, err = DoesCheckpointVersionExist(store, 2, true)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("Checkpoint at version 2 should exist")
	}

	// Verify commit file exists at version 2
	_, err = store.Head(CommitURIFromVersion(2))
	if err != nil {
		t.Fatalf("Commit file at version 2 should exist: %v", err)
	}

	// Load the new checkpoint and verify it only has the filtered files
	table2 := NewTable(store, tableLock, state)
	err = table2.LoadVersion(&[]int64{2}[0])
	if err != nil {
		t.Fatal(err)
	}

	// Should have 5 files (dates 06-10)
	if table2.State.FileCount() != 5 {
		t.Errorf("Expected 5 files after filtering, got %d", table2.State.FileCount())
	}

	// Verify all files have date >= "2024-01-06"
	for path, add := range table2.State.Files {
		dateValue := add.PartitionValues["date"]
		if dateValue < "2024-01-06" {
			t.Errorf("File %s has partition date %s which is less than minimum 2024-01-06", path, dateValue)
		}
	}

	// Verify metadata is preserved
	if table2.State.CurrentMetadata == nil {
		t.Fatal("Metadata should be preserved in filtered checkpoint")
	}
	if table2.State.CurrentMetadata.Name != "test_table" {
		t.Errorf("Table name should be test_table, got %s", table2.State.CurrentMetadata.Name)
	}
}

// TestFilteredCheckpointMultiPart tests filtering a multi-part checkpoint
func TestFilteredCheckpointMultiPart(t *testing.T) {
	store, state, tableLock, _ := setupCheckpointTest(t, "")

	// Create a partitioned table
	schema := SchemaTypeStruct{
		Fields: []SchemaField{
			{Name: "id", Type: Integer, Nullable: false, Metadata: make(map[string]any)},
			{Name: "region", Type: String, Nullable: false, Metadata: make(map[string]any)},
			{Name: "value", Type: String, Nullable: true, Metadata: make(map[string]any)},
		},
	}
	metadata := NewTableMetaData("test_table", "test", new(Format).Default(), schema, []string{"region"}, make(map[string]string))
	protocol := new(Protocol).Default()

	table := NewTable(store, tableLock, state)
	err := table.Create(*metadata, protocol, make(CommitInfo), []Add{})
	if err != nil {
		t.Fatal(err)
	}

	// Add many files to force multi-part checkpoint
	// Create enough files to trigger multiple parts (> MaxRowsPerPart)
	regions := []string{"us-east", "us-west", "eu-west", "eu-east", "ap-south", "ap-north"}
	tx := table.CreateTransaction(NewTransactionOptions())
	for i := 0; i < 100; i++ {
		region := regions[i%len(regions)]
		add := &Add{
			Path:             fmt.Sprintf("part-%s-%d-%s.parquet", region, i, uuid.NewString()),
			PartitionValues:  map[string]string{"region": region},
			Size:             100 + int64(i),
			ModificationTime: time.Now().UnixMilli(),
			DataChange:       true,
		}
		tx.AddAction(add)
	}
	version, err := tx.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if version != 1 {
		t.Errorf("Expected version 1, got %d", version)
	}

	// Create a multi-part checkpoint with small part size
	checkpointConfig := NewCheckpointConfiguration()
	checkpointConfig.MaxRowsPerPart = 25 // Force multiple parts
	_, err = CreateCheckpoint(store, tableLock, checkpointConfig, 1)
	if err != nil {
		t.Fatal(err)
	}

	// Verify multi-part checkpoint exists
	exists, err := DoesCheckpointVersionExist(store, 1, true)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("Checkpoint at version 1 should exist")
	}

	// Filter to keep only regions >= "eu-west" (excludes "eu-east", "ap-south", "ap-north")
	err = table.CreateFilteredCheckpoint(1, "region", "eu-west")
	if err != nil {
		t.Fatal(err)
	}

	// Load the filtered checkpoint at version 2
	table2 := NewTable(store, tableLock, state)
	err = table2.LoadVersion(&[]int64{2}[0])
	if err != nil {
		t.Fatal(err)
	}

	// Should have filtered out regions < "eu-west"
	expectedCount := 0
	for i := 0; i < 100; i++ {
		region := regions[i%len(regions)]
		if region >= "eu-west" {
			expectedCount++
		}
	}

	if table2.State.FileCount() != expectedCount {
		t.Errorf("Expected %d files after filtering, got %d", expectedCount, table2.State.FileCount())
	}

	// Verify all files have region >= "eu-west"
	for path, add := range table2.State.Files {
		regionValue := add.PartitionValues["region"]
		if regionValue < "eu-west" {
			t.Errorf("File %s has partition region %s which is less than minimum eu-west", path, regionValue)
		}
	}
}

// TestFilteredCheckpointWithRemoves tests filtering a checkpoint with Remove actions
func TestFilteredCheckpointWithRemoves(t *testing.T) {
	store, state, tableLock, _ := setupCheckpointTest(t, "")

	// Create a partitioned table
	schema := SchemaTypeStruct{
		Fields: []SchemaField{
			{Name: "id", Type: Integer, Nullable: false, Metadata: make(map[string]any)},
			{Name: "year", Type: String, Nullable: false, Metadata: make(map[string]any)},
		},
	}
	metadata := NewTableMetaData("test_table", "test", new(Format).Default(), schema, []string{"year"}, make(map[string]string))
	protocol := new(Protocol).Default()

	table := NewTable(store, tableLock, state)
	err := table.Create(*metadata, protocol, make(CommitInfo), []Add{})
	if err != nil {
		t.Fatal(err)
	}

	// Add files with different years
	addedPaths := make(map[string]string) // map year to path
	tx := table.CreateTransaction(NewTransactionOptions())
	for year := 2020; year <= 2025; year++ {
		yearStr := fmt.Sprintf("%d", year)
		path := fmt.Sprintf("part-%s-%s.parquet", yearStr, uuid.NewString())
		add := &Add{
			Path:             path,
			PartitionValues:  map[string]string{"year": yearStr},
			Size:             100,
			ModificationTime: time.Now().UnixMilli(),
			DataChange:       true,
		}
		tx.AddAction(add)
		addedPaths[yearStr] = path
	}
	_, err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	// Remove some old files (2020, 2021)
	tx2 := table.CreateTransaction(NewTransactionOptions())
	for year := 2020; year <= 2021; year++ {
		yearStr := fmt.Sprintf("%d", year)
		path := addedPaths[yearStr]
		deletionTime := time.Now().UnixMilli()
		remove := &Remove{
			Path:              path,
			DeletionTimestamp: &deletionTime,
			DataChange:        true,
			ExtendedFileMetadata: true,
			PartitionValues:   &map[string]string{"year": yearStr},
			Size:              &[]int64{100}[0],
		}
		tx2.AddAction(remove)
	}
	_, err = tx2.Commit()
	if err != nil {
		t.Fatal(err)
	}

	// Create checkpoint at version 2
	checkpointConfig := NewCheckpointConfiguration()
	_, err = CreateCheckpoint(store, tableLock, checkpointConfig, 2)
	if err != nil {
		t.Fatal(err)
	}

	// Filter to keep only year >= "2023"
	// This should filter out Add actions for 2022 and Remove actions for 2020-2021
	err = table.CreateFilteredCheckpoint(2, "year", "2023")
	if err != nil {
		t.Fatal(err)
	}

	// Load the filtered checkpoint at version 3
	table2 := NewTable(store, tableLock, state)
	err = table2.LoadVersion(&[]int64{3}[0])
	if err != nil {
		t.Fatal(err)
	}

	// Should have only 2023, 2024, 2025 files (3 files)
	if table2.State.FileCount() != 3 {
		t.Errorf("Expected 3 files after filtering, got %d", table2.State.FileCount())
	}

	// Should have no tombstones (2020-2021 removes were filtered out)
	if table2.State.TombstoneCount() != 0 {
		t.Errorf("Expected 0 tombstones after filtering, got %d", table2.State.TombstoneCount())
	}

	// Verify all files have year >= "2023"
	for path, add := range table2.State.Files {
		yearValue := add.PartitionValues["year"]
		if yearValue < "2023" {
			t.Errorf("File %s has partition year %s which is less than minimum 2023", path, yearValue)
		}
	}
}

// TestFilteredCheckpointNoPartitionKey tests handling files without the partition key
func TestFilteredCheckpointNoPartitionKey(t *testing.T) {
	store, state, tableLock, _ := setupCheckpointTest(t, "")

	// Create a partitioned table
	schema := SchemaTypeStruct{
		Fields: []SchemaField{
			{Name: "id", Type: Integer, Nullable: false, Metadata: make(map[string]any)},
			{Name: "date", Type: String, Nullable: false, Metadata: make(map[string]any)},
		},
	}
	metadata := NewTableMetaData("test_table", "test", new(Format).Default(), schema, []string{"date"}, make(map[string]string))
	protocol := new(Protocol).Default()

	table := NewTable(store, tableLock, state)
	err := table.Create(*metadata, protocol, make(CommitInfo), []Add{})
	if err != nil {
		t.Fatal(err)
	}

	// Add some files with partition values
	tx := table.CreateTransaction(NewTransactionOptions())
	for i := 1; i <= 3; i++ {
		dateValue := fmt.Sprintf("2024-01-%02d", i)
		add := &Add{
			Path:             fmt.Sprintf("part-%s-%s.parquet", dateValue, uuid.NewString()),
			PartitionValues:  map[string]string{"date": dateValue},
			Size:             100,
			ModificationTime: time.Now().UnixMilli(),
			DataChange:       true,
		}
		tx.AddAction(add)
	}
	// Add a file without the partition key (should be preserved)
	add := &Add{
		Path:             fmt.Sprintf("part-no-key-%s.parquet", uuid.NewString()),
		PartitionValues:  map[string]string{}, // Empty partition values
		Size:             100,
		ModificationTime: time.Now().UnixMilli(),
		DataChange:       true,
	}
	tx.AddAction(add)
	_, err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	// Create checkpoint
	checkpointConfig := NewCheckpointConfiguration()
	_, err = CreateCheckpoint(store, tableLock, checkpointConfig, 1)
	if err != nil {
		t.Fatal(err)
	}

	// Filter to keep only date >= "2024-01-02"
	err = table.CreateFilteredCheckpoint(1, "date", "2024-01-02")
	if err != nil {
		t.Fatal(err)
	}

	// Load the filtered checkpoint
	table2 := NewTable(store, tableLock, state)
	err = table2.LoadVersion(&[]int64{2}[0])
	if err != nil {
		t.Fatal(err)
	}

	// Should have 3 files: 2024-01-02, 2024-01-03, and the file without partition key
	if table2.State.FileCount() != 3 {
		t.Errorf("Expected 3 files after filtering, got %d", table2.State.FileCount())
	}
}

// TestFilteredCheckpointValidation tests validation errors
func TestFilteredCheckpointValidation(t *testing.T) {
	store, state, tableLock, _ := setupCheckpointTest(t, "")

	// Create a simple table
	schema := SchemaTypeStruct{
		Fields: []SchemaField{
			{Name: "id", Type: Integer, Nullable: false, Metadata: make(map[string]any)},
			{Name: "date", Type: String, Nullable: false, Metadata: make(map[string]any)},
		},
	}
	metadata := NewTableMetaData("test_table", "test", new(Format).Default(), schema, []string{"date"}, make(map[string]string))
	protocol := new(Protocol).Default()

	table := NewTable(store, tableLock, state)
	err := table.Create(*metadata, protocol, make(CommitInfo), []Add{})
	if err != nil {
		t.Fatal(err)
	}

	// Try to filter a checkpoint that doesn't exist
	err = table.CreateFilteredCheckpoint(0, "date", "2024-01-01")
	if !errors.Is(err, ErrCheckpointIncomplete) {
		t.Errorf("Expected ErrCheckpointIncomplete when checkpoint doesn't exist, got %v", err)
	}

	// Add a file and commit to create version 1
	tx := table.CreateTransaction(NewTransactionOptions())
	add := &Add{
		Path:             fmt.Sprintf("part-%s.parquet", uuid.NewString()),
		PartitionValues:  map[string]string{"date": "2024-01-01"},
		Size:             100,
		ModificationTime: time.Now().UnixMilli(),
		DataChange:       true,
	}
	tx.AddAction(add)
	_, err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	// Create checkpoint at version 0 (not the latest)
	checkpointConfig := NewCheckpointConfiguration()
	_, err = CreateCheckpoint(store, tableLock, checkpointConfig, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Try to filter checkpoint at version 0 when version 1 exists
	err = table.CreateFilteredCheckpoint(0, "date", "2024-01-01")
	if !errors.Is(err, ErrInvalidVersion) {
		t.Errorf("Expected ErrInvalidVersion when checkpoint is not latest, got %v", err)
	}

	// Create checkpoint at version 1 (the latest)
	_, err = CreateCheckpoint(store, tableLock, checkpointConfig, 1)
	if err != nil {
		t.Fatal(err)
	}

	// Now filtering should work
	err = table.CreateFilteredCheckpoint(1, "date", "2024-01-01")
	if err != nil {
		t.Fatalf("Expected success when filtering latest checkpoint, got %v", err)
	}
}

// TestFilteredCheckpointEmptyResult tests filtering that removes all Add actions
func TestFilteredCheckpointEmptyResult(t *testing.T) {
	store, state, tableLock, _ := setupCheckpointTest(t, "")

	// Create a partitioned table
	schema := SchemaTypeStruct{
		Fields: []SchemaField{
			{Name: "id", Type: Integer, Nullable: false, Metadata: make(map[string]any)},
			{Name: "date", Type: String, Nullable: false, Metadata: make(map[string]any)},
		},
	}
	metadata := NewTableMetaData("test_table", "test", new(Format).Default(), schema, []string{"date"}, make(map[string]string))
	protocol := new(Protocol).Default()

	table := NewTable(store, tableLock, state)
	err := table.Create(*metadata, protocol, make(CommitInfo), []Add{})
	if err != nil {
		t.Fatal(err)
	}

	// Add files with old dates
	tx := table.CreateTransaction(NewTransactionOptions())
	for i := 1; i <= 3; i++ {
		dateValue := fmt.Sprintf("2020-01-%02d", i)
		add := &Add{
			Path:             fmt.Sprintf("part-%s-%s.parquet", dateValue, uuid.NewString()),
			PartitionValues:  map[string]string{"date": dateValue},
			Size:             100,
			ModificationTime: time.Now().UnixMilli(),
			DataChange:       true,
		}
		tx.AddAction(add)
	}
	_, err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	// Create checkpoint
	checkpointConfig := NewCheckpointConfiguration()
	_, err = CreateCheckpoint(store, tableLock, checkpointConfig, 1)
	if err != nil {
		t.Fatal(err)
	}

	// Filter with a minimum that excludes all files
	err = table.CreateFilteredCheckpoint(1, "date", "2025-01-01")
	if err != nil {
		t.Fatal(err)
	}

	// Load the filtered checkpoint
	table2 := NewTable(store, tableLock, state)
	err = table2.LoadVersion(&[]int64{2}[0])
	if err != nil {
		t.Fatal(err)
	}

	// Should have 0 files
	if table2.State.FileCount() != 0 {
		t.Errorf("Expected 0 files after filtering all, got %d", table2.State.FileCount())
	}

	// Metadata should still be present
	if table2.State.CurrentMetadata == nil {
		t.Fatal("Metadata should be preserved even when all files are filtered")
	}
}

// TestFilteredCheckpointPreservesProtocolAndTxn tests that Protocol and Txn actions are preserved
func TestFilteredCheckpointPreservesProtocolAndTxn(t *testing.T) {
	store, state, tableLock, _ := setupCheckpointTest(t, "")

	// Create a partitioned table
	schema := SchemaTypeStruct{
		Fields: []SchemaField{
			{Name: "id", Type: Integer, Nullable: false, Metadata: make(map[string]any)},
			{Name: "date", Type: String, Nullable: false, Metadata: make(map[string]any)},
		},
	}
	metadata := NewTableMetaData("test_table", "test", new(Format).Default(), schema, []string{"date"}, make(map[string]string))
	protocol := new(Protocol).Default()

	table := NewTable(store, tableLock, state)
	err := table.Create(*metadata, protocol, make(CommitInfo), []Add{})
	if err != nil {
		t.Fatal(err)
	}

	// Add some files
	tx := table.CreateTransaction(NewTransactionOptions())
	for i := 1; i <= 5; i++ {
		dateValue := fmt.Sprintf("2024-01-%02d", i)
		add := &Add{
			Path:             fmt.Sprintf("part-%s-%s.parquet", dateValue, uuid.NewString()),
			PartitionValues:  map[string]string{"date": dateValue},
			Size:             100,
			ModificationTime: time.Now().UnixMilli(),
			DataChange:       true,
		}
		tx.AddAction(add)
	}
	// Add a Txn action
	txn := &Txn{
		AppID:   "test-app",
		Version: 123,
	}
	tx.AddAction(txn)
	_, err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	// Create checkpoint
	checkpointConfig := NewCheckpointConfiguration()
	_, err = CreateCheckpoint(store, tableLock, checkpointConfig, 1)
	if err != nil {
		t.Fatal(err)
	}

	// Filter checkpoint
	err = table.CreateFilteredCheckpoint(1, "date", "2024-01-03")
	if err != nil {
		t.Fatal(err)
	}

	// Load filtered checkpoint
	table2 := NewTable(store, tableLock, state)
	err = table2.LoadVersion(&[]int64{2}[0])
	if err != nil {
		t.Fatal(err)
	}

	// Verify Protocol is preserved
	if table2.State.MinReaderVersion != 1 {
		t.Errorf("Expected MinReaderVersion 1, got %d", table2.State.MinReaderVersion)
	}
	if table2.State.MinWriterVersion != 1 {
		t.Errorf("Expected MinWriterVersion 1, got %d", table2.State.MinWriterVersion)
	}

	// Verify Txn is preserved
	txnVersion, exists := table2.State.AppTransactionVersion["test-app"]
	if !exists {
		t.Error("Expected Txn action to be preserved")
	} else if txnVersion != 123 {
		t.Errorf("Expected Txn version 123, got %d", txnVersion)
	}

	// Verify files were filtered (should have 3 files: dates 03, 04, 05)
	if table2.State.FileCount() != 3 {
		t.Errorf("Expected 3 files after filtering, got %d", table2.State.FileCount())
	}
}
