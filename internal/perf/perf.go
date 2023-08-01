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
package perf

import (
	"log"
	"runtime"
	"sort"
	"time"

	"gonum.org/v1/gonum/floats"
	"gonum.org/v1/gonum/stat"
)

var (
	timings         map[string][]float64
	memorySnapshots map[string][]runtime.MemStats
	initialized     bool
)

func Init() {
	timings = make(map[string][]float64, 20)
	memorySnapshots = make(map[string][]runtime.MemStats, 20)
	initialized = true
}

func TrackTime(start time.Time, name string) {
	if !initialized {
		return
	}
	elapsed := time.Since(start) / time.Millisecond
	durations, ok := timings[name]
	if !ok {
		durations = make([]float64, 0, 500)
	}
	durations = append(durations, float64(elapsed))
	timings[name] = durations
}

func SnapshotMemory(name string) {
	if !initialized {
		return
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	snapshots, ok := memorySnapshots[name]
	if !ok {
		snapshots = make([]runtime.MemStats, 0, 500)
	}
	snapshots = append(snapshots, m)
	memorySnapshots[name] = snapshots
}

func PrintTimings(verbose bool) {
	if !initialized {
		return
	}
	names := make([]string, 0, len(timings))
	for k := range timings {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		timingsForName := timings[k]
		min := floats.Min(timingsForName)
		max := floats.Max(timingsForName)
		stdev := stat.StdDev(timingsForName, nil)
		mean := stat.Mean(timingsForName, nil)
		log.Printf("%s in ms: min %0.2f, max %0.2f, mean %0.2f, stdev %0.2f, count %d", k, min, max, mean, stdev, len(timingsForName))
		if verbose {
			log.Printf("%v", timingsForName)
		}
		log.Printf("----------------------------------")
	}
}

func PrintSnapshots(verbose bool) {
	if !initialized {
		return
	}
	mb := float64(1024) * 1024
	names := make([]string, 0, len(memorySnapshots))
	for k := range memorySnapshots {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		snapshotsForName := memorySnapshots[k]
		heapAllocs := make([]float64, 0, len(snapshotsForName))
		verboseHeapAllocs := make([]uint64, 0, len(snapshotsForName))
		totalAllocs := make([]float64, 0, len(snapshotsForName))
		syses := make([]float64, 0, len(snapshotsForName))
		for _, snapshot := range snapshotsForName {
			heapAllocs = append(heapAllocs, float64(snapshot.HeapAlloc)/mb)
			verboseHeapAllocs = append(verboseHeapAllocs, uint64(float64(snapshot.HeapAlloc)/mb))
			totalAllocs = append(totalAllocs, float64(snapshot.TotalAlloc)/mb)
			syses = append(syses, float64(snapshot.Sys)/mb)
		}
		log.Printf("Memory snapshot for %s", k)
		min := floats.Min(heapAllocs)
		max := floats.Max(heapAllocs)
		stdev := stat.StdDev(heapAllocs, nil)
		mean := stat.Mean(heapAllocs, nil)
		log.Printf("Heap Allocations: min %0.f, max %0.f, mean %0.f, stdev %0.f, count %d", min, max, mean, stdev, len(snapshotsForName))
		min = floats.Min(totalAllocs)
		max = floats.Max(totalAllocs)
		stdev = stat.StdDev(totalAllocs, nil)
		mean = stat.Mean(totalAllocs, nil)
		log.Printf("Total Allocations: min %0.f, max %0.f, mean %0.f, stdev %0.f, count %d", min, max, mean, stdev, len(snapshotsForName))
		min = floats.Min(syses)
		max = floats.Max(syses)
		stdev = stat.StdDev(syses, nil)
		mean = stat.Mean(syses, nil)
		log.Printf("System: min %0.f, max %0.f, mean %0.f, stdev %0.f, count %d", min, max, mean, stdev, len(snapshotsForName))
		if verbose {
			log.Printf("%v", verboseHeapAllocs)
		}
		log.Printf("----------------------------------")
	}
}
