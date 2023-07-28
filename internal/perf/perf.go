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
	"time"

	"gonum.org/v1/gonum/floats"
	"gonum.org/v1/gonum/stat"
)

var (
	timings         map[string][]float64
	perfInitialized bool
)

func Init() {
	timings = make(map[string][]float64, 20)
}
func TimeTrack(start time.Time, name string) {
	if !perfInitialized {
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

func PrintTimings(verbose bool) {
	if !perfInitialized {
		return
	}
	for k, v := range timings {
		min := floats.Min(v)
		max := floats.Max(v)
		stdev := stat.StdDev(v, nil)
		mean := stat.Mean(v, nil)
		log.Printf("%s in ms: min %0.2f, max %0.2f, mean %0.2f, stdev %0.2f, count %d", k, min, max, mean, stdev, len(v))
		if verbose {
			log.Printf("%v", v)
		}
		log.Printf("----------------------------------")
	}
}
