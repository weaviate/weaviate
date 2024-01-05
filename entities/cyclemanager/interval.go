//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package cyclemanager

import "time"

const (
	compactionMinInterval = 3 * time.Second
	compactionMaxInterval = time.Minute
	compactionBase        = uint(2)
	compactionSteps       = uint(4)
)

// 3s . 6.8s .. 14.4s .... 29.6s ........ 60s
func CompactionCycleIntervals() CycleIntervals {
	return NewExpIntervals(compactionMinInterval, compactionMaxInterval,
		compactionBase, compactionSteps)
}

// run cycle ticker with fixed minimal interval and let each shard
// take care of its intervals
func CompactionCycleTicker() CycleTicker {
	return NewFixedTicker(compactionMinInterval)
}

const (
	memtableFlushMinInterval = 100 * time.Millisecond
	memtableFlushMaxInterval = 5 * time.Second
	memtableFlushBase        = uint(2)
	memtableFlushSteps       = uint(5)
)

// 100ms . 258ms .. 574ms .... 1.206s ........ 2.471s ................ 5s
func MemtableFlushCycleIntervals() CycleIntervals {
	return NewExpIntervals(memtableFlushMinInterval, memtableFlushMaxInterval,
		memtableFlushBase, memtableFlushSteps)
}

// run cycle ticker with fixed minimal interval and let each shard
// take care of its intervals
func MemtableFlushCycleTicker() CycleTicker {
	return NewFixedTicker(memtableFlushMinInterval)
}

const (
	geoCommitLoggerMinInterval = 10 * time.Second
	geoCommitLoggerMaxInterval = 60 * time.Second
	geoCommitLoggerBase        = uint(2)
	geoCommitLoggerSteps       = uint(4)
)

// 10s . 13.3s .. 20s .... 33.3s ........ 60s
func GeoCommitLoggerCycleIntervals() CycleIntervals {
	return NewExpIntervals(geoCommitLoggerMinInterval, geoCommitLoggerMaxInterval,
		geoCommitLoggerBase, geoCommitLoggerSteps)
}

// run cycle ticker with fixed minimal interval and let each shard
// take care of its intervals
func GeoCommitLoggerCycleTicker() CycleTicker {
	return NewFixedTicker(geoCommitLoggerMinInterval)
}

const (
	hnswCommitLoggerMinInterval = 500 * time.Millisecond
	hnswCommitLoggerMaxInterval = 10 * time.Second
	hnswCommitLoggerBase        = uint(2)
	hnswCommitLoggerSteps       = uint(5)
)

// 500ms . 806ms .. 1.42s .... 2.65s ........ 5.1s ................10s
func HnswCommitLoggerCycleIntervals() CycleIntervals {
	return NewExpIntervals(hnswCommitLoggerMinInterval, hnswCommitLoggerMaxInterval,
		hnswCommitLoggerBase, hnswCommitLoggerSteps)
}

// run cycle ticker with fixed minimal interval and let each shard
// take care of its intervals
func HnswCommitLoggerCycleTicker() CycleTicker {
	return NewFixedTicker(hnswCommitLoggerMinInterval)
}
