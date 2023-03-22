//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
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
func CompactionCycleTicker() CycleTicker {
	return NewExpTicker(compactionMinInterval, compactionMaxInterval,
		compactionBase, compactionSteps)
}

const (
	memtableFlushMinInterval = 100 * time.Millisecond
	memtableFlushMaxInterval = 5 * time.Second
	memtableFlushBase        = uint(2)
	memtableFlushSteps       = uint(5)
)

// 100ms . 258ms .. 574ms .... 1.206s ........ 2.471s ................ 5s
func MemtableFlushCycleTicker() CycleTicker {
	return NewExpTicker(memtableFlushMinInterval, memtableFlushMaxInterval,
		memtableFlushBase, memtableFlushSteps)
}
