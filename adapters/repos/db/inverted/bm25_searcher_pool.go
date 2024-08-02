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

package inverted

import (
	"math"
	"sync"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
)

type Bm25Pool struct {
	ListPool           chan *[]docPointerWithScore
	MapPool            chan *map[uint64]int
	init               bool
	lock               sync.Mutex
	sizeQueueList      *priorityqueue.Queue[time.Time]
	sizeQueueListCount uint64
	sizeQueueMap       *priorityqueue.Queue[time.Time]
	sizeQueueMapCount  uint64
	minSizeToKeep      int
	decayTime          time.Duration
}

func NewBm25Pool() *Bm25Pool {
	return &Bm25Pool{
		init:          false,
		sizeQueueList: priorityqueue.NewMax[time.Time](50),
		sizeQueueMap:  priorityqueue.NewMax[time.Time](50),
		decayTime:     time.Minute,
	}
}

func (b *Bm25Pool) Init(size, minSizeToKeep int) {
	b.lock.Lock()
	defer b.lock.Unlock()
	if b.init {
		return
	}
	b.init = true
	b.minSizeToKeep = minSizeToKeep
	b.ListPool = make(chan *[]docPointerWithScore, size)
	b.MapPool = make(chan *map[uint64]int, size)
}

func (b *Bm25Pool) Return(list []docPointerWithScore, m map[uint64]int) {
	b.lock.Lock() // prevent races in the decay calculations
	defer b.lock.Unlock()

	// the size of the returned items can be very large, so we keep track of the sizes and remove items that are too large
	// This is done in several steps:
	//   - the size of the returned items is added to a priority queue
	//   - items that are older than a minute are removed from the queue
	// => the largest requests in the last minute is the maximum capacity of any item in the pool
	b.sizeQueueList.InsertWithValue(b.sizeQueueListCount, float32(max(len(list), b.minSizeToKeep)), time.Now())
	b.sizeQueueListCount++

	for {
		if b.sizeQueueList.Len() == 0 {
			break
		}
		entry := b.sizeQueueList.Top()
		if time.Since(entry.Value) > b.decayTime {
			b.sizeQueueList.Pop()
		} else {
			break
		}
	}
	dist := float32(b.minSizeToKeep)
	if b.sizeQueueList.Len() > 0 {
		entry := b.sizeQueueList.Top()
		dist = entry.Dist
	}

	if cap(list) <= int(dist) {
		list = list[:0]
		select {
		case b.ListPool <- &list:
		default:
		}
	}

	// for maps we do not have a way to determine the capacity, so we keep track of the maximum size a given map has
	// ever had and use that as the capacity. This is stored in the map itself, with the key math.MaxUint64 which is
	// never used as a docID
	b.sizeQueueMap.InsertWithValue(b.sizeQueueMapCount, float32(max(len(m), b.minSizeToKeep)), time.Now())
	b.sizeQueueMapCount++

	for {
		if b.sizeQueueMap.Len() == 0 {
			break
		}

		entryMap := b.sizeQueueMap.Top()
		if time.Since(entryMap.Value) > b.decayTime {
			b.sizeQueueMap.Pop()
		} else {
			break
		}
	}
	length := float32(b.minSizeToKeep)
	if b.sizeQueueMap.Len() > 0 {
		entry := b.sizeQueueMap.Top()
		length = entry.Dist
	}
	if m[math.MaxUint64] <= int(length) && m != nil {
		capM := max(len(m), m[math.MaxUint64])
		clear(m)
		m[math.MaxUint64] = capM
		select {
		case b.MapPool <- &m:
		default:
		}
	}
}

func (b *Bm25Pool) Close() {
	b.lock.Lock()
	defer b.lock.Unlock()

	if !b.init {
		return
	}
	b.init = false
	close(b.ListPool)
	close(b.MapPool)
}
