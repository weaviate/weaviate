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

package roaringsetrange

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/filters"
)

type SegmentInMemory struct {
	lock    *sync.RWMutex
	bitmaps rangeBitmaps
}

func NewSegmentInMemory() *SegmentInMemory {
	s := &SegmentInMemory{
		lock: new(sync.RWMutex),
	}
	for key := range s.bitmaps {
		s.bitmaps[key] = sroar.NewBitmap()
	}
	return s
}

func (s *SegmentInMemory) MergeSegmentByCursor(cursor SegmentCursor) error {
	key, layer, ok := cursor.First()
	if !ok {
		// empty segment, nothing to merge
		return nil
	}
	if key != 0 {
		return fmt.Errorf("invalid first key of merged segment")
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	if deletions := layer.Deletions; !deletions.IsEmpty() {
		for key := range s.bitmaps {
			s.bitmaps[key].AndNotConc(deletions, concurrency.SROAR_MERGE)
		}
	}
	for ; ok; key, layer, ok = cursor.Next() {
		s.bitmaps[key].OrConc(layer.Additions, concurrency.SROAR_MERGE)
	}
	return nil
}

func (s *SegmentInMemory) MergeMemtable(memtable *Memtable) error {
	nodes := memtable.Nodes()
	if len(nodes) == 0 {
		// empty memtable, nothing to merge
		return nil
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	if deletions := nodes[0].Deletions; !deletions.IsEmpty() {
		for key := range s.bitmaps {
			s.bitmaps[key].AndNotConc(deletions, concurrency.SROAR_MERGE)
		}
	}
	for _, node := range nodes {
		s.bitmaps[node.Key].OrConc(node.Additions, concurrency.SROAR_MERGE)
	}
	return nil
}

func (s *SegmentInMemory) Size() int {
	size := 0
	for i := range s.bitmaps {
		size += s.bitmaps[i].LenInBytes()
	}
	return size
}

// -----------------------------------------------------------------------------

type segmentInMemoryReader struct {
	bitmaps rangeBitmaps
	bufPool roaringset.BitmapBufPool
}

func NewSegmentInMemoryReader(s *SegmentInMemory, bufPool roaringset.BitmapBufPool,
) (reader *segmentInMemoryReader, release func()) {
	// TODO aliszka:roaringrange optimize locking?
	s.lock.RLock()
	return &segmentInMemoryReader{
		bitmaps: s.bitmaps,
		bufPool: bufPool,
	}, s.lock.RUnlock
}

func (r *segmentInMemoryReader) Read(ctx context.Context, value uint64, operator filters.Operator,
) (roaringset.BitmapLayer, func(), error) {
	if err := ctx.Err(); err != nil {
		return roaringset.BitmapLayer{}, noopRelease, err
	}

	switch operator {
	case filters.OperatorEqual:
		bm, release := r.readEqual(value)
		return bm, release, nil

	case filters.OperatorNotEqual:
		bm, release := r.readNotEqual(value)
		return bm, release, nil

	case filters.OperatorLessThan:
		bm, release := r.readLessThan(value)
		return bm, release, nil

	case filters.OperatorLessThanEqual:
		bm, release := r.readLessThanEqual(value)
		return bm, release, nil

	case filters.OperatorGreaterThan:
		bm, release := r.readGreaterThan(value)
		return bm, release, nil

	case filters.OperatorGreaterThanEqual:
		bm, release := r.readGreaterThanEqual(value)
		return bm, release, nil

	default:
		// TODO move strategies to separate package?
		return roaringset.BitmapLayer{}, noopRelease,
			fmt.Errorf("operator %v not supported for segment-in-memory of strategy %q", operator.Name(), "roaringsetrange")
	}
}

func (r *segmentInMemoryReader) readEqual(value uint64) (roaringset.BitmapLayer, func()) {
	if value == 0 {
		return r.readLessThanEqual(value)
	}
	if value == math.MaxUint64 {
		return r.readGreaterThanEqual(value)
	}

	eq, eqRelease := r.mergeBetween(value, value+1)
	return roaringset.BitmapLayer{Additions: eq}, eqRelease
}

func (r *segmentInMemoryReader) readNotEqual(value uint64) (roaringset.BitmapLayer, func()) {
	if value == 0 {
		return r.readGreaterThan(value)
	}
	if value == math.MaxUint64 {
		return r.readLessThan(value)
	}

	eq, eqRelease := r.mergeBetween(value, value+1)
	defer eqRelease()

	neq, neqRelease := r.bufPool.CloneToBuf(r.bitmaps[0])
	neq.AndNotConc(eq, concurrency.SROAR_MERGE)
	return roaringset.BitmapLayer{Additions: neq}, neqRelease
}

func (r *segmentInMemoryReader) readLessThan(value uint64) (roaringset.BitmapLayer, func()) {
	if value == 0 {
		// no value is < 0
		return roaringset.BitmapLayer{Additions: sroar.NewBitmap()}, noopRelease
	}

	gte, gteRelease := r.mergeGreaterThanEqual(value)
	defer gteRelease()

	lt, ltRelease := r.bufPool.CloneToBuf(r.bitmaps[0])
	lt.AndNotConc(gte, concurrency.SROAR_MERGE)
	return roaringset.BitmapLayer{Additions: lt}, ltRelease
}

func (r *segmentInMemoryReader) readLessThanEqual(value uint64) (roaringset.BitmapLayer, func()) {
	if value == math.MaxUint64 {
		all, allRelease := r.bufPool.CloneToBuf(r.bitmaps[0])
		// all values are <= max uint64
		return roaringset.BitmapLayer{Additions: all}, allRelease
	}

	gte1, gte1Release := r.mergeGreaterThanEqual(value + 1)
	defer gte1Release()

	lte, lteRelease := r.bufPool.CloneToBuf(r.bitmaps[0])
	lte.AndNotConc(gte1, concurrency.SROAR_MERGE)
	return roaringset.BitmapLayer{Additions: lte}, lteRelease
}

func (r *segmentInMemoryReader) readGreaterThan(value uint64) (roaringset.BitmapLayer, func()) {
	if value == math.MaxUint64 {
		// no value is > max uint64
		return roaringset.BitmapLayer{Additions: sroar.NewBitmap()}, noopRelease
	}

	gte1, gte1Release := r.mergeGreaterThanEqual(value + 1)
	return roaringset.BitmapLayer{Additions: gte1}, gte1Release
}

func (r *segmentInMemoryReader) readGreaterThanEqual(value uint64) (roaringset.BitmapLayer, func()) {
	gte, gteRelease := r.mergeGreaterThanEqual(value)
	return roaringset.BitmapLayer{Additions: gte}, gteRelease
}

func (r *segmentInMemoryReader) mergeGreaterThanEqual(value uint64) (*sroar.Bitmap, func()) {
	result, release := r.bufPool.CloneToBuf(r.bitmaps[0])
	ANDed := false

	for bit := 1; bit < len(r.bitmaps); bit++ {
		if value&(1<<(bit-1)) != 0 {
			result.AndConc(r.bitmaps[bit], concurrency.SROAR_MERGE)
			ANDed = true
		} else if ANDed {
			result.OrConc(r.bitmaps[bit], concurrency.SROAR_MERGE)
		}
	}
	return result, release
}

func (r *segmentInMemoryReader) mergeBetween(valueMinInc, valueMaxExc uint64) (*sroar.Bitmap, func()) {
	resultMin, releaseMin := r.bufPool.CloneToBuf(r.bitmaps[0])
	resultMax, releaseMax := r.bufPool.CloneToBuf(r.bitmaps[0])
	defer releaseMax()
	ANDedMin := false
	ANDedMax := false

	for bit := 1; bit < len(r.bitmaps); bit++ {
		var b uint64 = 1 << (bit - 1)

		if valueMinInc&b != 0 {
			resultMin.AndConc(r.bitmaps[bit], concurrency.SROAR_MERGE)
			ANDedMin = true
		} else if ANDedMin {
			resultMin.OrConc(r.bitmaps[bit], concurrency.SROAR_MERGE)
		}

		if valueMaxExc&b != 0 {
			resultMax.AndConc(r.bitmaps[bit], concurrency.SROAR_MERGE)
			ANDedMax = true
		} else if ANDedMax {
			resultMax.OrConc(r.bitmaps[bit], concurrency.SROAR_MERGE)
		}
	}

	return resultMin.AndNotConc(resultMax, concurrency.SROAR_MERGE), releaseMin
}

// -----------------------------------------------------------------------------

type rangeBitmaps [65]*sroar.Bitmap

var noopRelease = func() {}
