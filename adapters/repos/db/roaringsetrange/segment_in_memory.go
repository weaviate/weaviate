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

func (s *SegmentInMemory) Reader() (reader InnerReader, release func()) {
	// TODO aliszka:roaringrange optimize locking?
	s.lock.RLock()
	return newSegmentInMemoryReader(s.bitmaps), s.lock.RUnlock
}

// -----------------------------------------------------------------------------

type segmentInMemoryReader struct {
	bitmaps rangeBitmaps
}

func newSegmentInMemoryReader(bitmaps rangeBitmaps) *segmentInMemoryReader {
	return &segmentInMemoryReader{bitmaps: bitmaps}
}

func (r *segmentInMemoryReader) Read(ctx context.Context, value uint64, operator filters.Operator,
) (roaringset.BitmapLayer, error) {
	if err := ctx.Err(); err != nil {
		return roaringset.BitmapLayer{}, err
	}

	switch operator {
	case filters.OperatorEqual:
		return r.readEqual(value), nil

	case filters.OperatorNotEqual:
		return r.readNotEqual(value), nil

	case filters.OperatorLessThan:
		return r.readLessThan(value), nil

	case filters.OperatorLessThanEqual:
		return r.readLessThanEqual(value), nil

	case filters.OperatorGreaterThan:
		return r.readGreaterThan(value), nil

	case filters.OperatorGreaterThanEqual:
		return r.readGreaterThanEqual(value), nil

	default:
		return roaringset.BitmapLayer{}, fmt.Errorf("operator %v not supported for segment-in-memory of strategy %q",
			operator.Name(), "roaringsetrange") // TODO move strategies to separate package?
	}
}

func (r *segmentInMemoryReader) readEqual(value uint64) roaringset.BitmapLayer {
	if value == 0 {
		return r.readLessThanEqual(value)
	}
	if value == math.MaxUint64 {
		return r.readGreaterThanEqual(value)
	}

	eq := r.mergeBetween(value, value+1)
	return roaringset.BitmapLayer{Additions: eq}
}

func (r *segmentInMemoryReader) readNotEqual(value uint64) roaringset.BitmapLayer {
	if value == 0 {
		return r.readGreaterThan(value)
	}
	if value == math.MaxUint64 {
		return r.readLessThan(value)
	}

	eq := r.mergeBetween(value, value+1)
	neq := r.bitmaps[0].Clone().AndNotConc(eq, concurrency.SROAR_MERGE)
	return roaringset.BitmapLayer{Additions: neq}
}

func (r *segmentInMemoryReader) readLessThan(value uint64) roaringset.BitmapLayer {
	if value == 0 {
		// no value is < 0
		return roaringset.BitmapLayer{Additions: sroar.NewBitmap()}
	}

	gte := r.mergeGreaterThanEqual(value)
	lt := r.bitmaps[0].Clone().AndNotConc(gte, concurrency.SROAR_MERGE)
	return roaringset.BitmapLayer{Additions: lt}
}

func (r *segmentInMemoryReader) readLessThanEqual(value uint64) roaringset.BitmapLayer {
	if value == math.MaxUint64 {
		// all values are <= max uint64
		return roaringset.BitmapLayer{Additions: r.bitmaps[0].Clone()}
	}

	gte1 := r.mergeGreaterThanEqual(value + 1)
	lte := r.bitmaps[0].Clone().AndNotConc(gte1, concurrency.SROAR_MERGE)
	return roaringset.BitmapLayer{Additions: lte}
}

func (r *segmentInMemoryReader) readGreaterThan(value uint64) roaringset.BitmapLayer {
	if value == math.MaxUint64 {
		// no value is > max uint64
		return roaringset.BitmapLayer{Additions: sroar.NewBitmap()}
	}

	gte1 := r.mergeGreaterThanEqual(value + 1)
	return roaringset.BitmapLayer{Additions: gte1}
}

func (r *segmentInMemoryReader) readGreaterThanEqual(value uint64) roaringset.BitmapLayer {
	gte := r.mergeGreaterThanEqual(value)
	return roaringset.BitmapLayer{Additions: gte}
}

func (r *segmentInMemoryReader) mergeGreaterThanEqual(value uint64) *sroar.Bitmap {
	// TODO aliszka:roaringrange use buf pool
	result := r.bitmaps[0].Clone()
	ANDed := false

	for bit := 1; bit < len(r.bitmaps); bit++ {
		if value&(1<<(bit-1)) != 0 {
			result.AndConc(r.bitmaps[bit], concurrency.SROAR_MERGE)
			ANDed = true
		} else if ANDed {
			result.OrConc(r.bitmaps[bit], concurrency.SROAR_MERGE)
		}
	}
	return result
}

func (r *segmentInMemoryReader) mergeBetween(valueMinInc, valueMaxExc uint64) *sroar.Bitmap {
	// TODO aliszka:roaringrange use buf pool
	resultMin := r.bitmaps[0].Clone()
	resultMax := r.bitmaps[0].Clone()
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

	return resultMin.AndNotConc(resultMax, concurrency.SROAR_MERGE)
}

// -----------------------------------------------------------------------------

type rangeBitmaps [65]*sroar.Bitmap
