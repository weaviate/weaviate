//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package roaringsetrange

import (
	"context"
	"fmt"
	"math"
	"math/bits"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	entsync "github.com/weaviate/weaviate/entities/sync"
)

type SegmentInMemory struct {
	logger logrus.FieldLogger

	bitmaps       rangeBitmaps
	bitmapsLock   *entsync.ReadPreferringRWMutex
	memtables     []*Memtable // flushed memtables, waiting to be merged into bitmaps
	memtablesLock *sync.Mutex
}

func NewSegmentInMemory(logger logrus.FieldLogger) *SegmentInMemory {
	s := &SegmentInMemory{
		logger:        logger,
		bitmapsLock:   entsync.NewReadPreferringRWMutex(),
		memtables:     make([]*Memtable, 0, 8),
		memtablesLock: new(sync.Mutex),
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

	s.bitmapsLock.Lock()
	defer s.bitmapsLock.Unlock()

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

func (s *SegmentInMemory) MergeMemtableEventually(memtable *Memtable) {
	s.memtablesLock.Lock()
	s.memtables = append(s.memtables, memtable)
	ln := len(s.memtables)
	s.memtablesLock.Unlock()

	// run background merge only once,
	// handle also all memtables added while merge is performed
	if ln == 1 {
		errors.GoWrapper(s.mergeMemtables, s.logger)
	}
}

func (s *SegmentInMemory) mergeMemtables() {
	s.bitmapsLock.Lock()
	defer s.bitmapsLock.Unlock()

	i := 0
	for {
		s.memtablesLock.Lock()
		if i == len(s.memtables) {
			s.memtables = s.memtables[:0]
			s.memtablesLock.Unlock()
			return
		}
		memtable := s.memtables[i]
		i++
		s.memtablesLock.Unlock()

		nodes := memtable.Nodes()
		if len(nodes) == 0 {
			continue
		}
		if deletions := nodes[0].Deletions; !deletions.IsEmpty() {
			for key := range s.bitmaps {
				s.bitmaps[key].AndNotConc(deletions, concurrency.SROAR_MERGE)
			}
		}
		for _, node := range nodes {
			s.bitmaps[node.Key].OrConc(node.Additions, concurrency.SROAR_MERGE)
		}
	}
}

func (s *SegmentInMemory) countPendingMemtables() int {
	s.memtablesLock.Lock()
	defer s.memtablesLock.Unlock()

	return len(s.memtables)
}

func (s *SegmentInMemory) Size() int {
	size := 0
	for i := range s.bitmaps {
		size += s.bitmaps[i].LenInBytes()
	}
	return size
}

func (s *SegmentInMemory) Readers(bufPool roaringset.BitmapBufPool) (readers []InnerReader, release func()) {
	s.bitmapsLock.RLock()
	s.memtablesLock.Lock()
	memtables := s.memtables
	s.memtablesLock.Unlock()

	readers = make([]InnerReader, 1+len(memtables))
	readers[0] = &segmentInMemoryReader{
		bitmaps: s.bitmaps,
		bufPool: bufPool,
	}
	for i := range memtables {
		readers[1+i] = NewMemtableReader(memtables[i])
	}
	return readers, s.bitmapsLock.RUnlock
}

// -----------------------------------------------------------------------------

type segmentInMemoryReader struct {
	bitmaps rangeBitmaps
	bufPool roaringset.BitmapBufPool
}

func (r *segmentInMemoryReader) Read(ctx context.Context, value uint64, operator filters.Operator,
) (roaringset.BitmapLayer, func(), error) {
	if err := ctx.Err(); err != nil {
		return roaringset.BitmapLayer{}, noopRelease, err
	}

	// conc is the per-query merge budget, threaded through every read/merge helper below.
	conc := concurrency.BudgetFromCtxCapped(ctx, concurrency.SROAR_MERGE)

	switch operator {
	case filters.OperatorEqual:
		bm, release := r.readEqual(value, conc)
		return bm, release, nil

	case filters.OperatorNotEqual:
		bm, release := r.readNotEqual(value, conc)
		return bm, release, nil

	case filters.OperatorLessThan:
		bm, release := r.readLessThan(value, conc)
		return bm, release, nil

	case filters.OperatorLessThanEqual:
		bm, release := r.readLessThanEqual(value, conc)
		return bm, release, nil

	case filters.OperatorGreaterThan:
		bm, release := r.readGreaterThan(value, conc)
		return bm, release, nil

	case filters.OperatorGreaterThanEqual:
		bm, release := r.readGreaterThanEqual(value, conc)
		return bm, release, nil

	default:
		// TODO move strategies to separate package?
		return roaringset.BitmapLayer{}, noopRelease,
			fmt.Errorf("operator %v not supported for segment-in-memory of strategy %q", operator.Name(), "roaringsetrange")
	}
}

func (r *segmentInMemoryReader) readEqual(value uint64, conc int) (roaringset.BitmapLayer, func()) {
	if value == 0 {
		return r.readLessThanEqual(value, conc)
	}
	if value == math.MaxUint64 {
		return r.readGreaterThanEqual(value, conc)
	}

	eq, eqRelease := r.mergeBetween(value, value+1, conc)
	return roaringset.BitmapLayer{Additions: eq}, eqRelease
}

func (r *segmentInMemoryReader) readNotEqual(value uint64, conc int) (roaringset.BitmapLayer, func()) {
	if value == 0 {
		return r.readGreaterThan(value, conc)
	}
	if value == math.MaxUint64 {
		return r.readLessThan(value, conc)
	}

	eq, eqRelease := r.mergeBetween(value, value+1, conc)
	defer eqRelease()

	neq, neqRelease := r.bufPool.CloneToBuf(r.bitmaps[0])
	neq.AndNotConc(eq, conc)
	return roaringset.BitmapLayer{Additions: neq}, neqRelease
}

func (r *segmentInMemoryReader) readLessThan(value uint64, conc int) (roaringset.BitmapLayer, func()) {
	if value == 0 {
		// no value is < 0
		return roaringset.BitmapLayer{Additions: sroar.NewBitmap()}, noopRelease
	}

	gte, gteRelease := r.mergeGreaterThanEqual(value, conc)
	defer gteRelease()

	lt, ltRelease := r.bufPool.CloneToBuf(r.bitmaps[0])
	lt.AndNotConc(gte, conc)
	return roaringset.BitmapLayer{Additions: lt}, ltRelease
}

func (r *segmentInMemoryReader) readLessThanEqual(value uint64, conc int) (roaringset.BitmapLayer, func()) {
	if value == math.MaxUint64 {
		all, allRelease := r.bufPool.CloneToBuf(r.bitmaps[0])
		// all values are <= max uint64
		return roaringset.BitmapLayer{Additions: all}, allRelease
	}

	gte1, gte1Release := r.mergeGreaterThanEqual(value+1, conc)
	defer gte1Release()

	lte, lteRelease := r.bufPool.CloneToBuf(r.bitmaps[0])
	lte.AndNotConc(gte1, conc)
	return roaringset.BitmapLayer{Additions: lte}, lteRelease
}

func (r *segmentInMemoryReader) readGreaterThan(value uint64, conc int) (roaringset.BitmapLayer, func()) {
	if value == math.MaxUint64 {
		// no value is > max uint64
		return roaringset.BitmapLayer{Additions: sroar.NewBitmap()}, noopRelease
	}

	gte1, gte1Release := r.mergeGreaterThanEqual(value+1, conc)
	return roaringset.BitmapLayer{Additions: gte1}, gte1Release
}

func (r *segmentInMemoryReader) readGreaterThanEqual(value uint64, conc int) (roaringset.BitmapLayer, func()) {
	if value == 0 {
		all, allRelease := r.bufPool.CloneToBuf(r.bitmaps[0])
		// all values are >= 0
		return roaringset.BitmapLayer{Additions: all}, allRelease
	}

	gte, gteRelease := r.mergeGreaterThanEqual(value, conc)
	return roaringset.BitmapLayer{Additions: gte}, gteRelease
}

// cascadeSeed returns the plane the bit-sliced cascade for value starts from,
// and the first bit that still needs merging.
//
// The cascade's first operation is always an AND against plane f+1, where f is
// value's lowest set bit, because the OR branch is gated on having ANDed once.
// Every plane is a subset of plane 0 (plane 0's additions are the union of all
// additions, and deletions are removed from all 65 planes uniformly), so
// clone(plane 0) AND plane f+1 is plane f+1, and one whole-shard pass can be
// dropped. value 0 has no set bit at all: its cascade never leaves plane 0.
func (r *segmentInMemoryReader) cascadeSeed(value uint64) (seed *sroar.Bitmap, nextBit int) {
	if value == 0 {
		return r.bitmaps[0], len(r.bitmaps)
	}

	bit := bits.TrailingZeros64(value) + 1
	assertPlaneIsSubsetOfPlaneZero(r.bitmaps, bit)
	return r.bitmaps[bit], bit + 1
}

// cloneSeed sizes the pooled buffer to plane 0 rather than to the seed, so the
// merges downstream keep the growth headroom the plane-0 clone gave them.
func (r *segmentInMemoryReader) cloneSeed(seed *sroar.Bitmap) (*sroar.Bitmap, func()) {
	buf, release := r.bufPool.Get(max(seed.LenInBytes(), r.bitmaps[0].LenInBytes()))
	return seed.CloneToBuf(buf), release
}

func (r *segmentInMemoryReader) mergeGreaterThanEqual(value uint64, conc int) (*sroar.Bitmap, func()) {
	seed, bit := r.cascadeSeed(value)
	result, release := r.cloneSeed(seed)

	for ; bit < len(r.bitmaps); bit++ {
		if value&(1<<(bit-1)) != 0 {
			result.AndConc(r.bitmaps[bit], conc)
		} else {
			result.OrConc(r.bitmaps[bit], conc)
		}
	}
	return result, release
}

func (r *segmentInMemoryReader) mergeBetween(valueMinInc, valueMaxExc uint64, conc int) (*sroar.Bitmap, func()) {
	seedMin, bitMin := r.cascadeSeed(valueMinInc)
	seedMax, bitMax := r.cascadeSeed(valueMaxExc)

	resultMin, releaseMin := r.cloneSeed(seedMin)
	resultMax, releaseMax := r.cloneSeed(seedMax)
	defer releaseMax()

	// the two cascades share one loop so each plane is read once, even though
	// they start at different bits
	for bit := min(bitMin, bitMax); bit < len(r.bitmaps); bit++ {
		var b uint64 = 1 << (bit - 1)

		if bit >= bitMin {
			if valueMinInc&b != 0 {
				resultMin.AndConc(r.bitmaps[bit], conc)
			} else {
				resultMin.OrConc(r.bitmaps[bit], conc)
			}
		}

		if bit >= bitMax {
			if valueMaxExc&b != 0 {
				resultMax.AndConc(r.bitmaps[bit], conc)
			} else {
				resultMax.OrConc(r.bitmaps[bit], conc)
			}
		}
	}

	return resultMin.AndNotConc(resultMax, conc), releaseMin
}

// -----------------------------------------------------------------------------

type rangeBitmaps [65]*sroar.Bitmap

var noopRelease = func() {}
