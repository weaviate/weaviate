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

// cascadeStart is where a bit-plane cascade begins: the seed plane, the next
// bit to merge, and whether the result is already narrowed below plane 0. An
// unnarrowed result is unchanged by OR, so those leading merges are skipped.
type cascadeStart struct {
	seed     *sroar.Bitmap
	nextBit  int
	narrowed bool
}

// cascadeSeed returns where value's cascade starts. Every plane is a subset of
// plane 0, so seeding from the lowest set bit's plane directly skips the
// whole-shard AND that would otherwise produce it. With CascadeSeedEnabledEnv
// switched off the cascade starts at plane 0 and runs exactly as v1.37 ships
// it.
func (r *segmentInMemoryReader) cascadeSeed(value uint64) cascadeStart {
	if !cascadeSeedEnabled {
		return cascadeStart{seed: r.bitmaps[0], nextBit: 1}
	}
	if value == 0 {
		// Every doc is >= 0, so plane 0 is already the whole answer and there
		// is nothing left to cascade: nextBit past the last plane leaves the
		// merge loop with no iterations. The early return is also what keeps
		// the lowest-set-bit lookup below from indexing past the last plane,
		// since 0 has no set bit to find.
		return cascadeStart{seed: r.bitmaps[0], nextBit: len(r.bitmaps)}
	}

	bit := bits.TrailingZeros64(value) + 1
	assertPlaneIsSubsetOfPlaneZero(r.bitmaps, bit)
	return cascadeStart{seed: r.bitmaps[bit], nextBit: bit + 1, narrowed: true}
}

// cloneSeed buffers from plane 0's size, not the seed's, so later merges keep
// the same growth headroom as before seeding.
func (r *segmentInMemoryReader) cloneSeed(seed *sroar.Bitmap) (*sroar.Bitmap, func()) {
	buf, release := r.bufPool.Get(max(seed.LenInBytes(), r.bitmaps[0].LenInBytes()))
	return seed.CloneToBuf(buf), release
}

func (r *segmentInMemoryReader) mergeGreaterThanEqual(value uint64, conc int) (*sroar.Bitmap, func()) {
	start := r.cascadeSeed(value)
	result, release := r.cloneSeed(start.seed)
	anded := start.narrowed

	for bit := start.nextBit; bit < len(r.bitmaps); bit++ {
		if value&(1<<(bit-1)) != 0 {
			result.AndConc(r.bitmaps[bit], conc)
			anded = true
		} else if anded {
			result.OrConc(r.bitmaps[bit], conc)
		}
	}
	return result, release
}

func (r *segmentInMemoryReader) mergeBetween(valueMinInc, valueMaxExc uint64, conc int) (*sroar.Bitmap, func()) {
	startMin := r.cascadeSeed(valueMinInc)
	startMax := r.cascadeSeed(valueMaxExc)

	resultMin, releaseMin := r.cloneSeed(startMin.seed)
	resultMax, releaseMax := r.cloneSeed(startMax.seed)
	defer releaseMax()
	andedMin := startMin.narrowed
	andedMax := startMax.narrowed

	// one loop for both cascades: each plane is read once despite the two
	// starting at different bits
	for bit := min(startMin.nextBit, startMax.nextBit); bit < len(r.bitmaps); bit++ {
		var b uint64 = 1 << (bit - 1)

		if bit >= startMin.nextBit {
			if valueMinInc&b != 0 {
				resultMin.AndConc(r.bitmaps[bit], conc)
				andedMin = true
			} else if andedMin {
				resultMin.OrConc(r.bitmaps[bit], conc)
			}
		}

		if bit >= startMax.nextBit {
			if valueMaxExc&b != 0 {
				resultMax.AndConc(r.bitmaps[bit], conc)
				andedMax = true
			} else if andedMax {
				resultMax.OrConc(r.bitmaps[bit], conc)
			}
		}
	}

	return resultMin.AndNotConc(resultMax, conc), releaseMin
}

// -----------------------------------------------------------------------------

type rangeBitmaps [65]*sroar.Bitmap

var noopRelease = func() {}
