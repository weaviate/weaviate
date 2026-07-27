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

	// generation is leafCache's invalidation token: it only changes inside
	// mutateBitmaps and is read under RLock, so it's never stale relative to
	// the bitmaps it labels. leafCache is nil when caching is disabled.
	generation uint64
	leafCache  *leafCache
}

// mutateBitmaps is the only place the bit planes may be written. It bumps
// generation while holding the write lock, so leafCache can never be left
// serving a stale allow-list: skip this path and queries silently return
// wrong results, with no panic and no log. Enforced, not conventional — see
// TestPlanesAreOnlyMutatedThroughMutateBitmaps.
func (s *SegmentInMemory) mutateBitmaps(fn func(bitmaps *rangeBitmaps)) {
	s.bitmapsLock.Lock()
	defer s.bitmapsLock.Unlock()
	// deferred so an early return or a panic inside fn cannot skip it
	defer func() { s.generation++ }()

	fn(&s.bitmaps)
}

func NewSegmentInMemory(logger logrus.FieldLogger) *SegmentInMemory {
	s := &SegmentInMemory{
		logger:        logger,
		bitmapsLock:   entsync.NewReadPreferringRWMutex(),
		memtables:     make([]*Memtable, 0, 8),
		memtablesLock: new(sync.Mutex),
		leafCache:     newLeafCache(leafCacheMaxMemory),
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

	s.mutateBitmaps(func(bitmaps *rangeBitmaps) {
		if deletions := layer.Deletions; !deletions.IsEmpty() {
			for key := range bitmaps {
				bitmaps[key].AndNotConc(deletions, concurrency.SROAR_MERGE)
			}
		}
		for ; ok; key, layer, ok = cursor.Next() {
			bitmaps[key].OrConc(layer.Additions, concurrency.SROAR_MERGE)
		}
	})
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
	s.mutateBitmaps(func(bitmaps *rangeBitmaps) {
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
				for key := range bitmaps {
					bitmaps[key].AndNotConc(deletions, concurrency.SROAR_MERGE)
				}
			}
			for _, node := range nodes {
				bitmaps[node.Key].OrConc(node.Additions, concurrency.SROAR_MERGE)
			}
		}
	})
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
		bitmaps:    s.bitmaps,
		bufPool:    bufPool,
		cache:      s.leafCache,
		generation: s.generation,
	}
	for i := range memtables {
		readers[1+i] = NewMemtableReader(memtables[i])
	}
	return readers, s.bitmapsLock.RUnlock
}

// -----------------------------------------------------------------------------

// segmentInMemoryReader is only valid while the read lock from Readers() is
// held; that lock is what makes generation a sound cache token.
type segmentInMemoryReader struct {
	bitmaps    rangeBitmaps
	bufPool    roaringset.BitmapBufPool
	cache      *leafCache
	generation uint64
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

// cascadeSeed picks where value's cascade starts: every plane is a subset of
// plane 0, so the lowest set bit's plane skips a redundant AND against plane
// 0. Resolved before the cache probe, since a hit skips the cascade and any
// invariant checked inside it would stop covering cached predicates.
func (r *segmentInMemoryReader) cascadeSeed(value uint64) cascadeStart {
	if !cascadeSeedEnabled {
		return cascadeStart{seed: r.bitmaps[0], nextBit: 1}
	}
	if value == 0 {
		// no set bit, so every plane would be OR-ed into a result that still
		// equals plane 0: the whole cascade is a no-op
		return cascadeStart{seed: r.bitmaps[0], nextBit: len(r.bitmaps)}
	}

	bit := bits.TrailingZeros64(value) + 1
	assertPlaneIsSubsetOfPlaneZero(r.bitmaps, bit)
	return cascadeStart{seed: r.bitmaps[bit], nextBit: bit + 1, narrowed: true}
}

// cloneSeed buffers from plane 0's size, not the seed's, so later merges keep
// the same room as before seeding. CloneBufSize adds the growth headroom a
// direct Get would drop, which CloneToBuf cannot supply here because the buffer
// is sized from a bound wider than the bitmap being cloned.
func (r *segmentInMemoryReader) cloneSeed(seed *sroar.Bitmap) (*sroar.Bitmap, func()) {
	buf, release := r.bufPool.Get(
		roaringset.CloneBufSize(max(seed.LenInBytes(), r.bitmaps[0].LenInBytes())))
	return seed.CloneToBuf(buf), release
}

// cloneCached hands out a private copy of a cached leaf, sized to the widest
// plane rather than the leaf, plus CloneBufSize's growth headroom. That is the
// room the uncached path leaves for downstream memtable ORs; a raw Get on the
// bare max would drop it.
func (r *segmentInMemoryReader) cloneCached(bm *sroar.Bitmap) (*sroar.Bitmap, func()) {
	buf, release := r.bufPool.Get(
		roaringset.CloneBufSize(max(bm.LenInBytes(), r.bitmaps[0].LenInBytes())))
	return bm.CloneToBuf(buf), release
}

// leafBytesBound is what probe charges before the leaf exists: plane 0, since
// every plane is its subset regardless of where the cascade seeded. Charging
// the seed instead would under-estimate, because the cascade ORs higher planes
// back in and a leaf can far outgrow its seed.
func (r *segmentInMemoryReader) leafBytesBound() int {
	return r.bitmaps[0].LenInBytes()
}

func (r *segmentInMemoryReader) mergeGreaterThanEqual(value uint64, conc int) (*sroar.Bitmap, func()) {
	start := r.cascadeSeed(value)
	key := leafKey{kind: leafGreaterThanEqual, valueMin: value}

	cached, admit := r.cache.probe(r.generation, key, r.leafBytesBound())
	if cached != nil {
		return r.cloneCached(cached)
	}

	result, release := r.mergeGreaterThanEqualUncached(value, start, conc)
	if admit {
		r.cache.store(r.generation, key, result.Clone())
	}
	return result, release
}

func (r *segmentInMemoryReader) mergeGreaterThanEqualUncached(value uint64, start cascadeStart, conc int,
) (*sroar.Bitmap, func()) {
	observeCascadeSeed(start)
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
	key := leafKey{kind: leafBetween, valueMin: valueMinInc, valueMax: valueMaxExc}

	cached, admit := r.cache.probe(r.generation, key, r.leafBytesBound())
	if cached != nil {
		return r.cloneCached(cached)
	}

	result, release := r.mergeBetweenUncached(valueMinInc, valueMaxExc, startMin, startMax, conc)
	if admit {
		r.cache.store(r.generation, key, result.Clone())
	}
	return result, release
}

func (r *segmentInMemoryReader) mergeBetweenUncached(valueMinInc, valueMaxExc uint64,
	startMin, startMax cascadeStart, conc int,
) (*sroar.Bitmap, func()) {
	observeCascadeSeed(startMin)
	observeCascadeSeed(startMax)

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
