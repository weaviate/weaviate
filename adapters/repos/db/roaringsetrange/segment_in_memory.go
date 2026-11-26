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
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	entsync "github.com/weaviate/weaviate/entities/sync"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

type SegmentInMemory struct {
	logger       logrus.FieldLogger
	allocChecker memwatch.AllocChecker // may be nil

	bitmaps       rangeBitmaps
	bitmapsLock   *entsync.ReadPreferringRWMutex
	memtables     []*Memtable // flushed memtables, waiting to be merged into bitmaps
	merging       bool        // mergeMemtables is running, guarded by memtablesLock
	memtablesLock *sync.Mutex

	changed         bool // compactIfDue skips compaction unless a merge changed the layers since the last one
	compactedAt     time.Time
	compactInterval time.Duration
}

// NewSegmentInMemory takes an allocChecker, which may be nil, to skip run-time
// compaction under memory pressure.
func NewSegmentInMemory(logger logrus.FieldLogger, allocChecker memwatch.AllocChecker) *SegmentInMemory {
	s := &SegmentInMemory{
		logger:        logger,
		allocChecker:  allocChecker,
		bitmapsLock:   entsync.NewReadPreferringRWMutex(),
		memtables:     make([]*Memtable, 0, 8),
		memtablesLock: new(sync.Mutex),
		// compactedAt is backdated by up to half an interval, so indexes built together compact apart
		compactedAt:     time.Now().Add(-rand.N(compactInterval / 2)),
		compactInterval: compactInterval,
	}

	for key := range s.bitmaps {
		s.bitmaps[key] = sroar.NewBitmap()
	}
	return s
}

// MergeSegmentByCursor must run before the index is published, as it takes no lock.
func (s *SegmentInMemory) MergeSegmentByCursor(cursor SegmentCursor) error {
	key, layer, ok := cursor.First()
	if !ok {
		// empty segment, nothing to merge
		return nil
	}
	if key != 0 {
		return fmt.Errorf("invalid first key of merged segment")
	}

	if deletions := layer.Deletions; !deletions.IsEmpty() {
		for key := range s.bitmaps {
			s.bitmaps[key].AndNotConc(deletions, concurrency.SROAR_MERGE)
		}
	}
	for ; ok; key, layer, ok = cursor.Next() {
		if int(key) >= len(s.bitmaps) {
			return fmt.Errorf("invalid key %d from merged segment cursor: exceeds rangeable bitmap count %d", key, len(s.bitmaps))
		}
		s.bitmaps[key].OrConc(layer.Additions, concurrency.SROAR_MERGE)
	}
	return nil
}

// MergeMemtableEventually takes a memtable nothing writes to anymore, since it is
// read without a lock.
func (s *SegmentInMemory) MergeMemtableEventually(memtable *Memtable) {
	s.memtablesLock.Lock()
	s.memtables = append(s.memtables, memtable)
	start := !s.merging
	s.merging = true
	s.memtablesLock.Unlock()

	// run background merge only once,
	// handle also all memtables added while merge is performed
	if start {
		errors.GoWrapper(s.mergeMemtables, s.logger)
	}
}

// mergeMemtables runs on one goroutine at a time, the only one to change the
// layers of a published index.
func (s *SegmentInMemory) mergeMemtables() {
	merging := true
	defer func() {
		// after a panic, the next flushed memtable starts another run
		if merging {
			s.memtablesLock.Lock()
			s.merging = false
			s.memtablesLock.Unlock()
		}
	}()

	batches := 0
	for merging {
		pending := s.mergePending()
		if batches++; pending == 0 || batches >= compactDeferBatches {
			s.compactIfDue()
			batches = 0
		}

		s.memtablesLock.Lock()
		merging = len(s.memtables) > 0
		s.merging = merging
		s.memtablesLock.Unlock()
	}
}

// mergePending merges up to mergeBatchMemtables memtables and returns how many remain.
// It builds their nodes before taking bitmapsLock, then merges and removes them under
// one bitmapsLock, so no reader sees a memtable both merged and pending, or neither.
func (s *SegmentInMemory) mergePending() (pending int) {
	s.memtablesLock.Lock()
	memtables := slices.Clone(s.memtables[:min(len(s.memtables), mergeBatchMemtables)])
	s.memtablesLock.Unlock()

	prepared := make([][]*MemtableNode, len(memtables))
	for i := range memtables {
		prepared[i] = memtables[i].Nodes()
	}

	s.bitmapsLock.Lock()
	defer s.bitmapsLock.Unlock()

	for _, nodes := range prepared {
		if len(nodes) == 0 {
			continue
		}
		s.changed = true
		if deletions := nodes[0].Deletions; !deletions.IsEmpty() {
			for key := range s.bitmaps {
				s.bitmaps[key].AndNotConc(deletions, concurrency.SROAR_MERGE)
			}
		}
		for _, node := range nodes {
			s.bitmaps[node.Key].OrConc(node.Additions, concurrency.SROAR_MERGE)
		}
	}

	s.memtablesLock.Lock()
	defer s.memtablesLock.Unlock()

	// memtables are only ever appended, so the merged ones are still the first
	s.memtables = slices.Delete(s.memtables, 0, len(prepared))
	return len(s.memtables)
}

const (
	// mergeBatchMemtables bounds the memtables whose nodes mergePending holds at once.
	mergeBatchMemtables = 4
	// compactDeferBatches is how many batches a backlog may merge before compactIfDue runs
	// anyway, so readers starving the merge cannot defer compaction indefinitely.
	compactDeferBatches = 8
)

// compactIfDue swaps in compacted copies of the layers, so live memory peaks at a
// second copy of the index. Only the swap takes bitmapsLock, since the goroutine
// running it is the only one to change the layers of a published index.
func (s *SegmentInMemory) compactIfDue() {
	if !s.changed || time.Since(s.compactedAt) < s.compactInterval {
		return
	}

	if s.allocChecker != nil {
		if err := s.allocChecker.CheckAlloc(s.compactedSizeEstimate()); err != nil {
			s.logger.WithField("action", "rangeable_inmemory_compaction").
				Debugf("skipping compaction of rangeable segment-in-memory: %v", err)
			return
		}
	}

	var compacted rangeBitmaps
	s.compactInto(&compacted)

	s.bitmapsLock.Lock()
	defer s.bitmapsLock.Unlock()

	s.bitmaps = compacted
	s.changed = false
	s.compactedAt = time.Now()
}

// compactedSizeEstimate counts the space a compaction would reclaim too, which
// is known only once it has run.
func (s *SegmentInMemory) compactedSizeEstimate() int64 {
	size := 0
	for i := range s.bitmaps {
		size += s.bitmaps[i].LenInBytes()
	}
	return int64(math.Ceil(float64(size) * (1 + compactSpareRatio)))
}

func (s *SegmentInMemory) countPendingMemtables() int {
	s.memtablesLock.Lock()
	defer s.memtablesLock.Unlock()

	return len(s.memtables)
}

// IsUnpopulated reports whether the rep would serve no rows. Size() can't
// substitute: an unpopulated rep still allocates 65 empty bitmap skeletons.
// Also checks pending memtables to avoid a false positive in the post-flush
// window, where bitmaps[0] is briefly empty but Readers() already serves it.
//
// Locks bitmapsLock then memtablesLock (matches Readers()); never alongside
// the segment group's maintenanceLock.
func (s *SegmentInMemory) IsUnpopulated() bool {
	s.bitmapsLock.RLock()
	defer s.bitmapsLock.RUnlock()

	if !s.bitmaps[0].IsEmpty() {
		return false
	}

	s.memtablesLock.Lock()
	defer s.memtablesLock.Unlock()

	return len(s.memtables) == 0
}

// Size counts the bytes the layers hold, headroom included.
func (s *SegmentInMemory) Size() int {
	s.bitmapsLock.RLock()
	defer s.bitmapsLock.RUnlock()

	size := 0
	for i := range s.bitmaps {
		size += s.bitmaps[i].CapInBytes()
	}
	return size
}

// compactSpareRatio sizes the spare capacity of each compacted layer. A larger
// ratio holds more idle memory until merges fill it, a smaller one copies a whole layer
// under the write lock sooner. BenchmarkSegmentInMemoryCompactHeadroom sweeps it.
const compactSpareRatio = 0.10

// compactInterval is the least time compactIfDue lets pass between run-time compactions. Dead
// space is unknown without compacting, so an occasional copy that reclaims little is accepted.
const compactInterval = 12 * time.Hour

// compactInto writes to dst a copy of each layer without the space deleted values
// left behind, in a buffer (1+compactSpareRatio) times its size. Later growths
// follow sroar's own policy.
func (s *SegmentInMemory) compactInto(dst *rangeBitmaps) {
	for i := range s.bitmaps {
		dst[i] = s.bitmaps[i].CompactedTo(nil, sroar.BuildOpts{InitialSpareRatio: compactSpareRatio})
	}
}

// Compact must run before the index is published, as it takes no lock. Layers are
// replaced one at a time, so live memory peaks at one extra layer.
func (s *SegmentInMemory) Compact() {
	s.compactInto(&s.bitmaps)
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

// readSpareRatio sizes the spare capacity of each result segmentInMemoryReader returns.
// A larger ratio holds more idle memory per result, a smaller one copies a result sooner
// as memtables merge into it. BenchmarkSegmentInMemoryReadHeadroom sweeps it.
const readSpareRatio = 0.10

// segmentInMemoryReader returns bitmaps with layer 0's free key slots and more
// from spare capacity, since CombinedReader merges memtable layers into them.
// Scratch bitmaps it releases itself never grow, so their spare stays tail room.
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
	var eq *sroar.Bitmap
	var eqRelease func()
	if value == 0 {
		eq, eqRelease = r.bufPool.CloneWithHeadroom(r.bitmaps[0], readSpareRatio)
	} else {
		eq, eqRelease = r.bufPool.ZeroedLayoutWithHeadroom(r.bitmaps[0], readSpareRatio)
	}
	r.mergeEqual(value, eq, conc)
	return roaringset.BitmapLayer{Additions: eq}, eqRelease
}

func (r *segmentInMemoryReader) readNotEqual(value uint64, conc int) (roaringset.BitmapLayer, func()) {
	var eq *sroar.Bitmap
	var eqRelease func()
	if value == 0 {
		eq, eqRelease = r.bufPool.Clone(r.bitmaps[0])
	} else {
		eq, eqRelease = r.bufPool.ZeroedLayout(r.bitmaps[0])
	}
	defer eqRelease()
	r.mergeEqual(value, eq, conc)

	neq, neqRelease := r.bufPool.CloneWithHeadroom(r.bitmaps[0], readSpareRatio)
	neq.AndNotConc(eq, conc)
	return roaringset.BitmapLayer{Additions: neq}, neqRelease
}

func (r *segmentInMemoryReader) readLessThan(value uint64, conc int) (roaringset.BitmapLayer, func()) {
	if value == 0 {
		// no value is < 0
		return roaringset.BitmapLayer{Additions: sroar.NewBitmap()}, noopRelease
	}

	gte, gteRelease := r.bufPool.ZeroedLayout(r.bitmaps[0])
	defer gteRelease()
	r.mergeGreaterThanEqual(value, gte, conc)

	lt, ltRelease := r.bufPool.CloneWithHeadroom(r.bitmaps[0], readSpareRatio)
	lt.AndNotConc(gte, conc)
	return roaringset.BitmapLayer{Additions: lt}, ltRelease
}

func (r *segmentInMemoryReader) readLessThanEqual(value uint64, conc int) (roaringset.BitmapLayer, func()) {
	// <= 0 is Equal 0, which reads faster through mergeEqual (BenchmarkSegmentInMemoryReader).
	if value == 0 {
		return r.readEqual(value, conc)
	}
	if value == math.MaxUint64 {
		all, allRelease := r.bufPool.CloneWithHeadroom(r.bitmaps[0], readSpareRatio)
		// all values are <= max uint64
		return roaringset.BitmapLayer{Additions: all}, allRelease
	}

	return r.readLessThan(value+1, conc)
}

func (r *segmentInMemoryReader) readGreaterThan(value uint64, conc int) (roaringset.BitmapLayer, func()) {
	// > 0 is NotEqual 0, which reads faster through mergeEqual (BenchmarkSegmentInMemoryReader).
	if value == 0 {
		return r.readNotEqual(value, conc)
	}
	if value == math.MaxUint64 {
		// no value is > max uint64
		return roaringset.BitmapLayer{Additions: sroar.NewBitmap()}, noopRelease
	}

	return r.readGreaterThanEqual(value+1, conc)
}

func (r *segmentInMemoryReader) readGreaterThanEqual(value uint64, conc int) (roaringset.BitmapLayer, func()) {
	if value == 0 {
		all, allRelease := r.bufPool.CloneWithHeadroom(r.bitmaps[0], readSpareRatio)
		// all values are >= 0
		return roaringset.BitmapLayer{Additions: all}, allRelease
	}

	gte, gteRelease := r.bufPool.ZeroedLayoutWithHeadroom(r.bitmaps[0], readSpareRatio)
	r.mergeGreaterThanEqual(value, gte, conc)
	return roaringset.BitmapLayer{Additions: gte}, gteRelease
}

// mergeGreaterThanEqual fills result, an empty bitmap laid out like layer 0,
// with layer 0's rows whose value is >= value. value must be above 0, or the
// lowest set bit indexes past the layers and it panics.
func (r *segmentInMemoryReader) mergeGreaterThanEqual(value uint64, result *sroar.Bitmap, conc int) {
	// every layer is a subset of layer 0, so ORing one into the empty result
	// equals ANDing layer 0 with it
	first := bits.TrailingZeros64(value) + 1
	result.OrConc(r.bitmaps[first], conc)
	for bit := first + 1; bit < len(r.bitmaps); bit++ {
		if value&(1<<(bit-1)) != 0 {
			result.AndConc(r.bitmaps[bit], conc)
		} else {
			result.OrConc(r.bitmaps[bit], conc)
		}
	}
}

// mergeEqual keeps layer 0's rows that are in every layer of value's set bits
// and in no layer of its unset bits. result holds a copy of layer 0 for value 0,
// and an empty bitmap laid out like layer 0 otherwise.
func (r *segmentInMemoryReader) mergeEqual(value uint64, result *sroar.Bitmap, conc int) {
	first := 0
	if value != 0 {
		first = bits.TrailingZeros64(value) + 1
		result.OrConc(r.bitmaps[first], conc)
	}
	for bit := 1; bit < len(r.bitmaps); bit++ {
		if bit == first {
			continue
		}
		if value&(1<<(bit-1)) != 0 {
			result.AndConc(r.bitmaps[bit], conc)
		} else {
			result.AndNotConc(r.bitmaps[bit], conc)
		}
	}
}

// -----------------------------------------------------------------------------

type rangeBitmaps [65]*sroar.Bitmap

var noopRelease = func() {}
