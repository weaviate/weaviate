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

package roaringset

import (
	"context"
	"fmt"
	"math/bits"
	"slices"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type BitmapBufPool interface {
	// Get makes BitmapBufPool a sroar.BufPool, so a bitmap built into a buffer
	// from it also grows through it.
	Get(minCap int) (buf []byte, put func())
	// GetBitmapStruct returns a zeroed sroar.Bitmap to pass as dst to a sroar
	// To method. put releases the buffer the bitmap holds by then and recycles
	// the struct, so neither may be used after it.
	GetBitmapStruct() (dst *sroar.Bitmap, put func())
	Clone(bm *sroar.Bitmap) (cloned *sroar.Bitmap, put func())
	// CloneWithHeadroom is Clone for a bitmap about to take new values. It splits
	// spare capacity between key slots and tail room, in a first buffer of
	// (1+spareRatio) times bm's size. A negative or NaN spareRatio panics.
	CloneWithHeadroom(bm *sroar.Bitmap, spareRatio float64) (cloned *sroar.Bitmap, put func())
	// ZeroedLayout returns an empty bitmap with bm's keys and containers, which a
	// subset of bm's values fills without growing.
	ZeroedLayout(bm *sroar.Bitmap) (zeroed *sroar.Bitmap, put func())
	// ZeroedLayoutWithHeadroom is to ZeroedLayout what CloneWithHeadroom is to Clone.
	ZeroedLayoutWithHeadroom(bm *sroar.Bitmap, spareRatio float64) (zeroed *sroar.Bitmap, put func())
	// CloneBytes clones an already-serialized bitmap into a pooled buffer. src
	// must be a valid, non-empty sroar serialization. Like Clone, the clone owns
	// the buffer's full capacity and grows through the pool past it.
	CloneBytes(src []byte) (cloned *sroar.Bitmap, put func())
	// FromAccumulator materializes the accumulator's union into a pooled
	// buffer, so a warm accumulator building into pooled memory allocates
	// nothing. The returned bitmap owns the buffer's full capacity and grows
	// through the pool past it (same contract as Clone). Release via put;
	// the accumulator stays reusable. acc must be non-nil (unlike Clone's
	// nil tolerance, it panics).
	FromAccumulator(acc *sroar.Accumulator) (bm *sroar.Bitmap, put func())
}

func clone(pool BitmapBufPool, src *sroar.Bitmap) (cloned *sroar.Bitmap, put func()) {
	dst, put := pool.GetBitmapStruct()
	return src.CloneTo(dst, sroar.BuildOpts{Pool: pool, SpareTailOnly: true}), put
}

func cloneWithHeadroom(pool BitmapBufPool, src *sroar.Bitmap, spareRatio float64) (cloned *sroar.Bitmap, put func()) {
	dst, put := pool.GetBitmapStruct()
	return src.CloneTo(dst, sroar.BuildOpts{Pool: pool, InitialSpareRatio: spareRatio}), put
}

func zeroedLayout(pool BitmapBufPool, src *sroar.Bitmap) (zeroed *sroar.Bitmap, put func()) {
	dst, put := pool.GetBitmapStruct()
	return src.ZeroedLayoutTo(dst, sroar.BuildOpts{Pool: pool, SpareTailOnly: true}), put
}

func zeroedLayoutWithHeadroom(pool BitmapBufPool, src *sroar.Bitmap, spareRatio float64) (zeroed *sroar.Bitmap, put func()) {
	dst, put := pool.GetBitmapStruct()
	return src.ZeroedLayoutTo(dst, sroar.BuildOpts{Pool: pool, InitialSpareRatio: spareRatio}), put
}

// src must be a non-empty sroar serialization of at least 8 bytes, or the clone
// is empty. Odd-length src is corrupt and panics.
func cloneBytes(pool BitmapBufPool, src []byte) (cloned *sroar.Bitmap, put func()) {
	requireEvenLength(src)
	dst, put := pool.GetBitmapStruct()
	return sroar.FromBufferWithCopyTo(dst, src, sroar.BuildOpts{Pool: pool, SpareTailOnly: true}), put
}

// fromBufferToBuf wraps the serialized bitmap in buf, which may extend into
// spare capacity, and takes ownership of release, the func freeing buf's memory.
func fromBufferToBuf(pool BitmapBufPool, buf []byte, release func()) (bm *sroar.Bitmap, put func()) {
	dst, put := pool.GetBitmapStruct()
	return sroar.FromBufferOwned(dst, buf, release, pool), put
}

// requireEvenLength rejects odd-length serializations — always corruption,
// since sroar serializes []uint16 — with a recoverable panic. Reaching
// sroar's own even-length assert instead would log.Fatal the whole process
// on one corrupt segment region.
func requireEvenLength(src []byte) {
	if len(src)%2 != 0 {
		panic(fmt.Sprintf("roaringset: corrupt serialized bitmap: odd length %d", len(src)))
	}
}

func fromAccumulator(pool BitmapBufPool, acc *sroar.Accumulator) (bm *sroar.Bitmap, put func()) {
	dst, put := pool.GetBitmapStruct()
	return acc.BitmapTo(dst, sroar.BuildOpts{Pool: pool, SpareTailOnly: true}), put
}

// bitmapStructPool recycles the result structs sroar To methods build into.
// It is separate from the buffer pools because a bitmap growing through them
// releases its buffer while the struct is still in use.
type bitmapStructPool struct {
	pool *sync.Pool
}

// pooledBitmap pairs a result struct with its put, bound once at creation so
// Get doesn't allocate a closure per call.
type pooledBitmap struct {
	bm  sroar.Bitmap
	put func()
}

func newBitmapStructPool() *bitmapStructPool {
	p := &bitmapStructPool{}
	p.pool = &sync.Pool{
		New: func() any {
			pb := &pooledBitmap{}
			pb.put = func() {
				// The buffer handed out at clone may have been outgrown and already
				// released, so only the bitmap knows which one it holds now.
				pb.bm.ReleaseBuf()
				// A parked struct must not pin heap memory the bitmap may have moved to.
				pb.bm = sroar.Bitmap{}
				p.pool.Put(pb)
			}
			return pb
		},
	}
	return p
}

func (p *bitmapStructPool) Get() (dst *sroar.Bitmap, put func()) {
	pb := p.pool.Get().(*pooledBitmap)
	return &pb.bm, pb.put
}

func NewBitmapBufPoolDefault(logger logrus.FieldLogger, metrics *monitoring.PrometheusMetrics,
	inMemoMaxBufSize int, maxMemoSizeForBufs int,
) (pool BitmapBufPool, close func()) {
	syncMinRangeP2 := 9  // 2^9 = 512B
	syncMaxRangeP2 := 20 // 2^20 = 1MB
	syncRanges := calculateSyncBufferRanges(syncMinRangeP2, syncMaxRangeP2)
	syncMaxBufSize := syncRanges[len(syncRanges)-1]

	inMemoMinRangeP2 := syncMaxRangeP2 + 1
	inMemoRanges, inMemoBufsLimits := calculateInMemoBufferRangesAndLimits(syncMaxBufSize, inMemoMinRangeP2,
		inMemoMaxBufSize, maxMemoSizeForBufs)

	allRanges := syncRanges
	if len(inMemoRanges) > 0 {
		allRanges = append(allRanges, inMemoRanges...)
	}

	stopCleanup := func() {}
	p := NewBitmapBufPoolRanged(metrics, syncMaxBufSize, inMemoBufsLimits, allRanges...)
	if ln := len(inMemoRanges); ln > 0 {
		limitMaxRange := inMemoBufsLimits[inMemoRanges[ln-1]]
		nBuffers := (limitMaxRange + 1) / 2
		cleanupInterval := 1 * time.Minute
		// try to clean half of buffers every minute
		stopCleanup = p.StartPeriodicCleanup(logger, nBuffers, cleanupInterval)
	}

	return p, stopCleanup
}

// -----------------------------------------------------------------------------

type bitmapBufPoolNoop struct{}

func NewBitmapBufPoolNoop() *bitmapBufPoolNoop {
	return &bitmapBufPoolNoop{}
}

func (p *bitmapBufPoolNoop) Get(minCap int) (buf []byte, put func()) {
	return make([]byte, 0, minCap), func() {}
}

func (p *bitmapBufPoolNoop) GetBitmapStruct() (dst *sroar.Bitmap, put func()) {
	dst = &sroar.Bitmap{}
	return dst, dst.ReleaseBuf
}

func (p *bitmapBufPoolNoop) Clone(bm *sroar.Bitmap) (cloned *sroar.Bitmap, put func()) {
	return clone(p, bm)
}

func (p *bitmapBufPoolNoop) CloneWithHeadroom(bm *sroar.Bitmap, spareRatio float64) (cloned *sroar.Bitmap, put func()) {
	return cloneWithHeadroom(p, bm, spareRatio)
}

func (p *bitmapBufPoolNoop) ZeroedLayout(bm *sroar.Bitmap) (zeroed *sroar.Bitmap, put func()) {
	return zeroedLayout(p, bm)
}

func (p *bitmapBufPoolNoop) ZeroedLayoutWithHeadroom(bm *sroar.Bitmap, spareRatio float64) (zeroed *sroar.Bitmap, put func()) {
	return zeroedLayoutWithHeadroom(p, bm, spareRatio)
}

func (p *bitmapBufPoolNoop) CloneBytes(src []byte) (cloned *sroar.Bitmap, put func()) {
	return cloneBytes(p, src)
}

func (p *bitmapBufPoolNoop) FromAccumulator(acc *sroar.Accumulator) (bm *sroar.Bitmap, put func()) {
	return fromAccumulator(p, acc)
}

// -----------------------------------------------------------------------------

type bitmapBufPoolRanged struct {
	ranges            []int
	firstInMemoRngIdx int
	poolsSync         []*BufPoolFixedSync
	poolsInMemo       []*BufPoolFixedInMemory
	structs           *bitmapStructPool
	disposableMetrics bufDisposableMetrics
}

// Creates multiple pools, one for specified range of sizes (given in bytes).
// E.g. for ranges 1024, 2048 and 4096, 3 internal buffer pools will be
// created to handle ranges of size: [1-1024], [1025-2048], [2048-4096].
// Buffers of sizes bigger than highest range will be created but not kept in pool
// (to be removed by GC when no longer needed)
// Ranges <=0 or duplicated will be ignored.
func NewBitmapBufPoolRanged(metrics *monitoring.PrometheusMetrics,
	syncMaxBufSize int, inMemoBufsLimits map[int]int, ranges ...int,
) *bitmapBufPoolRanged {
	ranges = validateBufferRanges(ranges)
	poolsSync := []*BufPoolFixedSync{}
	poolsInMemo := []*BufPoolFixedInMemory{}

	var inMemoMetrics bufPoolInMemoMetrics
	var disposableMetrics bufDisposableMetrics
	if metrics == nil {
		inMemoMetrics = &bufPoolNoopMetrics{}
		disposableMetrics = &bufDisposableNoopMetrics{}
	} else {
		disposableMetrics = newPromBufDisposableMetrics(metrics)
	}

	i := 0
	for ; i < len(ranges) && ranges[i] <= syncMaxBufSize; i++ {
		poolsSync = append(poolsSync, NewBufPoolFixedSync(ranges[i]))
	}
	firstInMemoRngIdx := i
	for ; i < len(ranges); i++ {
		limit := 1
		if lmt, ok := inMemoBufsLimits[ranges[i]]; ok {
			limit = lmt
		}
		if metrics != nil {
			inMemoMetrics = newPromBufPoolInMemoMetrics(metrics, ranges[i])
		}
		poolsInMemo = append(poolsInMemo, NewBufPoolFixedInMemory(inMemoMetrics, ranges[i], limit))
	}

	return &bitmapBufPoolRanged{
		ranges:            ranges,
		firstInMemoRngIdx: firstInMemoRngIdx,
		poolsSync:         poolsSync,
		poolsInMemo:       poolsInMemo,
		structs:           newBitmapStructPool(),
		disposableMetrics: disposableMetrics,
	}
}

func (p *bitmapBufPoolRanged) Get(minCap int) (buf []byte, put func()) {
	for i := 0; i < p.firstInMemoRngIdx; i++ {
		if minCap <= p.ranges[i] {
			return p.poolsSync[i].Get()
		}
	}
	for i := p.firstInMemoRngIdx; i < len(p.ranges); i++ {
		if minCap <= p.ranges[i] {
			return p.poolsInMemo[i-p.firstInMemoRngIdx].Get()
		}
	}
	p.disposableMetrics.bufCreated(minCap)
	return make([]byte, 0, minCap), func() {}
}

func (p *bitmapBufPoolRanged) GetBitmapStruct() (dst *sroar.Bitmap, put func()) {
	return p.structs.Get()
}

func (p *bitmapBufPoolRanged) Clone(bm *sroar.Bitmap) (cloned *sroar.Bitmap, put func()) {
	return clone(p, bm)
}

func (p *bitmapBufPoolRanged) CloneWithHeadroom(bm *sroar.Bitmap, spareRatio float64) (cloned *sroar.Bitmap, put func()) {
	return cloneWithHeadroom(p, bm, spareRatio)
}

func (p *bitmapBufPoolRanged) ZeroedLayout(bm *sroar.Bitmap) (zeroed *sroar.Bitmap, put func()) {
	return zeroedLayout(p, bm)
}

func (p *bitmapBufPoolRanged) ZeroedLayoutWithHeadroom(bm *sroar.Bitmap, spareRatio float64) (zeroed *sroar.Bitmap, put func()) {
	return zeroedLayoutWithHeadroom(p, bm, spareRatio)
}

func (p *bitmapBufPoolRanged) CloneBytes(src []byte) (cloned *sroar.Bitmap, put func()) {
	return cloneBytes(p, src)
}

func (p *bitmapBufPoolRanged) FromAccumulator(acc *sroar.Accumulator) (bm *sroar.Bitmap, put func()) {
	return fromAccumulator(p, acc)
}

func (p *bitmapBufPoolRanged) cleanup(n int) map[int]int {
	cleaned := map[int]int{}
	for i := p.firstInMemoRngIdx; i < len(p.ranges); i++ {
		cleaned[p.ranges[i]] = p.poolsInMemo[i-p.firstInMemoRngIdx].Cleanup(n)
	}
	return cleaned
}

func (p *bitmapBufPoolRanged) StartPeriodicCleanup(logger logrus.FieldLogger, n int, interval time.Duration) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	errors.GoWrapper(func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.cleanup(n)
			}
		}
	}, logger)
	return cancel
}

// -----------------------------------------------------------------------------

type BufPoolFixedSync struct {
	pool *sync.Pool
}

// pooledBuf pairs a reusable buffer with its release closure, bound once at
// creation so Get doesn't allocate a closure per call.
type pooledBuf struct {
	buf []byte
	put func()
}

func NewBufPoolFixedSync(cap int) *BufPoolFixedSync {
	p := &BufPoolFixedSync{}
	p.pool = &sync.Pool{
		New: func() any {
			pb := &pooledBuf{buf: make([]byte, 0, cap)}
			pb.put = func() { p.pool.Put(pb) }
			return pb
		},
	}
	return p
}

func (p *BufPoolFixedSync) Get() (buf []byte, put func()) {
	pb := p.pool.Get().(*pooledBuf)
	return pb.buf, pb.put
}

// -----------------------------------------------------------------------------

type BufPoolFixedInMemory struct {
	cap     int
	limit   int
	bufsCh  chan *pooledBuf
	metrics bufPoolInMemoMetrics
}

func NewBufPoolFixedInMemory(metrics bufPoolInMemoMetrics, cap int, limit int) *BufPoolFixedInMemory {
	return &BufPoolFixedInMemory{
		cap:     cap,
		limit:   limit,
		bufsCh:  make(chan *pooledBuf, limit),
		metrics: metrics,
	}
}

func (p *BufPoolFixedInMemory) Get() (buf []byte, put func()) {
	var pb *pooledBuf
	select {
	case pb = <-p.bufsCh:
		p.metrics.bufGot()
	default:
		// bind the release closure once; reused buffers keep theirs
		pb = &pooledBuf{buf: make([]byte, 0, p.cap)}
		pb.put = func() { p.put(pb) }
		p.metrics.bufCreated()
	}
	return pb.buf, pb.put
}

func (p *BufPoolFixedInMemory) put(pb *pooledBuf) bool {
	select {
	case p.bufsCh <- pb:
		p.metrics.bufPut()
		// successfully returned
		return true
	default:
		p.metrics.bufDiscarded()
		// chan full, discard buffer
		return false
	}
}

// Cleanup removes available buffers from the pool and returs number of buffers removed.
// Buffers are removed up to configured limit to prevent indefinite removal in case
// new ones are created in the parallel.
func (p *BufPoolFixedInMemory) Cleanup(n int) int {
	i := 0
outer:
	for n = min(n, p.limit); i < n; i++ {
		select {
		case <-p.bufsCh:
			p.metrics.bufCleanedUp()
			// discard taken buffer
		default:
			break outer
		}
	}
	return i
}

// -----------------------------------------------------------------------------

func validateBufferRanges(ranges []int) []int {
	if ln := len(ranges); ln > 0 {
		// cleanup ranges, keep unique and > 0
		unique_gt0 := map[int]struct{}{}
		for i := 0; i < ln; i++ {
			if rng := ranges[i]; rng > 0 {
				unique_gt0[rng] = struct{}{}
			}
		}
		i := 0
		for rng := range unique_gt0 {
			ranges[i] = rng
			i++
		}
		ranges = ranges[:i]
		slices.Sort(ranges)
	}
	return ranges
}

func calculateSyncBufferRanges(minRangeP2, maxRangeP2 int) []int {
	if minRangeP2 < 0 || maxRangeP2 < 0 || minRangeP2 > maxRangeP2 {
		return []int{}
	}

	rangesLn := maxRangeP2 - minRangeP2 + 1
	ranges := make([]int, rangesLn)
	for i := range ranges {
		ranges[i] = 1 << (i + minRangeP2)
	}
	return ranges
}

func calculateInMemoBufferRangesAndLimits(maxSyncBufSize, minRangeP2, maxBufSize, maxMemoSize int,
) ([]int, map[int]int) {
	if maxBufSize > maxSyncBufSize {
		maxRangeP2 := 63 - bits.LeadingZeros64(uint64(maxBufSize))

		rangesLn := maxRangeP2 - minRangeP2 + 1
		ranges := make([]int, rangesLn, rangesLn+1)
		for i := 0; i < rangesLn; i++ {
			ranges[i] = 1 << (i + minRangeP2)
		}
		if maxBufSize != 1<<maxRangeP2 {
			rangesLn++
			ranges = append(ranges, maxBufSize)
		}

		sums := make([]int, rangesLn)
		for i := 0; i < rangesLn; i++ {
			sums[i] = ranges[i]
			if i != 0 {
				sums[i] += sums[i-1]
			}
			if sums[i] > maxMemoSize {
				sums = sums[:i]
				ranges = ranges[:i]
				break
			}
		}

		rangesLn = len(ranges)
		bufsLimits := make(map[int]int, len(ranges)) // range -> limit
		for i := rangesLn - 1; i >= 0; i-- {
			bufsLimits[ranges[i]] = maxMemoSize / sums[i]
			maxMemoSize -= sums[i] * bufsLimits[ranges[i]]

			if i != rangesLn-1 {
				bufsLimits[ranges[i]] += bufsLimits[ranges[i+1]]
			}
		}

		return ranges, bufsLimits
	}
	return []int{}, map[int]int{}
}

// -----------------------------------------------------------------------------

type bufPoolInMemoMetrics interface {
	bufCreated()
	bufGot()
	bufPut()
	bufDiscarded()
	bufCleanedUp()
}

type promBufPoolInMemoMetrics struct {
	usageCounter *prometheus.CounterVec
	size         string
}

func newPromBufPoolInMemoMetrics(metrics *monitoring.PrometheusMetrics, sizeInBytes int) *promBufPoolInMemoMetrics {
	return &promBufPoolInMemoMetrics{
		usageCounter: metrics.LSMBitmapBuffersUsage,
		size:         humanize.IBytes(uint64(sizeInBytes)),
	}
}

func (m *promBufPoolInMemoMetrics) bufCreated() {
	m.usageCounter.WithLabelValues(m.size, "inmemo_created").Inc()
}

func (m *promBufPoolInMemoMetrics) bufGot() {
	m.usageCounter.WithLabelValues(m.size, "inmemo_got").Inc()
}

func (m *promBufPoolInMemoMetrics) bufPut() {
	m.usageCounter.WithLabelValues(m.size, "inmemo_put").Inc()
}

func (m *promBufPoolInMemoMetrics) bufDiscarded() {
	m.usageCounter.WithLabelValues(m.size, "inmemo_discarded").Inc()
}

func (m *promBufPoolInMemoMetrics) bufCleanedUp() {
	m.usageCounter.WithLabelValues(m.size, "inmemo_cleanedUp").Inc()
}

type bufPoolNoopMetrics struct{}

func (m *bufPoolNoopMetrics) bufCreated()   {}
func (m *bufPoolNoopMetrics) bufGot()       {}
func (m *bufPoolNoopMetrics) bufPut()       {}
func (m *bufPoolNoopMetrics) bufDiscarded() {}
func (m *bufPoolNoopMetrics) bufCleanedUp() {}

// -----------------------------------------------------------------------------

type bufDisposableMetrics interface {
	bufCreated(sizeInBytes int)
}

type promBufDisposableMetrics struct {
	usageCounter *prometheus.CounterVec
}

func newPromBufDisposableMetrics(metrics *monitoring.PrometheusMetrics) *promBufDisposableMetrics {
	return &promBufDisposableMetrics{
		usageCounter: metrics.LSMBitmapBuffersUsage,
	}
}

func (m *promBufDisposableMetrics) bufCreated(sizeInBytes int) {
	// smallest power of two >= sizeInBytes, so the label names the same size
	// class the pooled tiers use
	ceil := uint64(1)
	if sizeInBytes > 1 {
		ceil = 1 << bits.Len64(uint64(sizeInBytes)-1)
	}
	m.usageCounter.WithLabelValues(humanize.IBytes(ceil), "disposable_created").Inc()
}

type bufDisposableNoopMetrics struct{}

func (m *bufDisposableNoopMetrics) bufCreated(sizeInBytes int) {}
