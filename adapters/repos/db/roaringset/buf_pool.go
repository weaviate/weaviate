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
	"math"
	"math/bits"
	"slices"
	"sync"
	"sync/atomic"
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
	// ToBuf method. put releases the buffer the bitmap holds by then and
	// recycles the struct, so neither may be used after it.
	GetBitmapStruct() (dst *sroar.Bitmap, put func())
	CloneToBuf(bm *sroar.Bitmap) (cloned *sroar.Bitmap, put func())
	// CloneBytesToBuf clones an already-serialized bitmap into a pooled
	// buffer without materializing an intermediate bitmap over src. src must
	// be a valid, non-empty sroar serialization. Like CloneToBuf, the clone
	// owns the buffer's full capacity and grows through the pool past it.
	CloneBytesToBuf(src []byte) (cloned *sroar.Bitmap, put func())
	// AccumulatorToBuf materializes the accumulator's union into a pooled
	// buffer, so a warm accumulator building into pooled memory allocates
	// nothing. The returned bitmap owns the buffer's full capacity and grows
	// through the pool past it (same contract as CloneToBuf). Release via put;
	// the accumulator stays reusable. acc must be non-nil (unlike CloneToBuf's
	// nil tolerance, it panics).
	AccumulatorToBuf(acc *sroar.Accumulator) (bm *sroar.Bitmap, put func())
}

func cloneToBuf(pool BitmapBufPool, src *sroar.Bitmap) (cloned *sroar.Bitmap, put func()) {
	dst, put := pool.GetBitmapStruct()
	return src.CloneToBuf(dst, pool), put
}

// src must be a non-empty sroar serialization of at least 8 bytes, or the clone
// is empty. Odd-length src is corrupt and panics.
func cloneBytesToBuf(pool BitmapBufPool, src []byte) (cloned *sroar.Bitmap, put func()) {
	requireEvenLength(src)
	buf, release := pool.Get(len(src))
	buf = buf[:len(src)]
	copy(buf, src)
	return fromBufferToBuf(pool, buf, release)
}

// fromBufferToBuf wraps the serialized bitmap in buf, which may extend into
// spare capacity, and takes ownership of release, the func freeing buf's memory.
func fromBufferToBuf(pool BitmapBufPool, buf []byte, release func()) (bm *sroar.Bitmap, put func()) {
	dst, put := pool.GetBitmapStruct()
	return sroar.FromBufferToBuf(dst, buf, release, pool), put
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

func accumulatorToBuf(pool BitmapBufPool, acc *sroar.Accumulator) (bm *sroar.Bitmap, put func()) {
	dst, put := pool.GetBitmapStruct()
	return acc.BitmapToBuf(dst, pool), put
}

// bitmapStructPool recycles the result structs sroar ToBuf methods build into.
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

// These in-memory tier defaults are provisional. A shorter inMemoIdleTTL or a
// larger inMemoDecayFraction frees idle buffers sooner and lowers the hit rate.
// BenchmarkInMemoBufPoolDecaySweep sweeps both.
const (
	inMemoDecayTick     = 10 * time.Second
	inMemoIdleTTL       = time.Minute
	inMemoDecayFraction = 0.5
)

// idleBudget bounds the bytes of idle in-memory buffers. Buffers handed out are
// not counted.
func NewBitmapBufPoolDefault(logger logrus.FieldLogger, metrics *monitoring.PrometheusMetrics,
	inMemoMaxBufSize int, idleBudget int,
) (pool BitmapBufPool, close func()) {
	syncMinRangePo2 := 9  // 2^9 = 512B
	syncMaxRangePo2 := 20 // 2^20 = 1MB
	syncRanges := calculateSyncBufferRanges(syncMinRangePo2, syncMaxRangePo2)
	syncMaxBufSize := syncRanges[len(syncRanges)-1]

	inMemoMinRangePo2 := syncMaxRangePo2 + 1
	inMemoRanges := calculateInMemoBufferRanges(syncMaxBufSize, inMemoMinRangePo2,
		inMemoMaxBufSize, idleBudget)

	stopDecay := func() {}
	p := NewBitmapBufPoolRanged(metrics, idleBudget, syncRanges, inMemoRanges)
	if len(inMemoRanges) > 0 {
		stopDecay = p.startDecay(logger, inMemoDecayTick)
	}

	return p, stopDecay
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
	return &sroar.Bitmap{}, func() {}
}

func (p *bitmapBufPoolNoop) CloneToBuf(bm *sroar.Bitmap) (cloned *sroar.Bitmap, put func()) {
	return cloneToBuf(p, bm)
}

func (p *bitmapBufPoolNoop) CloneBytesToBuf(src []byte) (cloned *sroar.Bitmap, put func()) {
	return cloneBytesToBuf(p, src)
}

func (p *bitmapBufPoolNoop) AccumulatorToBuf(acc *sroar.Accumulator) (bm *sroar.Bitmap, put func()) {
	return accumulatorToBuf(p, acc)
}

// -----------------------------------------------------------------------------

type bitmapBufPoolRanged struct {
	ranges            []int
	poolsSync         []*BufPoolFixedSync
	inMemo            *inMemoBufPool
	structs           *bitmapStructPool
	disposableMetrics bufDisposableMetrics
}

// NewBitmapBufPoolRanged pools buffers of syncSizes in sync.Pools and of inMemoSizes
// under idleBudget. Get rounds a request up to the next size, and above the largest
// allocates a buffer GC reclaims. Sizes must ascend across both lists.
func NewBitmapBufPoolRanged(metrics *monitoring.PrometheusMetrics, idleBudget int,
	syncSizes, inMemoSizes []int,
) *bitmapBufPoolRanged {
	var disposableMetrics bufDisposableMetrics
	if metrics == nil {
		disposableMetrics = &bufDisposableNoopMetrics{}
	} else {
		disposableMetrics = newPromBufDisposableMetrics(metrics)
	}

	poolsSync := make([]*BufPoolFixedSync, len(syncSizes))
	for i, size := range syncSizes {
		poolsSync[i] = NewBufPoolFixedSync(size)
	}

	return &bitmapBufPoolRanged{
		ranges:            slices.Concat(syncSizes, inMemoSizes),
		poolsSync:         poolsSync,
		inMemo:            newInMemoBufPool(metrics, inMemoSizes, idleBudget, inMemoIdleTTL, inMemoDecayFraction),
		structs:           newBitmapStructPool(),
		disposableMetrics: disposableMetrics,
	}
}

func (p *bitmapBufPoolRanged) Get(minCap int) (buf []byte, put func()) {
	i, _ := slices.BinarySearch(p.ranges, minCap)
	switch nSync := len(p.poolsSync); {
	case i < nSync:
		return p.poolsSync[i].Get()
	case i < len(p.ranges):
		return p.inMemo.get(i - nSync)
	}
	p.disposableMetrics.bufCreated(minCap)
	return make([]byte, 0, minCap), func() {}
}

func (p *bitmapBufPoolRanged) GetBitmapStruct() (dst *sroar.Bitmap, put func()) {
	return p.structs.Get()
}

func (p *bitmapBufPoolRanged) CloneToBuf(bm *sroar.Bitmap) (cloned *sroar.Bitmap, put func()) {
	return cloneToBuf(p, bm)
}

func (p *bitmapBufPoolRanged) CloneBytesToBuf(src []byte) (cloned *sroar.Bitmap, put func()) {
	return cloneBytesToBuf(p, src)
}

func (p *bitmapBufPoolRanged) AccumulatorToBuf(acc *sroar.Accumulator) (bm *sroar.Bitmap, put func()) {
	return accumulatorToBuf(p, acc)
}

// startDecay advances the in-memory tier's clock and decays its idle buffers
// every interval until stop. The pool stays usable after stop, with idle bytes
// still bounded by the budget.
func (p *bitmapBufPoolRanged) startDecay(logger logrus.FieldLogger, interval time.Duration) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	epoch := time.Now()
	errors.GoWrapper(func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.inMemo.tick(time.Since(epoch))
			}
		}
	}, logger)
	return func() {
		cancel()
		<-done
	}
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

// inMemoBufPool keeps idle buffers of every size under one budget of idle bytes. A
// put over budget evicts the oldest, the larger size on a tie, only when no buffer of
// its own size is idle. No path holds two idleStack locks at once, so eviction cannot deadlock.
type inMemoBufPool struct {
	stacks []*idleStack
	budget int64
	// idle is the bytes of buffers idle in all stacks. put adds a buffer's size
	// before pushing it, and every removal from a stack subtracts it.
	idle atomic.Int64
	// now is the clock tick last stored, in nanoseconds. push stamps buffers with it.
	now           atomic.Int64
	ttl           int64
	decayFraction float64
}

func newInMemoBufPool(metrics *monitoring.PrometheusMetrics, sizes []int, budget int,
	ttl time.Duration, decayFraction float64,
) *inMemoBufPool {
	p := &inMemoBufPool{
		stacks:        make([]*idleStack, len(sizes)),
		budget:        int64(budget),
		ttl:           int64(ttl),
		decayFraction: decayFraction,
	}
	for i, size := range sizes {
		var m bufPoolInMemoMetrics = &bufPoolNoopMetrics{}
		if metrics != nil {
			m = newPromBufPoolInMemoMetrics(metrics, size)
		}
		p.stacks[i] = &idleStack{size: size, poolIdle: &p.idle, poolNow: &p.now, metrics: m}
		p.stacks[i].oldest.Store(math.MaxInt64)
	}
	return p
}

// get takes the newest idle buffer of size i, else borrows one of a larger size
// up to twice size i. A borrowed buffer returns to its own size on put.
func (p *inMemoBufPool) get(i int) (buf []byte, put func()) {
	s := p.stacks[i]
	for _, lender := range p.stacks[i:] {
		if lender.size > 2*s.size {
			break
		}
		if pb := lender.popNewest(); pb != nil {
			s.metrics.bufGot()
			return pb.buf, pb.put
		}
	}
	pb := &pooledBuf{buf: make([]byte, 0, s.size)}
	pb.put = func() { p.put(pb, i) }
	s.metrics.bufCreated()
	return pb.buf, pb.put
}

// put reserves the buffer's bytes before pushing it, so idle bytes never exceed the budget.
// Over budget it discards the buffer while its size has one idle, so only a drained size evicts.
// BenchmarkInMemoBufPoolOverBudgetPut sweeps the rule.
func (p *inMemoBufPool) put(pb *pooledBuf, i int) {
	s := p.stacks[i]
	size := int64(s.size)
	for {
		cur := p.idle.Load()
		if cur+size <= p.budget {
			if p.idle.CompareAndSwap(cur, cur+size) {
				break
			}
			continue
		}
		if s.oldest.Load() != math.MaxInt64 || !p.evictOldest() {
			s.metrics.bufDiscarded()
			return
		}
	}
	s.push(pb)
	s.metrics.bufPut()
}

// evictOldest drops the oldest idle buffer across all sizes. It returns false
// only when every stack is empty.
func (p *inMemoBufPool) evictOldest() bool {
	for {
		var victim *idleStack
		oldest := int64(math.MaxInt64)
		for _, s := range slices.Backward(p.stacks) {
			if since := s.oldest.Load(); since < oldest {
				victim, oldest = s, since
			}
		}
		if victim == nil {
			return false
		}
		if victim.popOldest() != nil {
			victim.metrics.bufEvicted()
			return true
		}
	}
}

// tick sets the clock to elapsed and decays every stack's expired buffers.
func (p *inMemoBufPool) tick(elapsed time.Duration) {
	now := int64(elapsed)
	p.now.Store(now)
	for _, s := range p.stacks {
		s.expire(now, p.ttl, p.decayFraction)
	}
}

type idleBuf struct {
	pb    *pooledBuf
	since int64
}

// idleStack holds the idle buffers of one size in bufs[head:], oldest first. Gets
// take the newest from the back, and eviction and decay the oldest from the front.
// Methods removing buffers subtract their bytes from poolIdle.
type idleStack struct {
	size int
	mu   sync.Mutex
	bufs []idleBuf
	// head indexes the oldest buffer. Taking from the front advances it instead of
	// shifting bufs, and push reuses the freed front before growing bufs.
	head int
	// oldest mirrors bufs[head].since, or math.MaxInt64 when empty, so put and
	// evictOldest can read it without locking the stack.
	oldest    atomic.Int64
	nextDecay int64
	poolIdle  *atomic.Int64
	poolNow   *atomic.Int64
	metrics   bufPoolInMemoMetrics
}

// push reads the clock under mu, so stamps stay in age order when a tick
// lands between put's reservation and the push.
func (s *idleStack) push(pb *pooledBuf) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.bufs) == cap(s.bufs) && s.head > 0 {
		n := copy(s.bufs, s.bufs[s.head:])
		clear(s.bufs[n:])
		s.bufs = s.bufs[:n]
		s.head = 0
	}
	s.bufs = append(s.bufs, idleBuf{pb: pb, since: s.poolNow.Load()})
	s.updateOldest()
}

func (s *idleStack) popNewest() *pooledBuf {
	s.mu.Lock()
	defer s.mu.Unlock()

	last := len(s.bufs) - 1
	if last < s.head {
		return nil
	}
	pb := s.bufs[last].pb
	s.bufs[last] = idleBuf{}
	s.bufs = s.bufs[:last]
	s.removed(1)
	return pb
}

func (s *idleStack) popOldest() *pooledBuf {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.head == len(s.bufs) {
		return nil
	}
	pb := s.bufs[s.head].pb
	s.bufs[s.head] = idleBuf{}
	s.head++
	s.removed(1)
	return pb
}

// expire drops the oldest decayFraction of the buffers idle longer than ttl,
// rounded up, at most once per ttl. Survivors keep their stamps.
func (s *idleStack) expire(now, ttl int64, decayFraction float64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if now < s.nextDecay {
		return
	}
	expired := 0
	for _, b := range s.bufs[s.head:] {
		if now-b.since <= ttl {
			break
		}
		expired++
	}
	if expired == 0 {
		return
	}
	dropped := int(math.Ceil(float64(expired) * decayFraction))
	clear(s.bufs[s.head : s.head+dropped])
	s.head += dropped
	s.removed(dropped)
	s.nextDecay = now + ttl
	for range dropped {
		s.metrics.bufCleanedUp()
	}
}

// removed returns n removed buffers' bytes to the budget. Caller holds mu.
func (s *idleStack) removed(n int) {
	s.poolIdle.Add(-int64(n * s.size))
	if s.head == len(s.bufs) {
		s.bufs = s.bufs[:0]
		s.head = 0
	}
	s.updateOldest()
}

func (s *idleStack) updateOldest() {
	if s.head == len(s.bufs) {
		s.oldest.Store(math.MaxInt64)
		return
	}
	s.oldest.Store(s.bufs[s.head].since)
}

// -----------------------------------------------------------------------------

// powersOfTwo returns 2^minPo2, 2^(minPo2+1), ... up to limit, stopping before
// 1<<63, which overflows int.
func powersOfTwo(minPo2, limit int) []int {
	var sizes []int
	for po2 := minPo2; po2 < bits.UintSize-1 && 1<<po2 <= limit; po2++ {
		sizes = append(sizes, 1<<po2)
	}
	return sizes
}

func calculateSyncBufferRanges(minRangePo2, maxRangePo2 int) []int {
	if minRangePo2 < 0 || maxRangePo2 < 0 || minRangePo2 > maxRangePo2 {
		return []int{}
	}
	return powersOfTwo(minRangePo2, 1<<maxRangePo2)
}

// calculateInMemoBufferRanges returns powers of two from 2^minRangePo2 up to
// maxBufSize, plus maxBufSize itself, leaving out sizes that cannot fit idleBudget.
func calculateInMemoBufferRanges(maxSyncBufSize, minRangePo2, maxBufSize, idleBudget int) []int {
	if maxBufSize <= maxSyncBufSize {
		return nil
	}
	ranges := powersOfTwo(minRangePo2, min(maxBufSize, idleBudget))
	maxIsPowerOfTwo := bits.OnesCount(uint(maxBufSize)) == 1 // then powersOfTwo added it
	maxFitsBudget := maxBufSize <= idleBudget
	if !maxIsPowerOfTwo && maxFitsBudget {
		ranges = append(ranges, maxBufSize)
	}
	return ranges
}

// -----------------------------------------------------------------------------

type bufPoolInMemoMetrics interface {
	bufCreated()
	bufGot()
	bufPut()
	bufDiscarded()
	bufCleanedUp()
	bufEvicted()
}

// promBufPoolInMemoMetrics resolves its counters once, so get and put skip the
// label lookup.
type promBufPoolInMemoMetrics struct {
	created, got, put, discarded, cleanedUp, evicted prometheus.Counter
}

func newPromBufPoolInMemoMetrics(metrics *monitoring.PrometheusMetrics, sizeInBytes int) *promBufPoolInMemoMetrics {
	size := humanize.IBytes(uint64(sizeInBytes))
	counter := func(op string) prometheus.Counter {
		return metrics.LSMBitmapBuffersUsage.WithLabelValues(size, op)
	}
	return &promBufPoolInMemoMetrics{
		created:   counter("inmemo_created"),
		got:       counter("inmemo_got"),
		put:       counter("inmemo_put"),
		discarded: counter("inmemo_discarded"),
		cleanedUp: counter("inmemo_cleanedUp"),
		evicted:   counter("inmemo_evicted"),
	}
}

func (m *promBufPoolInMemoMetrics) bufCreated()   { m.created.Inc() }
func (m *promBufPoolInMemoMetrics) bufGot()       { m.got.Inc() }
func (m *promBufPoolInMemoMetrics) bufPut()       { m.put.Inc() }
func (m *promBufPoolInMemoMetrics) bufDiscarded() { m.discarded.Inc() }
func (m *promBufPoolInMemoMetrics) bufCleanedUp() { m.cleanedUp.Inc() }
func (m *promBufPoolInMemoMetrics) bufEvicted()   { m.evicted.Inc() }

type bufPoolNoopMetrics struct{}

func (m *bufPoolNoopMetrics) bufCreated()   {}
func (m *bufPoolNoopMetrics) bufGot()       {}
func (m *bufPoolNoopMetrics) bufPut()       {}
func (m *bufPoolNoopMetrics) bufDiscarded() {}
func (m *bufPoolNoopMetrics) bufCleanedUp() {}
func (m *bufPoolNoopMetrics) bufEvicted()   {}

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
