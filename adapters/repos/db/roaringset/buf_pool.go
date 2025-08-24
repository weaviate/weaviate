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

package roaringset

import (
	"context"
	"math"
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
	Get(minCap int) (buf []byte, put func())
	CloneToBuf(bm *sroar.Bitmap) (cloned *sroar.Bitmap, put func())
}

func cloneToBuf(pool BitmapBufPool, bm *sroar.Bitmap) (cloned *sroar.Bitmap, put func()) {
	buf, put := pool.Get(bm.LenInBytes())
	return bm.CloneToBuf(buf), put
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

func (p *bitmapBufPoolNoop) CloneToBuf(bm *sroar.Bitmap) (cloned *sroar.Bitmap, put func()) {
	return cloneToBuf(p, bm)
}

// -----------------------------------------------------------------------------

type bitmapBufPoolRanged struct {
	ranges            []int
	firstInMemoRngIdx int
	poolsSync         []*BufPoolFixedSync
	poolsInMemo       []*BufPoolFixedInMemory
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

func (p *bitmapBufPoolRanged) CloneToBuf(bm *sroar.Bitmap) (cloned *sroar.Bitmap, put func()) {
	return cloneToBuf(p, bm)
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

type bitmapBufPoolFactorWrapper struct {
	pool   BitmapBufPool
	factor float64
}

func NewBitmapBufPoolFactorWrapper(pool BitmapBufPool, factor float64) *bitmapBufPoolFactorWrapper {
	factor = max(factor, 1.0)
	return &bitmapBufPoolFactorWrapper{pool: pool, factor: factor}
}

func (p *bitmapBufPoolFactorWrapper) Get(minCap int) (buf []byte, put func()) {
	newMinCap := int(math.Ceil(float64(minCap) * p.factor))
	return p.pool.Get(newMinCap)
}

func (p *bitmapBufPoolFactorWrapper) CloneToBuf(bm *sroar.Bitmap) (cloned *sroar.Bitmap, put func()) {
	return cloneToBuf(p, bm)
}

// -----------------------------------------------------------------------------

type BufPoolFixedSync struct {
	pool *sync.Pool
}

func NewBufPoolFixedSync(cap int) *BufPoolFixedSync {
	return &BufPoolFixedSync{
		pool: &sync.Pool{
			New: func() any {
				buf := make([]byte, 0, cap)
				return &buf
			},
		},
	}
}

func (p *BufPoolFixedSync) Get() (buf []byte, put func()) {
	ptr := p.pool.Get().(*[]byte)
	return *ptr, func() { p.pool.Put(ptr) }
}

// -----------------------------------------------------------------------------

type BufPoolFixedInMemory struct {
	cap     int
	limit   int
	bufsCh  chan *[]byte
	metrics bufPoolInMemoMetrics
}

func NewBufPoolFixedInMemory(metrics bufPoolInMemoMetrics, cap int, limit int) *BufPoolFixedInMemory {
	return &BufPoolFixedInMemory{
		cap:     cap,
		limit:   limit,
		bufsCh:  make(chan *[]byte, limit),
		metrics: metrics,
	}
}

func (p *BufPoolFixedInMemory) Get() (buf []byte, put func()) {
	var ptr *[]byte
	select {
	case ptr = <-p.bufsCh:
		buf = *ptr
		p.metrics.bufGot()
	default:
		buf = make([]byte, 0, p.cap)
		ptr = &buf
		p.metrics.bufCreated()
	}
	return buf, func() { p.put(ptr) }
}

func (p *BufPoolFixedInMemory) put(ptr *[]byte) bool {
	select {
	case p.bufsCh <- ptr:
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
	s := uint64(sizeInBytes)
	ceil := uint64(1 << bits.Len64(s))
	if s^ceil != 0 {
		ceil *= 2
	}
	size := humanize.IBytes(ceil)
	m.usageCounter.WithLabelValues(size, "disposable_created").Inc()
}

type bufDisposableNoopMetrics struct{}

func (m *bufDisposableNoopMetrics) bufCreated(sizeInBytes int) {}
