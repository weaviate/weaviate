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
	"fmt"
	"math"
	"math/bits"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/entities/errors"
)

type BitmapBufPool interface {
	Get(minCap int) (buf []byte, put func())
	CloneToBuf(bm *sroar.Bitmap) (cloned *sroar.Bitmap, put func())
}

func cloneToBuf(pool BitmapBufPool, bm *sroar.Bitmap) (cloned *sroar.Bitmap, put func()) {
	buf, put := pool.Get(bm.LenInBytes())
	return bm.CloneToBuf(buf), put
}

func NewBitmapBufPoolDefault(logger logrus.FieldLogger, inMemoMaxBufSize int, maxMemoSizeForBufs int,
) (pool BitmapBufPool, close func()) {
	cleanupInterval := 10 * time.Second

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

	fmt.Printf(". ==> all ranges %v\n\n", allRanges)
	p := NewBitmapBufPoolRanged(syncMaxBufSize, inMemoBufsLimits, allRanges...)
	stop := p.StartPeriodicCleanup(logger, cleanupInterval)

	go func() {
		var topAlloc uint64
		var stats runtime.MemStats

		i := 0
		for {
			time.Sleep(1 * time.Second)
			runtime.ReadMemStats(&stats)
			if stats.Alloc > topAlloc {
				topAlloc = stats.Alloc
			}
			i++

			if i%30 == 0 {
				fmt.Printf("  ==> alloc: %.2fMB, top alloc: %.2fMB, total alloc: %.2fMB\n",
					float64(stats.Alloc)/1024/1024, float64(topAlloc)/1024/1024, float64(stats.TotalAlloc)/1024/1024)

				for _, pf := range p.poolsSync {
					fmt.Printf("      pool [%d]: got [%d] created [%d] put [%d]\n",
						pf.cap, pf.got.Load(), pf.created.Load(), pf.put.Load())
				}
				for _, pl := range p.poolsInMemo {
					fmt.Printf("      pool [%d]: got [%d] created [%d] put [%d] outside [%d] cleaned [%d]\n",
						pl.cap, pl.got.Load(), pl.created.Load(), pl.put.Load(), pl.outside.Load(), pl.cleaned.Load())
				}
			}
		}
	}()

	return p, stop
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
}

// Creates multiple pools, one for specified range of sizes (given in bytes).
// E.g. for ranges 1024, 2048 and 4096, 3 internal buffer pools will be
// created to handle ranges of size: [1-1024], [1025-2048], [2048-4096].
// Buffers of sizes bigger than highest range will be created but not kept in pool
// (to be removed by GC when no longer needed)
// Ranges <=0 or duplicated will be ignored.
func NewBitmapBufPoolRanged(syncMaxBufSize int, inMemoBufsLimits map[int]int, ranges ...int) *bitmapBufPoolRanged {
	ranges = validateBufferRanges(ranges)
	poolsSync := []*BufPoolFixedSync{}
	poolsInMemo := []*BufPoolFixedInMemory{}

	i := 0
	for ; i < len(ranges) && ranges[i] <= syncMaxBufSize; i++ {
		poolsSync = append(poolsSync, NewBufPoolFixedSync(ranges[i]))
	}
	firstInMemoRngIdx := i
	for ; i < len(ranges); i++ {
		if limit, ok := inMemoBufsLimits[ranges[i]]; ok {
			poolsInMemo = append(poolsInMemo, NewBufPoolFixedInMemory(ranges[i], limit))
		}
	}

	return &bitmapBufPoolRanged{
		ranges:            ranges,
		firstInMemoRngIdx: firstInMemoRngIdx,
		poolsSync:         poolsSync,
		poolsInMemo:       poolsInMemo,
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
	return make([]byte, 0, minCap), func() {}
}

func (p *bitmapBufPoolRanged) CloneToBuf(bm *sroar.Bitmap) (cloned *sroar.Bitmap, put func()) {
	return cloneToBuf(p, bm)
}

func (p *bitmapBufPoolRanged) cleanup() map[int]int {
	cleaned := map[int]int{}
	for i := p.firstInMemoRngIdx; i < len(p.ranges); i++ {
		cleaned[p.ranges[i]] = p.poolsInMemo[i-p.firstInMemoRngIdx].Cleanup()
	}
	return cleaned
}

func (p *bitmapBufPoolRanged) StartPeriodicCleanup(logger logrus.FieldLogger, interval time.Duration) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	errors.GoWrapper(func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.cleanup()
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

	cap               int
	created, got, put atomic.Int32
}

func NewBufPoolFixedSync(cap int) *BufPoolFixedSync {
	p := &BufPoolFixedSync{
		cap: cap,
	}

	pool := &sync.Pool{
		New: func() any {
			p.created.Add(1)
			buf := make([]byte, 0, cap)
			return &buf
		},
	}

	p.pool = pool
	return p
}

func (p *BufPoolFixedSync) Get() (buf []byte, put func()) {
	ptr := p.pool.Get().(*[]byte)
	defer p.got.Add(1)
	return *ptr, func() { p.pool.Put(ptr); p.put.Add(1) }
}

// -----------------------------------------------------------------------------

type BufPoolFixedInMemory struct {
	cap    int
	limit  int
	bufsCh chan *[]byte  // chan of free buffers
	leftCh chan struct{} // chan of markings of buffers left to be created up to provided limit

	got, put, created, cleaned, outside atomic.Int32
}

func NewBufPoolFixedInMemory(cap int, limit int) *BufPoolFixedInMemory {
	bufCh := make(chan *[]byte, limit)
	leftCh := make(chan struct{}, limit)
	for i := 0; i < limit; i++ {
		leftCh <- struct{}{}
	}

	return &BufPoolFixedInMemory{
		cap:    cap,
		limit:  limit,
		bufsCh: bufCh,
		leftCh: leftCh,
	}
}

func (p *BufPoolFixedInMemory) Get() (buf []byte, put func()) {
	// take free buffer if available
	select {
	case ptr := <-p.bufsCh:
		defer p.got.Add(1)
		return *ptr, func() {
			select {
			case p.bufsCh <- ptr:
				p.put.Add(1)
				// ok
			default:
				// should not happen
			}
		}
	default:
	}

	// no free buffer
	select {
	case <-p.leftCh:
		// create new buffer if limit not yet reached
		buf := make([]byte, 0, p.cap)
		p.created.Add(1)
		ptr := &buf
		defer p.got.Add(1)
		return *ptr, func() {
			select {
			case p.bufsCh <- ptr:
				p.put.Add(1)
				// ok
			default:
				// should not happen
			}
		}
	default:
		// limit reached, create temp buffer outside of pool
		p.outside.Add(1)
		return make([]byte, 0, p.cap), func() {}
	}
}

// Cleanup removes available buffers from the pool and returs number of buffers removed.
// Buffers are removed up to configured limit to prevent indefinite removal in case
// new ones are created in the parallel.
func (p *BufPoolFixedInMemory) Cleanup() int {
	i := 0
	for ; i < p.limit; i++ {
		select {
		case <-p.bufsCh:
			p.cleaned.Add(1)
			p.leftCh <- struct{}{}
		default:
			return i
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
	rangesLn := maxRangeP2 - minRangeP2 + 1
	ranges := make([]int, rangesLn)
	for i := range ranges {
		ranges[i] = 1 << (i + minRangeP2)
	}
	return ranges
}

func calculateInMemoBufferRangesAndLimits(maxSyncBufSize, minRangeP2, maxBufSize, maxMemoSize int,
) ([]int, map[int]int) {
	var bufsLimits map[int]int // range -> limit
	var ranges []int
	if maxBufSize > maxSyncBufSize {
		maxRangeP2 := 63 - bits.LeadingZeros64(uint64(maxBufSize))

		rangesLn := maxRangeP2 - minRangeP2 + 1
		ranges = make([]int, rangesLn, rangesLn+1)
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
		bufsLimits = make(map[int]int, len(ranges))
		for i := rangesLn - 1; i >= 0; i-- {
			bufsLimits[ranges[i]] = maxMemoSize / sums[i]
			maxMemoSize -= sums[i] * bufsLimits[ranges[i]]

			if i != rangesLn-1 {
				bufsLimits[ranges[i]] += bufsLimits[ranges[i+1]]
			}
		}

		fmt.Printf("inMemoRanges %v\n", ranges)
		fmt.Printf("sums %v\n", sums)
		fmt.Printf("limits %v\n", bufsLimits)
	}
	return ranges, bufsLimits
}
