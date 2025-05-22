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
	"math"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/weaviate/sroar"
)

// -----------------------------------------------------------------------------

type BitmapBufPool interface {
	Get(minCap int) (buf []byte, put func())
	CloneToBuf(bm *sroar.Bitmap) (cloned *sroar.Bitmap, put func())
}

func cloneToBuf(pool BitmapBufPool, bm *sroar.Bitmap) (cloned *sroar.Bitmap, put func()) {
	buf, put := pool.Get(bm.LenInBytes())
	return bm.CloneToBuf(buf), put
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

type bitmapBufPoolVar struct {
	pool *sync.Pool
}

func NewBitmapBufPoolVar() *bitmapBufPoolVar {
	return &bitmapBufPoolVar{
		pool: &sync.Pool{
			New: func() any {
				dummyBuf := make([]byte, 0)
				return &dummyBuf
			},
		},
	}
}

func (p *bitmapBufPoolVar) Get(minCap int) (buf []byte, put func()) {
	ptr := p.pool.Get().(*[]byte)
	buf = *ptr
	if cap(buf) < minCap {
		buf = make([]byte, 0, minCap)
		*ptr = buf
	} else if len(buf) > 0 {
		buf = buf[:0]
		*ptr = buf
	}
	return buf, func() { p.pool.Put(ptr) }
}

func (p *bitmapBufPoolVar) CloneToBuf(bm *sroar.Bitmap) (cloned *sroar.Bitmap, put func()) {
	return cloneToBuf(p, bm)
}

// -----------------------------------------------------------------------------

type bitmapBufPoolFixed struct {
	pool *sync.Pool
}

func NewBitmapBufPoolFixed(cap int) *bitmapBufPoolFixed {
	return &bitmapBufPoolFixed{
		pool: &sync.Pool{
			New: func() any {
				buf := make([]byte, 0, cap)
				return &buf
			},
		},
	}
}

func (p *bitmapBufPoolFixed) Get() (buf []byte, put func()) {
	ptr := p.pool.Get().(*[]byte)
	return *ptr, func() { p.pool.Put(ptr) }
}

func (p *bitmapBufPoolFixed) CloneToBuf(bm *sroar.Bitmap) (cloned *sroar.Bitmap, put func()) {
	buf, put := p.Get()
	if cap(buf) < bm.LenInBytes() {
		panic("buffer too small to fit bitmap")
	}
	return bm.CloneToBuf(buf), put
}

// -----------------------------------------------------------------------------

type bitmapBufPoolRanged struct {
	ranges     []int
	poolsFixed []*bitmapBufPoolFixed
	poolVar    *bitmapBufPoolVar
}

// Creates multiple pools, one for specified range of sizes (given in bytes).
// E.g. for ranges 1_000, 10_000 and 100_000, 4 internal buffer pools will be
// created to handle listed ranges of sizes:
// [0-1_000], [1_001-10_000], [10_001-100_000], [100_001-*]
// Ranges <=0 or duplicated will be ignored.
func NewBitmapBufPoolRanged(ranges ...int) *bitmapBufPoolRanged {
	if ln := len(ranges); ln > 0 {
		// cleanup ranges, keep unique and > 0
		unique_gt0 := map[int]struct{}{}
		for i := 0; i < ln; i++ {
			if r := ranges[i]; r > 0 {
				unique_gt0[ranges[i]] = struct{}{}
			}
		}
		i := 0
		for r := range unique_gt0 {
			ranges[i] = r
			i++
		}
		ranges = ranges[:i]
		slices.Sort(ranges)
	}

	poolsFixed := make([]*bitmapBufPoolFixed, len(ranges))
	for i := range poolsFixed {
		poolsFixed[i] = NewBitmapBufPoolFixed(ranges[i])
	}
	poolVar := NewBitmapBufPoolVar()

	return &bitmapBufPoolRanged{
		ranges:     ranges,
		poolsFixed: poolsFixed,
		poolVar:    poolVar,
	}
}

func (p *bitmapBufPoolRanged) Get(minCap int) (buf []byte, put func()) {
	for i, rng := range p.ranges {
		if minCap <= rng {
			return p.poolsFixed[i].Get()
		}
	}
	return p.poolVar.Get(minCap)
}

func (p *bitmapBufPoolRanged) CloneToBuf(bm *sroar.Bitmap) (cloned *sroar.Bitmap, put func()) {
	return cloneToBuf(p, bm)
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
} // -----------------------------------------------------------------------------

type BitmapBufPoolInUseCountingWrapper struct {
	pool         BitmapBufPool
	inUseCounter atomic.Int32
}

func NewBitmapBufPoolInUseCountingWrapper(pool BitmapBufPool) *BitmapBufPoolInUseCountingWrapper {
	return &BitmapBufPoolInUseCountingWrapper{pool: pool}
}

func (p *BitmapBufPoolInUseCountingWrapper) Get(minCap int) (buf []byte, put func()) {
	p.inUseCounter.Add(1)
	bbuf, pput := p.pool.Get(minCap)
	return bbuf, func() {
		pput()
		p.inUseCounter.Add(-1)
	}
}

func (p *BitmapBufPoolInUseCountingWrapper) CloneToBuf(bm *sroar.Bitmap) (cloned *sroar.Bitmap, put func()) {
	return cloneToBuf(p, bm)
}

func (p *BitmapBufPoolInUseCountingWrapper) Counter() int32 {
	return p.inUseCounter.Load()
}

// -----------------------------------------------------------------------------

func NewBitmapBufPoolDefault() BitmapBufPool {
	k := 1_000
	M := k * k
	return NewBitmapBufPoolRanged(1*k, 10*k, 100*k, 500*k, 1*M, 5*M, 10*M, 25*M, 50*M, 75*M, 100*M)
}
