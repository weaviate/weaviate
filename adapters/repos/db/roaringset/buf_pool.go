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

	"github.com/weaviate/sroar"
)

// -----------------------------------------------------------------------------

type BitmapBufPool interface {
	Get(minCap int) (buf []byte, put func())
	CloneToBuf(bm *sroar.Bitmap) (cloned *sroar.Bitmap, put func())
}

func cloneToBuf(pool BitmapBufPool, bm *sroar.Bitmap) (cloned *sroar.Bitmap, put func()) {
	buf, put := pool.Get(bm.LenInBytes())
	cloned = bm.CloneToBuf(buf)
	return cloned, put
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

type bitmapBufPoolBasic struct {
	pool *sync.Pool
}

func NewBitmapBufPoolBasic() *bitmapBufPoolBasic {
	return &bitmapBufPoolBasic{
		pool: &sync.Pool{
			New: func() any {
				dummyBuf := make([]byte, 0)
				return &dummyBuf
			},
		},
	}
}

func (p *bitmapBufPoolBasic) Get(minCap int) (buf []byte, put func()) {
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

func (p *bitmapBufPoolBasic) CloneToBuf(bm *sroar.Bitmap) (cloned *sroar.Bitmap, put func()) {
	return cloneToBuf(p, bm)
}

// -----------------------------------------------------------------------------

type bitmapBufPoolRanged struct {
	ranges []int
	pools  []*bitmapBufPoolBasic
}

// Creates multiple pools, one for specified range of sizes (given in bytes).
// E.g. for ranges 1_000, 10_000 and 100_000, 4 internal buffer pools will be
// created to handle listed ranges of sizes:
// [0-1_000], [1_001-10_000], [10_001-100_000], [100_001-*]
// Ranges <=0 or duplicated will be ignored.
func NewBitmapBufPoolRanged(ranges ...int) *bitmapBufPoolRanged {
	if ln := len(ranges); ln > 0 {
		// cleanup ranges
		m := map[int]struct{}{}
		for i := 0; i < ln; i++ {
			if r := ranges[i]; r > 0 {
				m[ranges[i]] = struct{}{}
			}
		}
		i := 0
		for r := range m {
			ranges[i] = r
			i++
		}
		ranges = ranges[:i]
		slices.Sort(ranges)
	}

	ln := len(ranges)
	pools := make([]*bitmapBufPoolBasic, ln+1)
	for i := 0; i < ln+1; i++ {
		pools[i] = NewBitmapBufPoolBasic()
	}

	return &bitmapBufPoolRanged{
		ranges: ranges,
		pools:  pools,
	}
}

func (p *bitmapBufPoolRanged) Get(minCap int) (buf []byte, put func()) {
	i := 0
	for ; i < len(p.ranges); i++ {
		if minCap <= p.ranges[i] {
			break
		}
	}
	pool := p.pools[i]
	return pool.Get(minCap)
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
}

// -----------------------------------------------------------------------------

func NewBitmapBufPoolDefault() BitmapBufPool {
	pool := NewBitmapBufPoolRanged(1_000, 10_000, 100_000, 1_000_000, 10_000_000)
	return NewBitmapBufPoolFactorWrapper(pool, 1.1)
}
