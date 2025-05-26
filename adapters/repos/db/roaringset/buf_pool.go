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
	"fmt"
	"math"
	"slices"
	"sync"

	"github.com/weaviate/sroar"
)

// import (
// 	"math"
// 	"sync"

// 	"github.com/weaviate/sroar"
// )

// type BitmapBufPool interface {
// 	Get(minCap int) (buf []byte, put func())
// 	CloneToBuf(bm *sroar.Bitmap) (cloned *sroar.Bitmap, put func())
// }

// // -----------------------------------------------------------------------------

type simpleBitmapBufPool struct {
	pool *sync.Pool
}

func NewSimpleBitmapBufPool() *simpleBitmapBufPool {
	return &simpleBitmapBufPool{
		pool: &sync.Pool{
			New: func() any {
				fmt.Printf("  ==> [simple] creating buffer\n")
				dummyBuf := make([]byte, 0)
				return &dummyBuf
			},
		},
	}
}

func (p *simpleBitmapBufPool) Get(minCap int) (buf []byte, put func()) {
	ptr := p.pool.Get().(*[]byte)
	buf = *ptr
	fmt.Printf("  ==> [simple] getting buffer\n")

	if cap(buf) < minCap {
		fmt.Printf("  ==> [simple] changing buffer curCap [%d] minCap [%d]\n\n", cap(buf), minCap)
		buf = make([]byte, 0, minCap)
		*ptr = buf
	} else if len(buf) > 0 {
		fmt.Printf("  ==> [simple] resizing buffer curCap [%d] minCap [%d]\n\n", cap(buf), minCap)
		buf = buf[:0]
		*ptr = buf
	} else {
		fmt.Printf("  ==> [simple] keeping buffer curCap [%d] minCap [%d]\n\n", cap(buf), minCap)
	}
	return buf, func() { p.pool.Put(ptr) }
}

func (p *simpleBitmapBufPool) CloneToBuf(bm *sroar.Bitmap) (cloned *sroar.Bitmap, put func()) {
	return cloneToBuf(p, bm)
}

// // -----------------------------------------------------------------------------

type multiBitmapBufPool struct {
	ranges []int
	pools  []*simpleBitmapBufPool
}

// 0 =     1_000
// 1 =    10_000
// 2 =   100_000
// 3 = 1_000_000
// 4 = 1_000_000+

func NewDefaultMultiBitmapBufPool() BitmapBufPool {
	p := NewMultiBitmapBufPool(1_000, 10_000, 100_000, 1_000_000, 10_000_000)
	return NewBitmapBufPoolFactorWrapper(p, 1.1)
}

func NewMultiBitmapBufPool(ranges ...int) *multiBitmapBufPool {
	if ln := len(ranges); ln > 0 {
		slices.Sort(ranges)
		for i := ln - 1; i >= 0; i-- {
			if ranges[i] <= 0 {
				ranges = ranges[i:]
			}
		}
	}

	ln := len(ranges)
	pools := make([]*simpleBitmapBufPool, ln+1)
	for i := 0; i < ln+1; i++ {
		pools[i] = NewSimpleBitmapBufPool()
	}

	fmt.Printf("  ==> [multi] ranges [%v] [%v]\n", ranges, pools)

	return &multiBitmapBufPool{
		ranges: ranges,
		pools:  pools,
	}
}

func (p *multiBitmapBufPool) Get(minCap int) (buf []byte, put func()) {
	i := 0
	for ; i < len(p.ranges); i++ {
		if p.ranges[i] >= minCap {
			break
		}
	}
	fmt.Printf("  ==> [multi] passing to pool [%d] minCap [%d]\n", i, minCap)
	pool := p.pools[i]
	return pool.Get(minCap)
}

func (p *multiBitmapBufPool) CloneToBuf(bm *sroar.Bitmap) (cloned *sroar.Bitmap, put func()) {
	return cloneToBuf(p, bm)
}

// // -----------------------------------------------------------------------------

func cloneToBuf(pool BitmapBufPool, bm *sroar.Bitmap) (cloned *sroar.Bitmap, put func()) {
	buf, put := pool.Get(bm.LenInBytes())
	cloned = bm.CloneToBuf(buf)
	return cloned, put
}

// // -----------------------------------------------------------------------------

// type bitmapBufPool2 struct {
// 	capFactor float64
// 	pool      *sync.Pool
// }

// func NewBitmapBufPool2(capFactor float64) *bitmapBufPool2 {
// 	return &bitmapBufPool2{
// 		capFactor: capFactor,
// 		pool: &sync.Pool{
// 			New: func() any {
// 				fmt.Printf("  ==> creating buffer\n")

// 				dummyBuf := make([]byte, 0)
// 				return &dummyBuf
// 			},
// 		},
// 	}
// }

// func (p *bitmapBufPool2) Get(minCap int) (buf []byte, put func()) {
// 	ptr := p.pool.Get().(*[]byte)
// 	buf = *ptr
// 	fmt.Printf("  ==> getting buffer\n")

// 	if cap(buf) < minCap {
// 		cp := int(math.Ceil(float64(minCap) * p.capFactor))
// 		fmt.Printf("  ==> changing buffer curCap [%d] minCap [%d] newCap [%d]\n\n", cap(buf), minCap, cp)
// 		buf = make([]byte, 0, cp)
// 		*ptr = buf
// 	} else if len(buf) > 0 {
// 		fmt.Printf("  ==> resizing buffer curCap [%d] minCap [%d]\n\n", cap(buf), minCap)
// 		buf = buf[:0]
// 		*ptr = buf
// 	} else {
// 		fmt.Printf("  ==> keeping buffer curCap [%d] minCap [%d]\n\n", cap(buf), minCap)
// 	}
// 	return buf, func() { p.pool.Put(ptr) }
// }

// func (p *bitmapBufPool2) CloneToBuf(bm *sroar.Bitmap) (cloned *sroar.Bitmap, put func()) {
// 	buf, put := p.Get(bm.LenInBytes())
// 	cloned = bm.CloneToBuf(buf)
// 	return cloned, put
// }

// // -----------------------------------------------------------------------------

type bitmapBufPoolFactorWrapper struct {
	pool   BitmapBufPool
	factor float64
}

func NewBitmapBufPoolFactorWrapper(pool BitmapBufPool, factor float64) *bitmapBufPoolFactorWrapper {
	return &bitmapBufPoolFactorWrapper{pool: pool, factor: factor}
}

func (p *bitmapBufPoolFactorWrapper) Get(minCap int) (buf []byte, put func()) {
	fmt.Printf("  ==> getting factorized minCap [%d]\n", minCap)
	return p.pool.Get(int(math.Ceil(float64(minCap) * p.factor)))
}

func (p *bitmapBufPoolFactorWrapper) CloneToBuf(bm *sroar.Bitmap) (cloned *sroar.Bitmap, put func()) {
	return cloneToBuf(p, bm)
}

// 1_000, 10_000, 100_000, 1_000_000

// // -----------------------------------------------------------------------------

// type bitmapBufPool struct {
// 	pool          *sync.Pool
// 	lock          *sync.Mutex
// 	capFactor     float32
// 	initialMinCap int
// }

// func NewBitmapBufPool(initialMinCap int, capFactor float32) *bitmapBufPool {
// 	p := &bitmapBufPool{
// 		capFactor:     capFactor,
// 		initialMinCap: initialMinCap,
// 		lock:          new(sync.Mutex),
// 	}

// 	p.pool = &sync.Pool{
// 		New: p.createBuf,
// 	}
// 	return p
// }

// func (p *bitmapBufPool) Get(minCap int) (buf []byte, put func()) {
// 	ptr := p.pool.Get().(*[]byte)
// 	buf = *ptr
// 	if cap(buf) < minCap {
// 		p.updateInitialMinCap(minCap)
// 		*ptr = *p.createBuf().(*[]byte)
// 		buf = *ptr
// 	} else if len(buf) > 0 {
// 		buf = buf[:0]
// 		*ptr = buf
// 	}
// 	return buf, func() { p.pool.Put(ptr) }
// }

// func (p *bitmapBufPool) CloneToBuf(bm *sroar.Bitmap) (cloned *sroar.Bitmap, put func()) {
// 	buf, put := p.Get(bm.LenInBytes())
// 	cloned = bm.CloneToBuf(buf)
// 	return
// }

// func (p *bitmapBufPool) createBuf() any {
// 	p.lock.Lock()
// 	minCap := p.initialMinCap
// 	p.lock.Unlock()

// 	buf := make([]byte, 0, int(float32(minCap)*p.capFactor))
// 	return &buf
// }

// func (p *bitmapBufPool) updateInitialMinCap(minCap int) {
// 	p.lock.Lock()
// 	if minCap > p.initialMinCap {
// 		p.initialMinCap = minCap
// 	}
// 	p.lock.Unlock()
// }

// // -----------------------------------------------------------------------------

// type bitmapBufPoolNoop struct{}

// func NewBitmapBufPoolNoop() *bitmapBufPoolNoop {
// 	return &bitmapBufPoolNoop{}
// }

// func (p *bitmapBufPoolNoop) Get(minCap int) (buf []byte, put func()) {
// 	return make([]byte, 0, minCap), func() {}
// }

// func (p *bitmapBufPoolNoop) CloneToBuf(bm *sroar.Bitmap) (cloned *sroar.Bitmap, put func()) {
// 	buf, put := p.Get(bm.LenInBytes())
// 	cloned = bm.CloneToBuf(buf)
// 	return
// }
