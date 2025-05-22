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

type bitmapBufPool2 struct {
	capFactor float64
	pool      *sync.Pool
}

func NewBitmapBufPool2(capFactor float64) *bitmapBufPool2 {
	return &bitmapBufPool2{
		capFactor: capFactor,
		pool: &sync.Pool{
			New: func() any {
				dummyBuf := make([]byte, 0)
				return &dummyBuf
			},
		},
	}
}

func (p *bitmapBufPool2) Get(minCap int) (buf []byte, put func()) {
	ptr := p.pool.Get().(*[]byte)
	buf = *ptr
	if cap(buf) < minCap {
		cap := int(math.Ceil(float64(minCap) * p.capFactor))
		buf = make([]byte, 0, cap)
		*ptr = buf
	} else if len(buf) > 0 {
		buf = buf[:0]
		*ptr = buf
	}
	return buf, func() { p.pool.Put(ptr) }
}

func (p *bitmapBufPool2) CloneToBuf(bm *sroar.Bitmap) (cloned *sroar.Bitmap, put func()) {
	buf, put := p.Get(bm.LenInBytes())
	cloned = bm.CloneToBuf(buf)
	return cloned, put
}

// // -----------------------------------------------------------------------------

// type bitmapBufPoolFactorWrapper struct {
// 	bufPool BitmapBufPool
// 	factor  float64
// }

// func NewBitmapBufPoolFactorWrapper(bufPool BitmapBufPool, factor float64) *bitmapBufPoolFactorWrapper {
// 	return &bitmapBufPoolFactorWrapper{bufPool: bufPool, factor: factor}
// }

// func (p *bitmapBufPoolFactorWrapper) Get(minCap int) (buf []byte, put func()) {
// 	return p.bufPool.Get(int(math.Ceil(float64(minCap) * p.factor)))
// }

// func (p *bitmapBufPoolFactorWrapper) CloneToBuf(bm *sroar.Bitmap) (cloned *sroar.Bitmap, put func()) {
// 	buf, put := p.Get(bm.LenInBytes())
// 	cloned = bm.CloneToBuf(buf)
// 	return cloned, put
// }

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
