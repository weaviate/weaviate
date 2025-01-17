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
	"sync"

	"github.com/weaviate/sroar"
)

func NewBitmap(values ...uint64) *sroar.Bitmap {
	bm := sroar.NewBitmap()
	bm.SetMany(values)
	return bm
}

// Operations on bitmaps may result in oversized instances in relation to
// number of elements currently contained in bitmap
// Examples of such operations:
// - And-ing bitmaps may results in size being sum of both sizes
// (especially and-ing bitmap with itself)
// - Removing elements from bitmap results in size not being reduced
// (even if there is only few or no elements left)
//
// Method should be used before saving bitmap to file, to ensure
// minimal required size
//
// For most cases Or between empty bitmap and used bitmap
// works pretty well for reducing its final size, except for use case,
// where used bitmap uses internally bitmap - it will not be converted
// to underlying array, even if there are single elements left
func Condense(bm *sroar.Bitmap) *sroar.Bitmap {
	condensed := sroar.NewBitmap()
	condensed.Or(bm)
	return condensed
}

// defaultIdIncrement  is the amount of bits greater than <maxId>
// to reduce the amount of times BitmapFactory has to reallocate.
const defaultIdIncrement = uint64(1024)

type MaxIdGetterFunc func() uint64

// BitmapFactory exists to provide prefilled bitmaps using pool (reducing allocation of memory)
// and favor cloning (faster) over prefilling bitmap from scratch each time bitmap is requested
type BitmapFactory struct {
	lock           *sync.RWMutex
	prefilled      *sroar.Bitmap
	bufPool        *BitmapBufPool
	maxIdGetter    MaxIdGetterFunc
	prefilledMaxId uint64
}

func NewBitmapFactory(maxIdGetter MaxIdGetterFunc) *BitmapFactory {
	prefilledMaxId := maxIdGetter() + defaultIdIncrement
	prefilled := sroar.Prefill(prefilledMaxId)

	return &BitmapFactory{
		lock:           new(sync.RWMutex),
		prefilled:      prefilled,
		bufPool:        NewBitmapBufPool(prefilled.LenInBytes(), 1.2),
		maxIdGetter:    maxIdGetter,
		prefilledMaxId: prefilledMaxId,
	}
}

// GetPrefilledBitmap returns a prefilled bitmap, which is cloned from a shared internal.
// This method is safe to call concurrently. The purpose behind sharing an
// internal bitmap, is that a Clone() operation is cheaper than prefilling
// a bitmap up to <maxDocID>
func (bmf *BitmapFactory) GetPrefilledBitmap() (cloned *sroar.Bitmap, release func()) {
	bitmap, release, removeRange := bmf.getPrefilledBitmap()
	bitmap.RemoveRange(removeRange[0], removeRange[1])
	return bitmap, release
}

func (bmf *BitmapFactory) GetEmptyBitmap() (empty *sroar.Bitmap, release func()) {
	bmf.lock.RLock()
	l := bmf.prefilled.LenInBytes()
	bmf.lock.RUnlock()

	buf, release := bmf.bufPool.Get(l)
	empty = sroar.NewBitmap().CloneToBuf(buf)
	return
}

func (bmf *BitmapFactory) getPrefilledBitmap() (cloned *sroar.Bitmap, release func(), removeRange [2]uint64) {
	bmf.lock.RLock()

	maxId := bmf.maxIdGetter()
	prefilledMaxId := bmf.prefilledMaxId

	// No need to expand, maxId is included
	if maxId <= prefilledMaxId {
		cloned, release = bmf.clone()
		bmf.lock.RUnlock()
		return cloned, release, [2]uint64{maxId + 1, prefilledMaxId + 1}
	}

	bmf.lock.RUnlock()
	bmf.lock.Lock()
	defer bmf.lock.Unlock()

	maxId = bmf.maxIdGetter()
	prefilledMaxId = bmf.prefilledMaxId

	// 2nd check to ensure bitmap wasn't expanded by
	// concurrent request white waiting for write lock
	if maxId <= prefilledMaxId {
		cloned, release = bmf.clone()
		return cloned, release, [2]uint64{maxId + 1, prefilledMaxId + 1}
	}

	// expand bitmap with additional ids
	prefilledMaxId = maxId + defaultIdIncrement
	bmf.prefilled.FillUp(prefilledMaxId)
	bmf.prefilledMaxId = prefilledMaxId
	cloned, release = bmf.clone()
	return cloned, release, [2]uint64{maxId + 1, prefilledMaxId + 1}
}

func (bmf *BitmapFactory) clone() (cloned *sroar.Bitmap, release func()) {
	buf, release := bmf.bufPool.Get(bmf.prefilled.LenInBytes())
	cloned = bmf.prefilled.CloneToBuf(buf)
	return
}

type BitmapBufPool struct {
	pool          *sync.Pool
	lock          *sync.Mutex
	capFactor     float32
	initialMinCap int
}

func NewBitmapBufPool(initialMinCap int, capFactor float32) *BitmapBufPool {
	p := &BitmapBufPool{
		capFactor:     capFactor,
		initialMinCap: initialMinCap,
		lock:          new(sync.Mutex),
	}

	p.pool = &sync.Pool{
		New: p.createBuf,
	}
	return p
}

func (p *BitmapBufPool) Get(minCap int) (buf []byte, put func()) {
	ptr := p.pool.Get().(*[]byte)
	buf = *ptr
	if cap(buf) < minCap {
		p.updateInitialMinCap(minCap)
		ptr = p.createBuf().(*[]byte)
		buf = *ptr
	} else if len(buf) > 0 {
		buf = buf[:0]
		*ptr = buf
	}
	return buf, func() { p.pool.Put(ptr) }
}

func (p *BitmapBufPool) createBuf() any {
	p.lock.Lock()
	minCap := p.initialMinCap
	p.lock.Unlock()

	buf := make([]byte, 0, int(float32(minCap)*p.capFactor))
	return &buf
}

func (p *BitmapBufPool) updateInitialMinCap(minCap int) {
	p.lock.Lock()
	if minCap > p.initialMinCap {
		p.initialMinCap = minCap
	}
	p.lock.Unlock()
}
