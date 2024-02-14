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
	"runtime"
	"sync"

	"github.com/weaviate/sroar"
)

var (
	prefillBufferSize  = 65_536
	prefillMaxRoutines = 4
	_NUMCPU            = runtime.NumCPU()
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

// NewInvertedBitmap creates a bitmap that as all IDs filled from 0 to maxVal.
// Then the source bitmap is subtracted (AndNot) from the all-ids bitmap,
// resulting in a bitmap containing all ids from 0 to maxVal except the ones
// that were set on the source.
func NewInvertedBitmap(source *sroar.Bitmap, maxVal uint64) *sroar.Bitmap {
	bm := NewBitmapPrefill(maxVal)
	bm.AndNot(source)
	return bm
}

// Creates prefilled bitmap with values from 0 to maxVal (included).
//
// It is designed to be more performant both
// time-wise (compared to Set/SetMany)
// and memory-wise (compared to FromSortedList accepting entire slice of elements)
// Method creates multiple small bitmaps using FromSortedList (slice is reusable)
// and ORs them together to get final bitmap.
// For maxVal > prefillBufferSize (65_536) and multiple CPUs available task is performed
// by up to prefillMaxRoutines (4) goroutines.
func NewBitmapPrefill(maxVal uint64) *sroar.Bitmap {
	routinesLimit := prefillMaxRoutines
	if _NUMCPU < routinesLimit {
		routinesLimit = _NUMCPU
	}
	if routinesLimit == 1 || maxVal <= uint64(prefillBufferSize) {
		return newBitmapPrefillSequential(maxVal)
	}
	return newBitmapPrefillParallel(maxVal, routinesLimit)
}

func newBitmapPrefillSequential(maxVal uint64) *sroar.Bitmap {
	inc := uint64(prefillBufferSize)
	buf := make([]uint64, prefillBufferSize)
	finalBM := sroar.NewBitmap()

	for i := uint64(0); i <= maxVal; i += inc {
		j := uint64(0)
		for ; j < inc && i+j <= maxVal; j++ {
			buf[j] = i + j
		}
		finalBM.Or(sroar.FromSortedList(buf[:j]))
	}
	return finalBM
}

func newBitmapPrefillParallel(maxVal uint64, routinesLimit int) *sroar.Bitmap {
	inc := uint64(prefillBufferSize / routinesLimit)
	lock := new(sync.Mutex)
	ch := make(chan uint64, routinesLimit)
	wg := new(sync.WaitGroup)
	wg.Add(routinesLimit)
	finalBM := sroar.NewBitmap()

	for r := 0; r < routinesLimit; r++ {
		go func() {
			buf := make([]uint64, inc)

			for i := range ch {
				j := uint64(0)
				for ; j < inc && i+j <= maxVal; j++ {
					buf[j] = i + j
				}
				bm := sroar.FromSortedList(buf[:j])

				lock.Lock()
				finalBM.Or(bm)
				lock.Unlock()
			}
			wg.Done()
		}()
	}

	for i := uint64(0); i <= maxVal; i += inc {
		ch <- i
	}
	close(ch)
	wg.Wait()
	return finalBM
}

type MaxValGetterFunc func() uint64

const (
	// MaxBufferIncrement is the amount of bits greater than <maxVal> to reduce
	// the amount of times InvertedBitmapFactory has to reallocate. Note
	// that is only applied on the first initialization of the factory
	MaxBufferIncrement = uint64(1000)
	// DefaultBufferIncrement  is the amount of bits greater than <maxVal>
	// to reduce the amount of times InvertedBitmapFactory has to reallocate.
	// This value is used only on increments which occur after initialization
	DefaultBufferIncrement = uint64(100)
)

// InvertedBitmapFactory exists to prevent an expensive call to
// NewBitmapPrefill each time NewInvertedBitmap is invoked
type InvertedBitmapFactory struct {
	bitmap          *sroar.Bitmap
	maxValGetter    MaxValGetterFunc
	currentMaxVal   uint64
	bufferIncrement uint64
	lock            sync.RWMutex
}

func NewInvertedBitmapFactory(maxValGetter MaxValGetterFunc) *InvertedBitmapFactory {
	maxVal := maxValGetter() + MaxBufferIncrement
	return &InvertedBitmapFactory{
		bitmap:          NewBitmapPrefill(maxVal),
		maxValGetter:    maxValGetter,
		currentMaxVal:   maxVal,
		bufferIncrement: DefaultBufferIncrement,
	}
}

// GetBitmap returns a prefilled bitmap, which is cloned from a shared internal.
// This method is safe to call concurrently. The purpose behind sharing an
// internal bitmap, is that a Clone() operation is much cheaper than prefilling
// a map up to <maxDocID> elements is an expensive operation, and this way we
// only have to do it once.
func (bmf *InvertedBitmapFactory) GetBitmap() *sroar.Bitmap {
	bmf.lock.RLock()
	maxVal := bmf.maxValGetter()

	// We don't need to expand, maxVal is unchanged
	{
		if maxVal <= bmf.currentMaxVal {
			cloned := bmf.bitmap.Clone()
			bmf.lock.RUnlock()
			return cloned
		}
	}

	bmf.lock.RUnlock()
	bmf.lock.Lock()
	defer bmf.lock.Unlock()

	// 2nd check to ensure bitmap wasn't expanded by
	// concurrent request white waiting for write lock
	{
		maxVal = bmf.maxValGetter()
		if maxVal <= bmf.currentMaxVal {
			return bmf.bitmap.Clone()
		}
	}

	// maxVal has grown to exceed even the buffer,
	// time to expand
	{
		length := maxVal + bmf.bufferIncrement - bmf.currentMaxVal
		list := make([]uint64, length)
		for i := uint64(0); i < length; i++ {
			list[i] = bmf.currentMaxVal + i + 1
		}

		bmf.bitmap.Or(sroar.FromSortedList(list))
		bmf.currentMaxVal = maxVal + bmf.bufferIncrement
	}

	return bmf.bitmap.Clone()
}
