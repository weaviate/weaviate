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
	"golang.org/x/sync/errgroup"
)

var (
	filledToBufferSize  = 65_536
	filledToMaxRoutines = 4
	_NUMCPU             = runtime.NumCPU()
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
// works pretty well for reducing its final size, except for usecase,
// where used bitmap uses internally bitmap - it will not be converted
// to underlying array, even if there are single elements left
func Condense(bm *sroar.Bitmap) *sroar.Bitmap {
	condensed := sroar.NewBitmap()
	condensed.Or(bm)
	return condensed
}

func NewBitmapPrefill(maxVal uint64) *sroar.Bitmap {
	routinesLimit := filledToMaxRoutines
	if _NUMCPU < routinesLimit {
		routinesLimit = _NUMCPU
	}
	// 1 routine available or maxVal less than values handled by 1 routine
	if routinesLimit == 1 || maxVal < uint64(filledToBufferSize/routinesLimit) {
		return newBitmapPrefillSequential(maxVal)
	}
	return newBitmapPrefillParallel(maxVal, routinesLimit)
}

func newBitmapPrefillSequential(maxVal uint64) *sroar.Bitmap {
	inc := uint64(filledToBufferSize)
	buf := make([]uint64, filledToBufferSize)
	finalBM := sroar.NewBitmap()

	for i := uint64(1); i < maxVal; i += inc {
		j := uint64(0)
		for ; j < inc && i+j <= maxVal; j++ {
			buf[j] = i + j
		}
		finalBM.Or(sroar.FromSortedList(buf[:j]))
	}
	return finalBM
}

func newBitmapPrefillParallel(maxVal uint64, routinesLimit int) *sroar.Bitmap {
	inc := uint64(filledToBufferSize / routinesLimit)
	bufs := make([][]uint64, routinesLimit)
	freeBufIdsFifo := make([]int, routinesLimit)
	finalBM := sroar.NewBitmap()

	eg := new(errgroup.Group)
	eg.SetLimit(routinesLimit)
	lock := new(sync.Mutex)

	for i := range bufs {
		bufs[i] = make([]uint64, inc)
		freeBufIdsFifo[i] = i
	}

	for i := uint64(1); i < maxVal; i += inc {
		i := i

		eg.Go(func() error {
			lock.Lock()
			bufId := freeBufIdsFifo[0]
			freeBufIdsFifo = freeBufIdsFifo[1:]
			lock.Unlock()

			j := uint64(0)
			for ; j < inc && i+j <= maxVal; j++ {
				bufs[bufId][j] = i + j
			}
			bm := sroar.FromSortedList(bufs[bufId][:j])

			lock.Lock()
			freeBufIdsFifo = append(freeBufIdsFifo, bufId)
			finalBM.Or(bm)
			lock.Unlock()

			return nil
		})
	}
	eg.Wait()
	return finalBM
}
