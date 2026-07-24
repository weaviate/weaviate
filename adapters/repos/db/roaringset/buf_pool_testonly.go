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
	"sync/atomic"

	"github.com/weaviate/sroar"
)

// BitmapBufPoolTrackingForTests is a pool for use in tests. It tracks outstanding
// allocations and zeroes the backing buffer on release, so that any bitmap
// read after its release returns zeros — making premature releases visible as
// wrong values in test assertions. Double-release panics immediately.
//
// It lives in a non-test file because tests of several packages consume it;
// nothing in production code may use it.
//
// Call Outstanding() in a t.Cleanup to assert all buffers were released.
type BitmapBufPoolTrackingForTests struct {
	outstanding atomic.Int64
}

func NewBitmapBufPoolTrackingForTests() *BitmapBufPoolTrackingForTests {
	return &BitmapBufPoolTrackingForTests{}
}

func (p *BitmapBufPoolTrackingForTests) Get(minCap int) (buf []byte, put func()) {
	p.outstanding.Add(1)
	buf = make([]byte, 0, max(minCap, 0))
	var released atomic.Bool
	return buf, func() {
		if !released.CompareAndSwap(false, true) {
			panic("bitmap buffer released twice")
		}
		clear(buf[:cap(buf)])
		p.outstanding.Add(-1)
	}
}

func (p *BitmapBufPoolTrackingForTests) CloneToBuf(bm *sroar.Bitmap) (cloned *sroar.Bitmap, put func()) {
	return cloneToBuf(p, bm)
}

func (p *BitmapBufPoolTrackingForTests) CloneBytesToBuf(src []byte) (cloned *sroar.Bitmap, put func()) {
	return cloneBytesToBuf(p, src)
}

// Outstanding returns the number of buffers that have been allocated but not
// yet released. A non-zero value at the end of a test indicates a leak.
func (p *BitmapBufPoolTrackingForTests) Outstanding() int64 {
	return p.outstanding.Load()
}
