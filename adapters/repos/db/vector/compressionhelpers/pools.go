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

package compressionhelpers

import "sync"

// rotationBufPool pools []float32 scratch buffers used during quantizer Encode
// calls. The rotation result is purely local to each Encode invocation —
// it is consumed to write the output code and never returned to callers —
// so the buffer can safely be recycled across goroutines.
var rotationBufPool = &sync.Pool{
	New: func() any {
		b := make([]float32, 0, 512)
		return &b
	},
}

// getRotationBuf returns a []float32 handle of exactly length dim from the pool.
// The caller must call putRotationBuf when done.
func getRotationBuf(dim int) *[]float32 {
	h := rotationBufPool.Get().(*[]float32)
	if cap(*h) < dim {
		*h = make([]float32, dim)
	} else {
		*h = (*h)[:dim]
	}
	return h
}

func putRotationBuf(h *[]float32) {
	rotationBufPool.Put(h)
}
