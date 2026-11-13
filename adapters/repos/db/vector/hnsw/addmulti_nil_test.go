//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_| \__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestAddMultiBatch_RejectsNilVectors is a regression test for
// weaviate/weaviate#12959: AddMultiBatch indexed vectors[0][0] unconditionally,
// so a nil (or empty) multivector input panicked with an index-out-of-range
// instead of returning an error like every other malformed input.
func TestAddMultiBatch_RejectsNilVectors(t *testing.T) {
	index := newTestMultivectorIndex(t)
	ctx := context.Background()

	// nil multivector must return an error, not panic
	assert.Error(t, index.AddMultiBatch(ctx, []uint64{1}, [][][]float32{nil}))

	// empty multivector must return an error, not panic
	assert.Error(t, index.AddMultiBatch(ctx, []uint64{2}, [][][]float32{{}}))

	// nil inner vector must return an error, not panic
	assert.Error(t, index.AddMultiBatch(ctx, []uint64{3}, [][][]float32{{nil}}))

	// a valid multivector is still accepted
	assert.NoError(t, index.AddMultiBatch(ctx, []uint64{4}, [][][]float32{{{0.1, 0.2, 0.3}}}))
}
