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

package compressionhelpers_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

// Pins the nil-Bucket panic: a compressor outliving the store's Shutdown
// must skip on a nil bucket instead of panicking.
func TestCompressorSurvivesStoreShutdown(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	compressor, err := compressionhelpers.NewBQCompressor(
		distancer.NewCosineDistanceProvider(), 1e12, nil, store,
		lsmkv.MakeNoopBucketOptions, nil, "name", nil)
	require.NoError(t, err)

	compressor.Preload(1, []float32{-0.5, 0.5})
	require.NoError(t, store.Shutdown(context.Background()))

	compressor.Preload(2, []float32{0.25, 0.7})
	compressor.Delete(context.Background(), 1)
	compressor.PrefillCache(context.Background())
}
