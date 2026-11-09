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

import (
	"context"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestCenteredRQ1HonorsCacheToggle: under VECTOR_CACHE_IMPL=arena a centered
// 1-bit RQ index must use the arena both when it is created and when it is
// restored, as uncentered RQ1 and centered RQ4 do. Creation used to build the
// sharded cache directly, so the same index switched implementation on its
// first restart.
func TestCenteredRQ1HonorsCacheToggle(t *testing.T) {
	t.Setenv(cache.VectorCacheImplEnv, "arena")
	const dim = 200
	provider := distancer.NewCosineDistanceProvider()
	mean := make([]float32, dim)
	vec := make([]float32, dim)
	for i := range mean {
		mean[i] = 0.01
		vec[i] = float32(i%7) - 3
	}

	dir := t.TempDir()
	noop := cyclemanager.NewCallbackGroupNoop()
	store, err := lsmkv.New(dir, dir, logrus.New(), nil, nil, noop, noop, noop)
	require.NoError(t, err)
	t.Cleanup(func() { store.Shutdown(context.Background()) }) //nolint:errcheck

	usedArena := func(hook *test.Hook) bool {
		for _, e := range hook.AllEntries() {
			if e.Data["action"] == "vector_cache_impl" && strings.HasPrefix(e.Message, "using arena uint64 cache") {
				return true
			}
		}
		return false
	}
	roundTrip := func(t *testing.T, c VectorCompressor) {
		t.Helper()
		compressor := c.(*quantizedVectorsCompressor[uint64])
		_, ok := compressor.cache.(cache.PreloadSummarizer)
		require.True(t, ok, "the arena is the only uint64 cache implementing PreloadSummarizer")
		code := compressor.quantizer.Encode(vec)
		compressor.cache.Grow(1)
		compressor.cache.Preload(1, code)
		got, err := compressor.cache.Get(context.Background(), 1)
		require.NoError(t, err, "the arena record size must match the centered code")
		assert.Equal(t, code, got)
	}

	createLogger, createHook := test.NewNullLogger()
	created, err := NewCenteredRQ1Compressor(provider, 1e9, createLogger, store, nil,
		lsmkv.MakeNoopBucketOptions, dim, mean, "centered_rq1_toggle_create", nil)
	require.NoError(t, err)
	t.Cleanup(func() { created.Drop() }) //nolint:errcheck
	require.True(t, usedArena(createHook), "create must honor the toggle")
	roundTrip(t, created)

	data := created.(*quantizedVectorsCompressor[uint64]).quantizer.(*CenteredBinaryRotationalQuantizer).Data()
	restoreLogger, restoreHook := test.NewNullLogger()
	restored, err := RestoreRQCompressor(provider, 1e9, restoreLogger, int(data.InputDim), 1,
		int(data.Rotation.OutputDim), int(data.Rotation.Rounds), data.Rotation.Swaps, data.Rotation.Signs,
		nil, data.Mean, store, nil, lsmkv.MakeNoopBucketOptions, "centered_rq1_toggle_restore", nil)
	require.NoError(t, err)
	t.Cleanup(func() { restored.Drop() }) //nolint:errcheck
	require.True(t, usedArena(restoreHook), "restore honors the toggle")
	roundTrip(t, restored)
}
