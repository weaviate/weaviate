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

// TestPrefillCacheFlushesArenaBadRecordSummary: production arena caches run
// with deletionInterval 0, so no ticker flushes the refused-record count.
// A prefill whose records all have the wrong size must still log the count
// when it finishes, not only when the cache is dropped.
func TestPrefillCacheFlushesArenaBadRecordSummary(t *testing.T) {
	const n = 200
	dir := t.TempDir()
	noop := cyclemanager.NewCallbackGroupNoop()
	store, err := lsmkv.New(dir, dir, logrus.New(), nil, nil, noop, noop, noop)
	require.NoError(t, err)
	t.Cleanup(func() { store.Shutdown(context.Background()) }) //nolint:errcheck

	logger, hook := test.NewNullLogger()
	vc, err := NewRQCompressor(distancer.NewCosineDistanceProvider(), 1e12, logger, store,
		nil, lsmkv.MakeNoopBucketOptions, 8, 64, "prefill_summary", nil)
	require.NoError(t, err)
	compressor := vc.(*quantizedVectorsCompressor[byte])

	vec := make([]float32, 64)
	for id := uint64(0); id < n; id++ {
		vec[id%64] = float32(id + 1)
		compressor.Preload(id, vec)
	}

	// a cache whose record size disagrees with every stored record, built
	// like production (interval 0: no deletion ticker)
	recordSize := len(compressor.quantizer.Encode(vec)) + 1
	arena, err := cache.NewArenaByteCache(nil, recordSize, 1e12, 1, logger, 0, nil)
	require.NoError(t, err)
	t.Cleanup(arena.Drop)
	compressor.cache = arena

	hook.Reset()
	compressor.PrefillCache(context.Background())

	var warnings []string
	for _, e := range hook.AllEntries() {
		if e.Level == logrus.WarnLevel {
			warnings = append(warnings, e.Message)
		}
	}
	require.Len(t, warnings, 2, "first refusal in full, then one summary, before any Drop")
	assert.Contains(t, warnings[0], "further size mismatches")
	assert.Contains(t, warnings[1], "skipped 199 more records")
}
