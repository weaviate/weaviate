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

package flat

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
)

// TestFlatCompressionStats pins what a flat index reports about its own
// compression: the usage report bills cold and hot tenants from these numbers,
// so an index holding quantized vectors must not report itself uncompressed.
func TestFlatCompressionStats(t *testing.T) {
	dimensions := 64

	tests := []struct {
		name            string
		userConfig      flatent.UserConfig
		wantType        string
		wantRatio       float64
		wantCompression bool
	}{
		{
			name:       "without compression",
			userConfig: flatent.UserConfig{},
			wantType:   "none",
			wantRatio:  1,
		},
		{
			name: "with bq",
			userConfig: flatent.UserConfig{
				BQ: flatent.CompressionUserConfig{Enabled: true, RescoreLimit: 10},
			},
			wantType:        "bq",
			wantRatio:       32,
			wantCompression: true,
		},
		{
			name: "with rq on 1 bit",
			userConfig: flatent.UserConfig{
				RQ: flatent.RQUserConfig{Enabled: true, Bits: 1, RescoreLimit: 10},
			},
			wantType:        "rq",
			wantRatio:       16, // 64 * 4 bytes / (8 metadata + 8 packed bytes)
			wantCompression: true,
		},
		{
			name: "with rq on 8 bits",
			userConfig: flatent.UserConfig{
				RQ: flatent.RQUserConfig{Enabled: true, Bits: 8, RescoreLimit: 10},
			},
			wantType:        "rq",
			wantRatio:       3.2, // 64 * 4 bytes / (16 metadata + 64 codes)
			wantCompression: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			index := newTestIndex(t, tt.userConfig)

			// the RQ quantizer only exists once a vector fixed the dimensions
			require.NoError(t, index.Add(ctx, 0, make([]float32, dimensions)))
			require.Equal(t, tt.wantCompression, index.Compressed())

			stats := index.CompressionStats()
			assert.Equal(t, tt.wantType, stats.CompressionType())
			assert.InDelta(t, tt.wantRatio, stats.CompressionRatio(dimensions), 1e-9)
		})
	}
}

// newTestIndex builds a flat index on a temporary store. It is not shared with
// index_test.go: that file is excluded from race builds (//go:build !race), and
// the tests here must run under the race detector.
func newTestIndex(t *testing.T, userConfig flatent.UserConfig) *flat {
	t.Helper()

	logger, _ := test.NewNullLogger()
	dirName := t.TempDir()
	store, err := lsmkv.New(dirName, dirName, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { store.Shutdown(context.Background()) })

	index, err := New(Config{
		ID:                "compression-stats-test",
		RootPath:          dirName,
		DistanceProvider:  distancer.NewL2SquaredProvider(),
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
	}, userConfig, store)
	require.NoError(t, err)
	t.Cleanup(func() { index.Shutdown(context.Background()) })
	return index
}
