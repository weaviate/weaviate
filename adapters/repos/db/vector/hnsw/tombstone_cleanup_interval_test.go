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

package hnsw

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// The tombstone cleanup callback group is driven by one collection-wide ticker
// that runs at the shortest interval of any vector in the collection. Each index
// must therefore only run on the ticks where its own interval has elapsed.
func TestTombstoneCleanup_RunsAtConfiguredInterval(t *testing.T) {
	tests := []struct {
		name                  string
		cleanupIntervalSecs   int
		runsOnImmediateRetick bool
	}{
		{
			name:                  "long interval runs once until the interval elapses",
			cleanupIntervalSecs:   3600,
			runsOnImmediateRetick: false,
		},
		{
			name:                  "zero interval runs on every tick of the shared ticker",
			cleanupIntervalSecs:   0,
			runsOnImmediateRetick: true,
		},
		{
			name:                  "negative interval runs on every tick of the shared ticker",
			cleanupIntervalSecs:   -1,
			runsOnImmediateRetick: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			logger, _ := test.NewNullLogger()
			neverAbort := func() bool { return false }

			callbacks := cyclemanager.NewCallbackGroup("tombstone_cleanup", logger, 1)
			index, err := New(createVectorHnswIndexTestConfig(), ent.UserConfig{
				MaxConnections:         30,
				EFConstruction:         60,
				EF:                     36,
				VectorCacheMaxObjects:  100000,
				CleanupIntervalSeconds: tt.cleanupIntervalSecs,
			}, callbacks, testinghelpers.NewDummyStore(t))
			require.NoError(t, err)
			defer index.Shutdown(ctx)
			index.PostStartup(ctx)

			for i, vec := range testVectors {
				require.NoError(t, index.Add(ctx, uint64(i), vec))
			}

			require.NoError(t, index.Delete(0))
			require.Equal(t, 1, numTombstones(t, index))

			// WithIntervals back-dates the start, so the first tick always runs.
			require.True(t, callbacks.CycleCallback(neverAbort), "first tick must run the cleanup")
			require.Equal(t, 0, numTombstones(t, index))

			require.NoError(t, index.Delete(1))
			require.Equal(t, 1, numTombstones(t, index))

			ran := callbacks.CycleCallback(neverAbort)
			require.Equal(t, tt.runsOnImmediateRetick, ran)
			if tt.runsOnImmediateRetick {
				require.Equal(t, 0, numTombstones(t, index))
			} else {
				require.Equal(t, 1, numTombstones(t, index), "cleanup must not run again before its interval elapsed")
			}
		})
	}
}

func numTombstones(t *testing.T, index *hnsw) int {
	stats, err := index.Stats()
	require.NoError(t, err)
	return stats.NumTombstones
}
