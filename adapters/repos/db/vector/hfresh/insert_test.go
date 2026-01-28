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

package hfresh

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

func TestHFreshOptimizedPostingSize(t *testing.T) {
	tests := []struct {
		name                   string
		maxPostingSizeKB       uint32
		vectorDim              int
		expectedMaxPostingSize int
	}{
		{
			name:                   "max posting size kb defaults 1536 dim",
			maxPostingSizeKB:       48,
			vectorDim:              1536,
			expectedMaxPostingSize: 227,
		},
		{
			name:                   "max posting size kb defaults 768 dim",
			maxPostingSizeKB:       48,
			vectorDim:              768,
			expectedMaxPostingSize: 407,
		},
		{
			name:                   "max posting size kb defaults 512 dim",
			maxPostingSizeKB:       48,
			vectorDim:              512,
			expectedMaxPostingSize: 553,
		},
		{
			name:                   "max posting size kb defaults 256 dim",
			maxPostingSizeKB:       48,
			vectorDim:              256,
			expectedMaxPostingSize: 863,
		},
		{
			name:                   "max posting size kb set by the user",
			maxPostingSizeKB:       56,
			vectorDim:              256,
			expectedMaxPostingSize: 1007,
		},
		{
			name:                   "max posting size kb large vector",
			maxPostingSizeKB:       8,
			vectorDim:              4096,
			expectedMaxPostingSize: 16,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			scheduler := queue.NewScheduler(
				queue.SchedulerOptions{
					Logger: logrus.New(),
				},
			)
			cfg.Scheduler = scheduler
			cfg.RootPath = t.TempDir()
			cfg.Centroids.HNSWConfig = &hnsw.Config{
				RootPath:              t.TempDir(),
				ID:                    "hfresh",
				MakeCommitLoggerThunk: makeNoopCommitLogger,
				DistanceProvider:      distancer.NewCosineDistanceProvider(),
				MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
				AllocChecker:          memwatch.NewDummyMonitor(),
				GetViewThunk:          getViewThunk,
			}
			cfg.TombstoneCallbacks = cyclemanager.NewCallbackGroupNoop()

			scheduler.Start()
			defer scheduler.Close()

			uc := ent.NewDefaultUserConfig()
			uc.MaxPostingSizeKB = tt.maxPostingSizeKB
			store := testinghelpers.NewDummyStore(t)

			index, err := New(cfg, uc, store)
			require.NoError(t, err)
			defer index.Shutdown(t.Context())

			vector := make([]float32, tt.vectorDim)
			err = index.Add(t.Context(), 0, vector)
			require.NoError(t, err)

			maxPostingSize := index.maxPostingSize
			require.Equal(t, tt.expectedMaxPostingSize, int(maxPostingSize))
		})
	}
}
