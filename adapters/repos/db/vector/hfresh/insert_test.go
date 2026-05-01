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
	defer scheduler.Close(t.Context())
	uc := ent.NewDefaultUserConfig()
	store := testinghelpers.NewDummyStore(t)

	vector := make([]float32, 100)

	t.Run("max posting size computed by the index", func(t *testing.T) {
		uc.MaxPostingSize = 0 // it means that it will be computed dynamically by the index
		index, err := New(cfg, uc, store)
		require.NoError(t, err)
		defer index.Shutdown(t.Context())

		err = index.Add(t.Context(), 0, vector)
		require.NoError(t, err)

		maxPostingSize := index.maxPostingSize
		require.Equal(t, 99, int(maxPostingSize))
	})

	t.Run("max posting size set by the user", func(t *testing.T) {
		uc.MaxPostingSize = 56
		index, err := New(cfg, uc, store)
		require.NoError(t, err)
		defer index.Shutdown(t.Context())

		err = index.Add(t.Context(), 0, vector)
		require.NoError(t, err)

		maxPostingSize := index.maxPostingSize
		require.Equal(t, 56, int(maxPostingSize))
	})

	t.Run("max posting size small", func(t *testing.T) {
		uc.MaxPostingSize = 2
		index, err := New(cfg, uc, store)
		require.NoError(t, err)
		defer index.Shutdown(t.Context())

		err = index.Add(t.Context(), 0, vector)
		require.NoError(t, err)

		maxPostingSize := index.maxPostingSize
		require.Equal(t, 2, int(maxPostingSize))
	})
}
