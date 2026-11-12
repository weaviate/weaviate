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

//go:build integrationTest

package db

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex/dynamic"
)

// Searches run while the indexing queue upgrades a dynamic index whose flat
// and hnsw stages use different quantizers, then compresses the hnsw.
func TestShard_SearchDuringDynamicUpgrade(t *testing.T) {
	t.Setenv("QUEUE_SCHEDULER_INTERVAL", "1ms")

	const (
		className = "DynamicUpgradeSearch"
		threshold = 1000
		objects   = 1500
		batch     = 250
		dims      = 32
		limit     = 10
	)

	tests := []struct {
		name      string
		configure func(*dynamic.UserConfig)
	}{
		{
			name: "flat RQ8 -> hnsw BQ",
			configure: func(cfg *dynamic.UserConfig) {
				cfg.FlatUC.RQ.Enabled = true
				cfg.FlatUC.RQ.Bits = 8
				cfg.HnswUC.BQ.Enabled = true
			},
		},
		{
			name: "flat BQ -> hnsw RQ8",
			configure: func(cfg *dynamic.UserConfig) {
				cfg.FlatUC.BQ.Enabled = true
				cfg.HnswUC.RQ.Enabled = true
				cfg.HnswUC.RQ.Bits = 8
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			cfg := dynamic.NewDefaultUserConfig()
			cfg.Threshold = threshold
			tt.configure(&cfg)

			class := &models.Class{Class: className}
			shd, idx := testShardWithSettings(t, ctx, class, cfg, false, true)
			t.Cleanup(func() { shd.Shutdown(context.Background()) })

			r := rand.New(rand.NewSource(1))
			randomVector := func() []float32 {
				vec := make([]float32, dims)
				for i := range vec {
					vec[i] = r.Float32()*2 - 1
				}
				return vec
			}
			query := randomVector()
			search := func(s ShardLike) ([]*storobj.Object, error) {
				res, _, err := s.ObjectVectorSearch(ctx, []models.Vector{query}, []string{""}, 0, limit,
					nil, nil, nil, additional.Properties{}, nil, nil)
				return res, err
			}

			var (
				wg        sync.WaitGroup
				stop      atomic.Bool
				searches  atomic.Int64
				searchErr atomic.Pointer[error]
			)
			wg.Add(1)
			enterrors.GoWrapper(func() {
				defer wg.Done()
				for !stop.Load() {
					if _, err := search(shd); err != nil {
						searchErr.CompareAndSwap(nil, &err)
						return
					}
					searches.Add(1)
					// leaves the cpu to the upgrade on a small ci runner
					time.Sleep(time.Millisecond)
				}
			}, idx.logger)

			for i := 0; i < objects; i += batch {
				objs := make([]*storobj.Object, batch)
				for j := range objs {
					objs[j] = &storobj.Object{
						MarshallerVersion: 1,
						Object:            models.Object{ID: strfmt.UUID(uuid.NewString()), Class: className},
						Vector:            randomVector(),
					}
				}
				for _, err := range shd.PutObjectBatch(ctx, objs) {
					require.NoError(t, err)
				}
			}

			q, releaseQueue, ok := shd.AcquireVectorIndexQueue("")
			require.True(t, ok)
			vectorIndex, releaseIndex, ok := shd.AcquireVectorIndex("")
			require.True(t, ok)
			upgradable, ok := vectorIndex.(interface{ IsUpgraded() bool })
			require.True(t, ok)

			upgraded := assert.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Zero(c, q.Size())
				assert.True(c, upgradable.IsUpgraded())
				assert.True(c, vectorIndex.Compressed())
			}, 300*time.Second, 100*time.Millisecond)
			stop.Store(true)
			wg.Wait()
			releaseIndex()
			releaseQueue()
			require.True(t, upgraded, "the index must end up as a compressed hnsw")

			if err := searchErr.Load(); err != nil {
				require.NoError(t, *err, "search during the upgrade, after %d successful ones", searches.Load())
			}
			require.Positive(t, searches.Load())

			before, err := search(shd)
			require.NoError(t, err)
			require.Len(t, before, limit)

			require.NoError(t, shd.Shutdown(ctx))
			reloaded, err := idx.initShard(ctx, shd.Name(), class, nil, false, true)
			require.NoError(t, err)
			t.Cleanup(func() { reloaded.Shutdown(context.Background()) })

			after, err := search(reloaded)
			require.NoError(t, err)
			ids := func(objs []*storobj.Object) []strfmt.UUID {
				out := make([]strfmt.UUID, len(objs))
				for i, obj := range objs {
					out[i] = obj.ID()
				}
				return out
			}
			assert.ElementsMatch(t, ids(before), ids(after), "a reloaded shard must serve the same results")
		})
	}
}
