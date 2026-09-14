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
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/config"
)

// TestLazyDropVectorIndex_PendingShutdownGoesThroughTheShard pins how a drop is
// routed while a deactivation still waits on a held reference. The shard is
// loaded, its shutdown pending and its store open, so the vector's files have
// to go through that store. Removed from disk instead, the store's final flush
// writes into directories that are gone once the reference is released: the
// teardown fails, and the tenant cannot be loaded again until restart.
func TestLazyDropVectorIndex_PendingShutdownGoesThroughTheShard(t *testing.T) {
	ctx := t.Context()

	const (
		tenant  = "pending-shutdown"
		keep    = "keep"
		dropped = "to_drop"
		dims    = 64
		objects = 1500
	)

	keepCfg := hnsw.NewDefaultUserConfig()
	// BQ gives the dropped vector a bucket that every insert writes through the
	// store, which is the bucket a removal from disk pulls out from under it.
	dropCfg := hnsw.NewDefaultUserConfig()
	dropCfg.BQ = hnsw.BQConfig{Enabled: true}

	class := &models.Class{
		Class: "LazyDropPendingShutdown",
		InvertedIndexConfig: &models.InvertedIndexConfig{
			UsingBlockMaxWAND: config.DefaultUsingBlockMaxWAND,
		},
		Properties: []*models.Property{{
			Name:         "label",
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWord,
		}},
		VectorConfig: map[string]models.VectorConfig{
			keep:    {VectorIndexType: keepCfg.IndexType(), VectorIndexConfig: keepCfg},
			dropped: {VectorIndexType: dropCfg.IndexType(), VectorIndexConfig: dropCfg},
		},
	}
	_, idx := testShardWithSettings(t, ctx, class, hnsw.UserConfig{Distance: common.DefaultDistanceMetric},
		false, true, false, func(i *Index) {
			i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{
				keep:    keepCfg,
				dropped: dropCfg,
			}
		})

	newLazy := func() *LazyLoadShard {
		return NewLazyLoadShard(ctx, nil, tenant, idx, class, idx.centralJobQueue,
			idx.indexCheckpoints, idx.allocChecker, idx.shardLoadLimiter, idx.shardReindexer,
			false, idx.bitmapBufPool)
	}
	vector := func(i, offset int) []float32 {
		v := make([]float32, dims)
		for j := range v {
			v[j] = float32(i + j + offset)
		}
		return v
	}

	lazy := newLazy()
	require.NoError(t, lazy.Load(ctx))
	for i := 0; i < objects; i++ {
		require.NoError(t, lazy.PutObject(ctx, &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:         strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String()),
				Class:      class.Class,
				Properties: map[string]interface{}{"label": fmt.Sprintf("obj-%d", i)},
			},
			Vectors: map[string][]float32{keep: vector(i, 0), dropped: vector(i, 1000)},
		}))
	}

	// A request still holds the shard, so the deactivation gives up waiting and
	// leaves its shutdown pending.
	release, err := lazy.shard.preventShutdown()
	require.NoError(t, err)
	require.ErrorIs(t, lazy.Shutdown(ctx), errShardStillInUse)
	require.True(t, lazy.isLoaded(), "precondition: a timed-out deactivation leaves the shard loaded")

	require.NoError(t, lazy.DropVectorIndex(ctx, dropped))

	release()
	require.Eventually(t, func() bool { return lazy.shard.shut.Load() }, 10*time.Second, 50*time.Millisecond,
		"precondition: releasing the last reference completes the pending shutdown")
	require.NoError(t, shardTeardownError(lazy),
		"the pending shutdown failed: the drop removed the vector's files from disk while the "+
			"store still held them open, so its final flush wrote into directories that were gone")

	again := newLazy()
	require.NoError(t, again.Load(ctx),
		"the tenant cannot be loaded again: a failed teardown keeps its buckets registered "+
			"until the process restarts")
	require.NoError(t, again.Shutdown(ctx))
}
