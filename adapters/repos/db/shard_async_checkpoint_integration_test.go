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
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

const (
	ckCutoff = 1_000_000
	ckSeedTS = 800_000
	ckPreTS  = 900_000
	ckPostTS = 1_100_000
)

func ckID(n int) strfmt.UUID {
	return strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012x", n))
}

func ckCheckpointRoot(t *testing.T, ctx context.Context, sl ShardLike) hashtree.Digest {
	t.Helper()
	root, _, _, ok := sl.AsyncCheckpointRoot(ctx)
	require.True(t, ok)
	return root
}

func ckLiveRoot(t *testing.T, s *Shard) hashtree.Digest {
	t.Helper()
	root, ok := s.HashTreeRoot()
	require.True(t, ok)
	return root
}

// ckSetup enables async replication, seeds objects, and creates a checkpoint at ckCutoff; returns the checkpoint root.
func ckSetup(t *testing.T, ctx context.Context, sl ShardLike, s *Shard, seed []*storobj.Object) hashtree.Digest {
	t.Helper()
	require.NoError(t, s.enableAsyncReplication(ctx, minAsyncReplicationConfig()))
	awaitHashtreeInitialized(t, s)
	for _, o := range seed {
		require.NoError(t, sl.PutObject(ctx, o))
	}
	require.NoError(t, sl.CreateAsyncCheckpoint(ctx, ckCutoff, time.UnixMilli(ckCutoff)))
	return ckCheckpointRoot(t, ctx, sl)
}

// freshCkShard builds a property-less async-replicated shard seeded and checkpointed at ckCutoff.
func freshCkShard(t *testing.T, ctx context.Context, class string, seed []*storobj.Object) (ShardLike, *Index, *Shard, hashtree.Digest) {
	t.Helper()
	sl, idx := testShard(t, ctx, class, withAsyncScheduler(t))
	s := concreteShard(t, sl)
	return sl, idx, s, ckSetup(t, ctx, sl, s, seed)
}

func ckAssertAbsorbed(t *testing.T, ctx context.Context, sl ShardLike, s *Shard, r0 hashtree.Digest) {
	t.Helper()
	cp := ckCheckpointRoot(t, ctx, sl)
	assert.NotEqual(t, r0, cp, "≤cutoff op must move the checkpoint root")
	assert.Equal(t, ckLiveRoot(t, s), cp, "with only ≤cutoff ops the checkpoint must equal the live hashtree")
}

func ckAssertFrozen(t *testing.T, ctx context.Context, sl ShardLike, s *Shard, r0 hashtree.Digest) {
	t.Helper()
	cp := ckCheckpointRoot(t, ctx, sl)
	assert.Equal(t, r0, cp, ">cutoff op must not move the checkpoint root")
	assert.NotEqual(t, ckLiveRoot(t, s), cp, ">cutoff op must move the live hashtree away from the frozen checkpoint")
}

func ckOverwrite(t *testing.T, ctx context.Context, idx *Index, shard string, vo *objects.VObject) {
	t.Helper()
	res, err := idx.OverwriteObjects(ctx, shard, []*objects.VObject{vo})
	require.NoError(t, err)
	for _, r := range res {
		require.Empty(t, r.Err, "overwrite reported a conflict")
	}
}

func TestAsyncCheckpoint_LocalCreateJourney(t *testing.T) {
	ctx := context.Background()
	seed := []*storobj.Object{testObjWithTime("CkCreate", ckID(1), ckSeedTS)}

	t.Run("single pre-cutoff absorbed", func(t *testing.T) {
		sl, _, s, r0 := freshCkShard(t, ctx, "CkCreate", seed)
		require.NoError(t, sl.PutObject(ctx, testObjWithTime("CkCreate", ckID(2), ckPreTS)))
		ckAssertAbsorbed(t, ctx, sl, s, r0)
	})
	t.Run("single post-cutoff frozen", func(t *testing.T) {
		sl, _, s, r0 := freshCkShard(t, ctx, "CkCreate", seed)
		require.NoError(t, sl.PutObject(ctx, testObjWithTime("CkCreate", ckID(2), ckPostTS)))
		ckAssertFrozen(t, ctx, sl, s, r0)
	})
	t.Run("batch pre-cutoff absorbed", func(t *testing.T) {
		sl, _, s, r0 := freshCkShard(t, ctx, "CkCreate", seed)
		errs := sl.PutObjectBatch(ctx, []*storobj.Object{
			testObjWithTime("CkCreate", ckID(2), ckPreTS),
			testObjWithTime("CkCreate", ckID(3), ckPreTS),
		})
		for _, e := range errs {
			require.NoError(t, e)
		}
		ckAssertAbsorbed(t, ctx, sl, s, r0)
	})
	t.Run("batch post-cutoff frozen", func(t *testing.T) {
		sl, _, s, r0 := freshCkShard(t, ctx, "CkCreate", seed)
		errs := sl.PutObjectBatch(ctx, []*storobj.Object{
			testObjWithTime("CkCreate", ckID(2), ckPostTS),
			testObjWithTime("CkCreate", ckID(3), ckPostTS),
		})
		for _, e := range errs {
			require.NoError(t, e)
		}
		ckAssertFrozen(t, ctx, sl, s, r0)
	})
}

func TestAsyncCheckpoint_LocalDeleteJourney(t *testing.T) {
	ctx := context.Background()
	seed := []*storobj.Object{
		testObjWithTime("CkDelete", ckID(1), ckSeedTS),
		testObjWithTime("CkDelete", ckID(2), ckSeedTS),
	}

	t.Run("single pre-cutoff absorbed", func(t *testing.T) {
		sl, _, s, r0 := freshCkShard(t, ctx, "CkDelete", seed)
		require.NoError(t, sl.DeleteObject(ctx, ckID(2), time.UnixMilli(ckPreTS)))
		ckAssertAbsorbed(t, ctx, sl, s, r0)
	})
	t.Run("single post-cutoff frozen", func(t *testing.T) {
		sl, _, s, r0 := freshCkShard(t, ctx, "CkDelete", seed)
		require.NoError(t, sl.DeleteObject(ctx, ckID(2), time.UnixMilli(ckPostTS)))
		ckAssertFrozen(t, ctx, sl, s, r0)
	})
	t.Run("batch pre-cutoff absorbed", func(t *testing.T) {
		sl, _, s, r0 := freshCkShard(t, ctx, "CkDelete", seed)
		res := sl.DeleteObjectBatch(ctx, []strfmt.UUID{ckID(2)}, time.UnixMilli(ckPreTS), false)
		for _, r := range res {
			require.NoError(t, r.Err)
		}
		ckAssertAbsorbed(t, ctx, sl, s, r0)
	})
	t.Run("batch post-cutoff frozen", func(t *testing.T) {
		sl, _, s, r0 := freshCkShard(t, ctx, "CkDelete", seed)
		res := sl.DeleteObjectBatch(ctx, []strfmt.UUID{ckID(2)}, time.UnixMilli(ckPostTS), false)
		for _, r := range res {
			require.NoError(t, r.Err)
		}
		ckAssertFrozen(t, ctx, sl, s, r0)
	})
}

func TestAsyncCheckpoint_ReceiveFromReplicaJourney(t *testing.T) {
	ctx := context.Background()

	receiveObj := func(id strfmt.UUID, ts, stale int64) *objects.VObject {
		return &objects.VObject{
			ID:                      id,
			LatestObject:            &models.Object{ID: id, Class: "CkReceive", LastUpdateTimeUnix: ts},
			LastUpdateTimeUnixMilli: ts,
			StaleUpdateTime:         stale,
		}
	}
	receiveDelete := func(id strfmt.UUID, deletionTs, stale int64) *objects.VObject {
		return &objects.VObject{ID: id, Deleted: true, LastUpdateTimeUnixMilli: deletionTs, StaleUpdateTime: stale}
	}

	t.Run("create pre-cutoff absorbed", func(t *testing.T) {
		sl, idx, s, r0 := freshCkShard(t, ctx, "CkReceive", []*storobj.Object{testObjWithTime("CkReceive", ckID(1), ckSeedTS)})
		ckOverwrite(t, ctx, idx, s.Name(), receiveObj(ckID(2), ckPreTS, 0))
		ckAssertAbsorbed(t, ctx, sl, s, r0)
	})
	t.Run("create post-cutoff frozen", func(t *testing.T) {
		sl, idx, s, r0 := freshCkShard(t, ctx, "CkReceive", []*storobj.Object{testObjWithTime("CkReceive", ckID(1), ckSeedTS)})
		ckOverwrite(t, ctx, idx, s.Name(), receiveObj(ckID(2), ckPostTS, 0))
		ckAssertFrozen(t, ctx, sl, s, r0)
	})
	t.Run("update pre-cutoff absorbed", func(t *testing.T) {
		sl, idx, s, r0 := freshCkShard(t, ctx, "CkReceive", []*storobj.Object{testObjWithTime("CkReceive", ckID(1), ckSeedTS)})
		ckOverwrite(t, ctx, idx, s.Name(), receiveObj(ckID(1), ckPreTS, ckSeedTS))
		ckAssertAbsorbed(t, ctx, sl, s, r0)
	})
	t.Run("update post-cutoff frozen", func(t *testing.T) {
		sl, idx, s, r0 := freshCkShard(t, ctx, "CkReceive", []*storobj.Object{testObjWithTime("CkReceive", ckID(1), ckSeedTS)})
		ckOverwrite(t, ctx, idx, s.Name(), receiveObj(ckID(1), ckPostTS, ckSeedTS))
		ckAssertFrozen(t, ctx, sl, s, r0)
	})
	t.Run("delete pre-cutoff absorbed", func(t *testing.T) {
		seed := []*storobj.Object{testObjWithTime("CkReceive", ckID(1), ckSeedTS), testObjWithTime("CkReceive", ckID(2), ckSeedTS)}
		sl, idx, s, r0 := freshCkShard(t, ctx, "CkReceive", seed)
		ckOverwrite(t, ctx, idx, s.Name(), receiveDelete(ckID(2), ckPreTS, ckSeedTS))
		ckAssertAbsorbed(t, ctx, sl, s, r0)
	})
	t.Run("delete post-cutoff frozen", func(t *testing.T) {
		seed := []*storobj.Object{testObjWithTime("CkReceive", ckID(1), ckSeedTS), testObjWithTime("CkReceive", ckID(2), ckSeedTS)}
		sl, idx, s, r0 := freshCkShard(t, ctx, "CkReceive", seed)
		ckOverwrite(t, ctx, idx, s.Name(), receiveDelete(ckID(2), ckPostTS, ckSeedTS))
		ckAssertFrozen(t, ctx, sl, s, r0)
	})
}

func TestAsyncCheckpoint_MergeJourney(t *testing.T) {
	ctx := context.Background()
	class := &models.Class{
		Class: "CkMerge",
		Properties: []*models.Property{{
			Name:         "name",
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
		}},
		InvertedIndexConfig: &models.InvertedIndexConfig{UsingBlockMaxWAND: true},
	}
	seedObj := func() *storobj.Object {
		return &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:                 ckID(1),
				Class:              "CkMerge",
				Properties:         map[string]interface{}{"name": "word1"},
				LastUpdateTimeUnix: ckSeedTS,
			},
		}
	}
	setup := func(t *testing.T) (ShardLike, *Shard, hashtree.Digest) {
		sl, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, true, false, false, withAsyncScheduler(t))
		s := concreteShard(t, sl)
		return sl, s, ckSetup(t, ctx, sl, s, []*storobj.Object{seedObj()})
	}

	t.Run("pre-cutoff absorbed", func(t *testing.T) {
		sl, s, r0 := setup(t)
		require.NoError(t, sl.MergeObject(ctx, objects.MergeDocument{
			ID: ckID(1), Class: "CkMerge",
			PrimitiveSchema: map[string]interface{}{"name": "word2"},
			UpdateTime:      ckPreTS,
		}))
		ckAssertAbsorbed(t, ctx, sl, s, r0)
	})
	t.Run("post-cutoff frozen", func(t *testing.T) {
		sl, s, r0 := setup(t)
		require.NoError(t, sl.MergeObject(ctx, objects.MergeDocument{
			ID: ckID(1), Class: "CkMerge",
			PrimitiveSchema: map[string]interface{}{"name": "word2"},
			UpdateTime:      ckPostTS,
		}))
		ckAssertFrozen(t, ctx, sl, s, r0)
	})
}
