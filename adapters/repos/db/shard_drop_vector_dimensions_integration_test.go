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
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	shardusage "github.com/weaviate/weaviate/adapters/repos/db/shard_usage"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/config"
)

const dropDimsClassName = "DropVectorDimensionsClass"

const (
	dropDimsKeep    = "keep"
	dropDimsDropped = "to_drop"
	dropDimsDim     = 8
	dropDimsCount   = 10
)

// setupDropDimsShard builds a loaded, dimension-tracking shard with two named
// hnsw vectors.
func setupDropDimsShard(t *testing.T, ctx context.Context) (*Shard, *models.Class) {
	t.Helper()

	cfg := hnsw.NewDefaultUserConfig()
	class := &models.Class{
		Class: dropDimsClassName,
		InvertedIndexConfig: &models.InvertedIndexConfig{
			UsingBlockMaxWAND: config.DefaultUsingBlockMaxWAND,
		},
		Properties: []*models.Property{
			{
				Name:         "label",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWord,
			},
		},
		VectorConfig: map[string]models.VectorConfig{
			dropDimsKeep:    {VectorIndexType: cfg.IndexType(), VectorIndexConfig: cfg},
			dropDimsDropped: {VectorIndexType: cfg.IndexType(), VectorIndexConfig: cfg},
		},
	}

	vic := hnsw.UserConfig{Distance: common.DefaultDistanceMetric}
	shardLike, _ := testShardWithSettings(t, ctx, class, vic, false, false, func(i *Index) {
		// Applied before initShard, which is what creates the dimensions bucket.
		i.Config.TrackVectorDimensions = true
		i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{
			dropDimsKeep:    cfg,
			dropDimsDropped: cfg,
		}
	})

	switch s := shardLike.(type) {
	case *Shard:
		return s, class
	case *LazyLoadShard:
		shard, err := s.Unwrap(ctx)
		require.NoError(t, err)
		return shard, class
	default:
		t.Fatalf("unexpected shard type %T", shardLike)
		return nil, nil
	}
}

func dropDimsObject(i int) *storobj.Object {
	vec := make([]float32, dropDimsDim)
	for j := range vec {
		vec[j] = float32(i + j)
	}
	other := make([]float32, dropDimsDim)
	for j := range other {
		other[j] = float32(i + j + 100)
	}
	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:         strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String()),
			Class:      dropDimsClassName,
			Properties: map[string]interface{}{"label": fmt.Sprintf("obj-%d", i)},
		},
		Vectors: map[string][]float32{dropDimsKeep: vec, dropDimsDropped: other},
	}
}

// TestDropVectorIndex_ClearsDimensionRows pins that the drop takes the vector's
// rows out of the shard's dimensions bucket. Nothing else reclaims them: the
// drop strips the vector from the object bytes, so a later update or delete
// finds nothing to tombstone, and the rows are inherited by the next vector
// created under the same name.
//
// The clear is deliberately NOT part of Shard.DropVectorIndex — that runs
// inline in the RAFT apply — so this drives the route the drop task takes for
// one unit, and asserts the split on the way.
func TestDropVectorIndex_ClearsDimensionRows(t *testing.T) {
	ctx := t.Context()
	s, _ := setupDropDimsShard(t, ctx)

	for i := 0; i < dropDimsCount; i++ {
		require.NoError(t, s.PutObject(ctx, dropDimsObject(i)))
	}

	wantAll := dropDimsCount * dropDimsDim
	gotKeep, err := s.Dimensions(ctx, dropDimsKeep)
	require.NoError(t, err)
	require.Equal(t, wantAll, gotKeep, "precondition: %q must be tracked", dropDimsKeep)
	gotDropped, err := s.Dimensions(ctx, dropDimsDropped)
	require.NoError(t, err)
	require.Equal(t, wantAll, gotDropped, "precondition: %q must be tracked", dropDimsDropped)

	require.NoError(t, s.DropVectorIndex(ctx, dropDimsDropped))

	gotDropped, err = s.Dimensions(ctx, dropDimsDropped)
	require.NoError(t, err)
	require.Equal(t, wantAll, gotDropped,
		"the index drop cleared the rows itself; it runs inline in the RAFT apply "+
			"for every shard of the collection, and this clear is O(objects)")

	require.NoError(t, removeDimensionsForDroppedVector(ctx, s.index, s.name, dropDimsDropped))

	gotDropped, err = s.Dimensions(ctx, dropDimsDropped)
	require.NoError(t, err)
	require.Equal(t, 0, gotDropped,
		"the dropped vector's dimension rows survived the unit's clear; a vector "+
			"re-created under the name %q would inherit them and be counted before it "+
			"holds a single vector", dropDimsDropped)

	gotKeep, err = s.Dimensions(ctx, dropDimsKeep)
	require.NoError(t, err)
	require.Equal(t, wantAll, gotKeep,
		"the surviving sibling %q lost its dimension rows: the bucket is shared by "+
			"every vector on the shard, so the drop must remove one key range, not "+
			"the bucket", dropDimsKeep)
}

// TestDropVectorIndex_DimensionsPrefixIsNotEnough pins the key-length filter:
// a shorter name is a byte prefix of every key the longer one owns.
func TestDropVectorIndex_DimensionsPrefixIsNotEnough(t *testing.T) {
	ctx := t.Context()
	s, _ := setupDropDimsShard(t, ctx)

	// Written straight to the bucket — these names need rows, not indexes.
	const shortName, longName = "vec", "vec_extra"
	for docID := uint64(0); docID < 3; docID++ {
		require.NoError(t, s.addToDimensionBucket(dropDimsDim, docID, shortName, false))
		require.NoError(t, s.addToDimensionBucket(dropDimsDim, docID, longName, false))
	}

	got, err := s.Dimensions(ctx, longName)
	require.NoError(t, err)
	require.Equal(t, 3*dropDimsDim, got, "precondition: %q must be tracked", longName)

	require.NoError(t, s.removeAllDimensionsLSM(ctx, shortName))

	got, err = s.Dimensions(ctx, shortName)
	require.NoError(t, err)
	require.Equal(t, 0, got, "%q's own rows must be gone", shortName)

	got, err = s.Dimensions(ctx, longName)
	require.NoError(t, err)
	require.Equal(t, 3*dropDimsDim, got,
		"dropping %q took %q's rows: every key of the longer name carries the "+
			"shorter one as a byte prefix, so the scan must also match on key length",
		shortName, longName)
}

// TestDropVectorIndex_DimensionsClearOnShutDownStoreIsNotSilent pins that a
// clear against a torn-down store reports failure. Store.Shutdown blanks
// bucketsByName before draining, so the bucket lookup returns nil exactly as it
// does on a shard that never tracked dimensions. Reporting success there loses
// the clear permanently: the finalizer removes the name from the schema and no
// route revisits it, which is the reported incident arriving by another door.
func TestDropVectorIndex_DimensionsClearOnShutDownStoreIsNotSilent(t *testing.T) {
	ctx := t.Context()
	s, _ := setupDropDimsShard(t, ctx)

	for i := 0; i < dropDimsCount; i++ {
		require.NoError(t, s.PutObject(ctx, dropDimsObject(i)))
	}
	require.NoError(t, s.Shutdown(ctx))

	err := s.removeAllDimensionsLSM(ctx, dropDimsDropped)
	require.Error(t, err,
		"a clear against a shut-down store reported success while the rows are "+
			"still on disk; the caller then has nothing to fall back to")
	require.ErrorIs(t, err, errAlreadyShutdown)
}

// TestDropVectorIndex_ShardLoadClearsDimensionRows drives the route a cold
// tenant takes when it is activated after a drop it was too inactive to take
// part in. NewShard calls this once its store is open; the rows are cleared
// through the shard's own dimensions bucket rather than a second open of it
// from disk.
func TestDropVectorIndex_ShardLoadClearsDimensionRows(t *testing.T) {
	ctx := t.Context()
	s, class := setupDropDimsShard(t, ctx)

	for i := 0; i < dropDimsCount; i++ {
		require.NoError(t, s.PutObject(ctx, dropDimsObject(i)))
	}
	wantAll := dropDimsCount * dropDimsDim

	// The marker the sweep keys off: the drop rewrites the entry's index type
	// to "none" and keeps the name until the finalizer removes it.
	class.VectorConfig[dropDimsDropped] = models.VectorConfig{
		VectorIndexType: modelsext.VectorIndexTypeNone,
	}

	s.clearDroppedVectorDimensions(ctx, class)

	dropped, err := s.Dimensions(ctx, dropDimsDropped)
	require.NoError(t, err)
	require.Equal(t, 0, dropped,
		"the load-time sweep left %q's rows behind, so an activated cold tenant "+
			"keeps them: no other route revisits that shard once the drop task is done",
		dropDimsDropped)

	keep, err := s.Dimensions(ctx, dropDimsKeep)
	require.NoError(t, err)
	require.Equal(t, wantAll, keep,
		"the sweep took the surviving sibling %q's rows too", dropDimsKeep)
}

// TestDropVectorIndex_DimensionsClearSurvivesProcessCrash pins that a clear is
// on disk when it returns. The unit is recorded complete straight after, and the
// finalizer can then remove the marker, so no route ever retries a clear that a
// crash undid: the rows would come back, and a re-created vector of the same
// name would inherit them.
func TestDropVectorIndex_DimensionsClearSurvivesProcessCrash(t *testing.T) {
	ctx := t.Context()
	s, _ := setupDropDimsShard(t, ctx)

	const rows = 1000
	for docID := uint64(0); docID < rows; docID++ {
		require.NoError(t, s.addToDimensionBucket(dropDimsDim, docID, dropDimsDropped, false))
	}
	require.NoError(t, s.store.WriteWALs())
	require.Equal(t, rows*dropDimsDim, dimensionsAfterCrash(t, ctx, s, dropDimsDropped),
		"precondition: the seeded rows must survive a crash, or the assertion below proves nothing")

	require.NoError(t, removeDimensionsForDroppedVector(ctx, s.index, s.name, dropDimsDropped))

	require.Zero(t, dimensionsAfterCrash(t, ctx, s, dropDimsDropped),
		"the clear's deletes were still in a write buffer when it returned; a crash now "+
			"brings every row back")
}

// dimensionsAfterCrash reads what a restart after a kill at this moment would:
// the dimensions bucket's directory as it stands on disk, recovered from its
// segments and WAL, with nothing still buffered in memory.
func dimensionsAfterCrash(t *testing.T, ctx context.Context, s *Shard, targetVector string) int {
	t.Helper()
	crashed := t.TempDir()
	copyDirTree(t, filepath.Join(s.path(), "lsm", helpers.DimensionsBucketLSM),
		filepath.Join(crashed, s.name, "lsm", helpers.DimensionsBucketLSM))
	got, err := shardusage.CalculateUnloadedDimensionsUsage(ctx, logrus.New(), crashed, s.name, targetVector)
	require.NoError(t, err)
	return got.Count * got.Dimensions
}

// TestDropVectorIndex_DimensionsClearStopsWhenShardIsDropped pins that a clear
// gives way to a drop of its shard. Shard.drop cancels shutCtx once its bounded
// reference drain gives up, and Store.Shutdown then waits on the clear's bucket
// pin, so a clear that ignored the cancellation would hold the tenant or
// collection delete, a RAFT apply, until it finished.
func TestDropVectorIndex_DimensionsClearStopsWhenShardIsDropped(t *testing.T) {
	ctx := t.Context()
	s, _ := setupDropDimsShard(t, ctx)

	const rows = 100
	for docID := uint64(0); docID < rows; docID++ {
		require.NoError(t, s.addToDimensionBucket(dropDimsDim, docID, dropDimsDropped, false))
	}

	s.shutCtxCancel(errors.New("shard dropped"))

	require.ErrorContains(t, s.removeAllDimensionsLSM(ctx, dropDimsDropped), "shard dropped",
		"the clear has to stop, and say why")

	scan, err := shardusage.ScanTargetVectorDimensions(ctx,
		s.store.Bucket(helpers.DimensionsBucketLSM), dropDimsDropped, 0)
	require.NoError(t, err)
	require.Equal(t, rows, scan.Raw.Count,
		"the clear kept deleting after its shard's drop cancelled it")
}

// TestDropVectorIndex_DimensionsClearFailsWhenShardIsNotLoaded pins that the
// unit's clear only ever goes through a loaded shard. Opened from disk instead,
// the bucket's registry claim would be held for the whole clear under no shard
// lock, and an activation, backup or delete of that tenant would collide with
// it. Failing is safe: an uncredited unit is re-covered once the tenant loads,
// and the load clears the rows itself.
func TestDropVectorIndex_DimensionsClearFailsWhenShardIsNotLoaded(t *testing.T) {
	ctx := t.Context()
	s, class := setupDropDimsShard(t, ctx)
	idx := s.index

	require.ErrorIs(t, removeDimensionsForDroppedVector(ctx, idx, "absent", dropDimsDropped),
		errDimensionsShardNotLoaded, "a shard this node does not hold")

	const cold = "cold-tenant"
	lazy := NewLazyLoadShard(ctx, nil, cold, idx, class, idx.centralJobQueue,
		idx.allocChecker, idx.shardLoadLimiter, idx.shardReindexer,
		false, idx.bitmapBufPool)
	idx.shards.Store(cold, lazy)
	t.Cleanup(func() { idx.shards.LoadAndDelete(cold) })

	require.ErrorIs(t, removeDimensionsForDroppedVector(ctx, idx, cold, dropDimsDropped),
		errDimensionsShardNotLoaded, "a lazy shard that is not loaded")
	require.False(t, lazy.isLoaded(), "the clear loaded a tenant; loading is the tenant's own business")
}

// TestDropVectorIndex_StaleWriteDoesNotRecreateDimensionRows pins the write
// that read the class before the drop. Its dropped-vector check passed against
// that stale copy, so it reaches dimension tracking after the index is gone
// and the rows are cleared. Recording them there would hand them to a
// re-created vector of the same name.
func TestDropVectorIndex_StaleWriteDoesNotRecreateDimensionRows(t *testing.T) {
	ctx := t.Context()
	s, _ := setupDropDimsShard(t, ctx)

	for i := 0; i < dropDimsCount; i++ {
		require.NoError(t, s.PutObject(ctx, dropDimsObject(i)))
	}
	require.NoError(t, s.DropVectorIndex(ctx, dropDimsDropped))
	require.NoError(t, removeDimensionsForDroppedVector(ctx, s.index, s.name, dropDimsDropped))

	// The class this shard reads still lists the dropped vector as live, which
	// is exactly what a write that read it before the marker applied sees.
	require.Error(t, s.PutObject(ctx, dropDimsObject(dropDimsCount)),
		"precondition: the stale write gets past the class check and fails on the missing index")

	keep, err := s.Dimensions(ctx, dropDimsKeep)
	require.NoError(t, err)
	require.Equal(t, (dropDimsCount+1)*dropDimsDim, keep,
		"precondition: the stale write has to reach dimension tracking, or the assertion below proves nothing")

	dropped, err := s.Dimensions(ctx, dropDimsDropped)
	require.NoError(t, err)
	require.Zero(t, dropped,
		"a write that read the class before the drop re-created the dropped vector's rows after the clear")
}
