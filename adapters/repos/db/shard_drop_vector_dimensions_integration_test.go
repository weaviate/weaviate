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

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

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
	shardLike, _ := testShardWithSettings(t, ctx, class, vic, false, true, false, func(i *Index) {
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
		require.NoError(t, s.Load(ctx))
		return s.shard, class
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

// TestDropVectorIndex_ClearsDimensionRows pins that a drop takes the vector's
// rows out of the shard's dimensions bucket. Nothing else reclaims them: the
// drop strips the vector from the object bytes, so a later update or delete
// finds nothing to tombstone, and the rows are inherited by the next vector
// created under the same name.
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
	require.Equal(t, 0, gotDropped,
		"the dropped vector's dimension rows survived the drop; a vector re-created "+
			"under the name %q would inherit them and be counted before it holds a "+
			"single vector", dropDimsDropped)

	gotKeep, err = s.Dimensions(ctx, dropDimsKeep)
	require.NoError(t, err)
	require.Equal(t, wantAll, gotKeep,
		"the surviving sibling %q lost its dimension rows: the bucket is shared by "+
			"every vector on the shard, so the drop must remove one key range, not "+
			"the bucket", dropDimsKeep)
}

// TestDropVectorIndex_ClearsDimensionRowsOnUnloadedShard covers the cold route:
// a drop against an inactive tenant is deferred, so the live path never runs
// and the files-only sweep has to clear the rows from disk. This is the route
// the reported incident hit.
func TestDropVectorIndex_ClearsDimensionRowsOnUnloadedShard(t *testing.T) {
	ctx := t.Context()
	s, _ := setupDropDimsShard(t, ctx)

	for i := 0; i < dropDimsCount; i++ {
		require.NoError(t, s.PutObject(ctx, dropDimsObject(i)))
	}

	indexPath, shardName := s.index.path(), s.name
	logger := logrus.New()
	wantAll := dropDimsCount * dropDimsDim

	// From here the only route to the bucket is from disk.
	require.NoError(t, s.Shutdown(ctx))

	before, err := shardusage.CalculateUnloadedDimensionsUsage(ctx, logger, indexPath, shardName, dropDimsDropped)
	require.NoError(t, err)
	require.Equal(t, wantAll, before.Count*before.Dimensions,
		"precondition: the unloaded shard must still report %q", dropDimsDropped)

	require.NoError(t, shardusage.RemoveUnloadedTargetVectorDimensions(
		ctx, logger, indexPath, shardName, dropDimsDropped))

	after, err := shardusage.CalculateUnloadedDimensionsUsage(ctx, logger, indexPath, shardName, dropDimsDropped)
	require.NoError(t, err)
	require.Equal(t, 0, after.Count*after.Dimensions,
		"the dropped vector's rows survived the files-only sweep, so a cold tenant "+
			"keeps them until something loads the shard")

	keep, err := shardusage.CalculateUnloadedDimensionsUsage(ctx, logger, indexPath, shardName, dropDimsKeep)
	require.NoError(t, err)
	require.Equal(t, wantAll, keep.Count*keep.Dimensions,
		"the surviving sibling %q lost its rows: the sweep must clear one key "+
			"range, not the shard's whole dimensions bucket", dropDimsKeep)
}

// TestDropVectorIndex_DimensionsPrefixIsNotEnough pins the key-length filter:
// a shorter name is a byte prefix of every key the longer one owns.
func TestDropVectorIndex_DimensionsPrefixIsNotEnough(t *testing.T) {
	ctx := t.Context()
	s, _ := setupDropDimsShard(t, ctx)

	// Written straight to the bucket — these names need rows, not indexes.
	const shortName, longName = "vec", "vec_extra"
	for docID := uint64(0); docID < 3; docID++ {
		require.NoError(t, s.extendDimensionTrackerLSM(dropDimsDim, docID, shortName))
		require.NoError(t, s.extendDimensionTrackerLSM(dropDimsDim, docID, longName))
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

// TestDropVectorIndex_ShardInitSweepClearsDimensionRows drives the shard-init
// sweep, the route a cold tenant takes when it is activated after a drop it was
// too inactive to take part in. The three tests above reach the clear directly;
// this one reaches it the way NewShard does.
func TestDropVectorIndex_ShardInitSweepClearsDimensionRows(t *testing.T) {
	ctx := t.Context()
	s, class := setupDropDimsShard(t, ctx)

	for i := 0; i < dropDimsCount; i++ {
		require.NoError(t, s.PutObject(ctx, dropDimsObject(i)))
	}
	indexPath, shardName := s.index.path(), s.name
	logger := logrus.New()
	wantAll := dropDimsCount * dropDimsDim
	require.NoError(t, s.Shutdown(ctx))

	// The marker the sweep keys off: the drop rewrites the entry's index type
	// to "none" and keeps the name until the finalizer removes it.
	class.VectorConfig[dropDimsDropped] = models.VectorConfig{
		VectorIndexType: modelsext.VectorIndexTypeNone,
	}

	require.NoError(t, newVectorDropIndexHelper().ensureFilesAreRemovedForDroppedVectorIndexes(
		ctx, logger, indexPath, shardName, class))

	dropped, err := shardusage.CalculateUnloadedDimensionsUsage(ctx, logger, indexPath, shardName, dropDimsDropped)
	require.NoError(t, err)
	require.Equal(t, 0, dropped.Count*dropped.Dimensions,
		"the shard-init sweep left %q's rows behind, so an activated cold tenant "+
			"keeps them", dropDimsDropped)

	keep, err := shardusage.CalculateUnloadedDimensionsUsage(ctx, logger, indexPath, shardName, dropDimsKeep)
	require.NoError(t, err)
	require.Equal(t, wantAll, keep.Count*keep.Dimensions,
		"the sweep took the surviving sibling %q's rows too", dropDimsKeep)
}
