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

package db

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// dropRangeableProp disables the rangeable index but keeps the filterable
// fallback enabled.
func dropRangeableProp(propName string) *models.Property {
	no := false
	yes := true
	return &models.Property{
		Name:              propName,
		DataType:          schema.DataTypeInt.PropString(),
		IndexRangeFilters: &no,
		IndexFilterable:   &yes,
	}
}

// copyDirForRangeableTest recursively copies a bucket dir, used to snapshot
// and later restore a populated bucket as a re-created same-name index.
func copyDirForRangeableTest(t *testing.T, src, dst string) {
	t.Helper()
	require.NoError(t, filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o644)
	}))
}

// Regression for weaviate/0-weaviate-issues#335: index drop must clear the
// durable not-ready sentinel.
func TestRangeableRecovery_DropRangeableIndex_ClearsDurableIncompleteSentinel(t *testing.T) {
	ctx := testCtx()
	className := "RangeDropClearsSentinel_" + uuid.NewString()[:8]
	propName := filterableToRangeablePropName

	shard, idx := setupRangeableMigratedShard(t, ctx, className)
	defer shard.Shutdown(ctx)

	// A prior partial-population boot persisted a durable not-ready verdict.
	require.NoError(t, shard.writeRangeableIncompleteSentinel(propName))
	require.True(t, shard.rangeableIncompleteSentinelExists(propName),
		"precondition: durable not-ready verdict present")

	eg := enterrors.NewErrorGroupWrapper(idx.logger)
	shard.updatePropertyBuckets(ctx, eg, dropRangeableProp(propName))
	require.NoError(t, eg.Wait())

	require.Nil(t, shard.store.Bucket(helpers.BucketRangeableFromPropNameLSM(propName)),
		"precondition: the rangeable bucket was dropped")
	require.False(t, shard.rangeableIncompleteSentinelExists(propName),
		"dropping the rangeable index must clear the durable not-ready sentinel, else a "+
			"same-name rangeable re-creation stays gated not-ready forever")
}

// Regression for weaviate/0-weaviate-issues#335: drop then same-name
// re-creation with data must not stay gated not-ready.
func TestRangeableRecovery_DropThenRecreateSameNamePopulated_NotGatedForever(t *testing.T) {
	ctx := testCtx()
	className := "RangeDropRecreate_" + uuid.NewString()[:8]
	propName := filterableToRangeablePropName

	idx, _, shardName := driveRangeableMigrationToTidied(t, ctx, className)

	// Completion restart makes the canonical rangeable dir consistent to snapshot.
	shard1 := restartWithCompletedRangeableSchema(t, ctx, idx, shardName, className)
	lsmPath := shard1.pathLSM()
	rangeableDir := filepath.Join(lsmPath, helpers.BucketRangeableFromPropNameLSM(propName))
	require.True(t, rangeableReady(t, shard1), "precondition: populated rangeable bucket is ready")
	require.NoError(t, shard1.Shutdown(ctx))
	require.True(t, dirExists(rangeableDir), "precondition: canonical rangeable dir on disk")
	snapshot := filepath.Join(t.TempDir(), "rangeable-snapshot")
	copyDirForRangeableTest(t, rangeableDir, snapshot)

	shardA := restartWithCompletedRangeableSchema(t, ctx, idx, shardName, className)
	require.NoError(t, shardA.writeRangeableIncompleteSentinel(propName))
	require.True(t, shardA.rangeableIncompleteSentinelExists(propName))

	eg := enterrors.NewErrorGroupWrapper(idx.logger)
	shardA.updatePropertyBuckets(ctx, eg, dropRangeableProp(propName))
	require.NoError(t, eg.Wait())
	require.NoError(t, shardA.Shutdown(ctx))

	// Simulate a native re-creation by restoring into the path the drop removed.
	require.False(t, dirExists(rangeableDir), "the drop must have removed the rangeable bucket dir")
	copyDirForRangeableTest(t, snapshot, rangeableDir)

	shardB := restartWithCompletedRangeableSchema(t, ctx, idx, shardName, className)
	defer shardB.Shutdown(ctx)
	rb := shardB.store.Bucket(helpers.BucketRangeableFromPropNameLSM(propName))
	require.NotNil(t, rb)
	require.True(t, rb.HasAnyData(), "re-created rangeable bucket must load with its restored data")

	require.True(t, rangeableReady(t, shardB),
		"a same-name rangeable index re-created with data must be served ready; a stale "+
			"pre-drop sentinel must not keep it gated not-ready forever")
}
