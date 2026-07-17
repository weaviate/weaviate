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

// DropProperty edge on the durable rangeable-incomplete sentinel
// (weaviate/0-weaviate-issues#335): the marker is keyed by property NAME with
// no epoch and is cleared only by the migration's OnMigrationComplete. Dropping
// the rangeable index (IndexRangeFilters=false) must therefore clear it too —
// otherwise a same-name rangeable re-creation, which never runs that migration
// hook, inherits the stale not-ready verdict and stays gated forever.

// dropRangeableProp is the schema-apply payload that turns the rangeable index
// OFF while keeping the filterable sibling (the fallback the gate falls back to).
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

// copyDirForRangeableTest recursively copies a bucket dir. Used to snapshot a
// populated rangeable bucket and restore it as a "re-created same-name,
// natively populated" index after a drop.
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

// TestRangeableRecovery_DropRangeableIndex_ClearsDurableIncompleteSentinel pins
// the crux: the real schema-apply property-drop path must clear the durable
// not-ready sentinel. RED on head (the drop leaves the marker behind), green
// once the drop clears it alongside the sibling .migrations tracker cleanup.
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

	// Drop the rangeable index through the real schema-apply path.
	eg := enterrors.NewErrorGroupWrapper(idx.logger)
	shard.updatePropertyBuckets(ctx, eg, dropRangeableProp(propName))
	require.NoError(t, eg.Wait())

	require.Nil(t, shard.store.Bucket(helpers.BucketRangeableFromPropNameLSM(propName)),
		"precondition: the rangeable bucket was dropped")
	require.False(t, shard.rangeableIncompleteSentinelExists(propName),
		"dropping the rangeable index must clear the durable not-ready sentinel, else a "+
			"same-name rangeable re-creation stays gated not-ready forever")
}

// TestRangeableRecovery_DropThenRecreateSameNamePopulated_NotGatedForever is the
// end-to-end journey: a gated property is dropped, then re-created same-name with
// a populated rangeable bucket, and must be served READY. On head the stale
// pre-drop sentinel survives the drop and gates the re-created (populated) index
// not-ready forever; the fix clears it on drop so the populated bucket is ready.
func TestRangeableRecovery_DropThenRecreateSameNamePopulated_NotGatedForever(t *testing.T) {
	ctx := testCtx()
	className := "RangeDropRecreate_" + uuid.NewString()[:8]
	propName := filterableToRangeablePropName

	idx, _, shardName := driveRangeableMigrationToTidied(t, ctx, className)

	// Promote the deferred ingest dir into the canonical rangeable bucket via a
	// completion restart, then shut down so the canonical dir is consistent to
	// snapshot as the "re-created same-name, natively populated" index.
	shard1 := restartWithCompletedRangeableSchema(t, ctx, idx, shardName, className)
	lsmPath := shard1.pathLSM()
	rangeableDir := filepath.Join(lsmPath, helpers.BucketRangeableFromPropNameLSM(propName))
	require.True(t, rangeableReady(t, shard1), "precondition: populated rangeable bucket is ready")
	require.NoError(t, shard1.Shutdown(ctx))
	require.True(t, dirExists(rangeableDir), "precondition: canonical rangeable dir on disk")
	snapshot := filepath.Join(t.TempDir(), "rangeable-snapshot")
	copyDirForRangeableTest(t, rangeableDir, snapshot)

	// Restart, persist the stale not-ready verdict, then drop the rangeable
	// index via the real schema-apply path.
	shardA := restartWithCompletedRangeableSchema(t, ctx, idx, shardName, className)
	require.NoError(t, shardA.writeRangeableIncompleteSentinel(propName))
	require.True(t, shardA.rangeableIncompleteSentinelExists(propName))

	eg := enterrors.NewErrorGroupWrapper(idx.logger)
	shardA.updatePropertyBuckets(ctx, eg, dropRangeableProp(propName))
	require.NoError(t, eg.Wait())
	require.NoError(t, shardA.Shutdown(ctx))

	// Re-create the same-name rangeable index, natively populated: restore the
	// snapshot into the canonical bucket path the drop just removed.
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
