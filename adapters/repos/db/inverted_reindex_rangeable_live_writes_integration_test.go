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
	"encoding/binary"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/filters"
	entinverted "github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestEnableRangeable_LiveWritesDuringMigrationWindow is the end-to-end
// (shard + task) regression test for weaviate/weaviate#11688.
//
// Scenario: an enable-rangeable migration on an int property with
// indexFilterable=false (no other enabled inverted index — the exposed
// matrix cell). While the migration window is open (double-write callbacks
// registered, backfill iteration not yet finished), live updates land on a
// subset of the objects. Their LastUpdateTimeUnix is >= reindexStarted, so
// the backfill iterator deliberately skips them on the assumption that the
// double-write callbacks mirror them into the ingest bucket.
//
// Pre-fix, that assumption was false for exactly this property state: the
// live write path analyzed with the RAFT schema only, the analyzer dropped
// the no-index property, and the callbacks never fired — the updated values
// were permanently missing from the post-migration rangeable index even
// though the migration reported FINISHED.
//
// The updates are injected deterministically after OnAfterLsmInit returns
// (callbacks + migration analyzer overlay registered) and before the
// OnAfterLsmInitAsync iteration loop runs — squarely inside the window.
func TestEnableRangeable_LiveWritesDuringMigrationWindow(t *testing.T) {
	const (
		numObjects       = 40
		numWindowUpdates = 10
		mark             = int64(777777)
	)
	propName := filterableToRangeablePropName

	ctx := testCtx()
	className := "EnableRangeableLiveWrites_" + uuid.NewString()[:8]
	vFalse := false
	class := &models.Class{
		Class:             className,
		VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60,
			Stopwords:              &models.StopwordConfig{Preset: "none"},
			// Null/length aux indexes ON: proves the live path doesn't fail
			// writes on the missing aux buckets of the no-index property
			// while the migration overlay is active (ForcedViaOverlay skip).
			IndexNullState:      true,
			IndexPropertyLength: true,
		},
		Properties: []*models.Property{
			{
				Name:     propName,
				DataType: schema.DataTypeInt.PropString(),
				// The exposed matrix cell: no filterable index, rangeable
				// off until the migration's OnMigrationComplete flips it.
				IndexFilterable:   &vFalse,
				IndexRangeFilters: &vFalse,
			},
		},
	}

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	t.Cleanup(func() { shard.Shutdown(context.Background()) })

	objs := make([]*storobj.Object, numObjects)
	for i := 0; i < numObjects; i++ {
		objs[i] = &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:    strfmt.UUID(uuid.NewString()),
				Class: className,
				Properties: map[string]interface{}{
					propName: int64(i % filterableToRangeableNumDistinctValues),
				},
			},
		}
		require.NoError(t, shard.PutObject(ctx, objs[i]))
	}

	// update overwrites object i with a fresh value and a CURRENT
	// LastUpdateTimeUnix — exactly what a live PATCH/PUT does. The
	// timestamp makes the backfill iterator skip the object (>= the
	// reindexStarted captured during OnAfterLsmInit).
	update := func(i int, val int64) {
		obj := &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:    objs[i].ID(),
				Class: className,
				Properties: map[string]interface{}{
					propName: val,
				},
				CreationTimeUnix:   time.Now().UnixMilli(),
				LastUpdateTimeUnix: time.Now().UnixMilli(),
			},
		}
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	task, wrapped := newFilterableToRangeableTask(t, idx, className, propName)

	require.NoError(t, task.OnAfterLsmInit(ctx, shard))

	// Window writes: callbacks (and, post-fix, the migration analyzer
	// overlay) are registered; the iteration has not run yet. Every one of
	// these updates MUST be range-queryable after the migration completes.
	for i := 0; i < numWindowUpdates; i++ {
		update(i, mark+int64(i))
	}

	for {
		rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
		require.NoError(t, err)
		if rerunAt.IsZero() {
			break
		}
	}
	require.True(t, wrapped.migrationCompleted, "migration must complete")

	bucket := shard.store.Bucket(helpers.BucketRangeableFromPropNameLSM(propName))
	require.NotNil(t, bucket, "post-migration rangeable bucket must exist")

	// Range query: every window-updated object must be served at >= mark.
	gteMark := rangeableDocIDs(t, bucket, mark, filters.OperatorGreaterThanEqual)
	require.Lenf(t, gteMark, numWindowUpdates,
		"every live write during the migration window must be range-queryable "+
			"after the migration — got %d of %d (missing writes were dropped by "+
			"the dead double-write callbacks, weaviate/weaviate#11688)",
		len(gteMark), numWindowUpdates)

	// Exact-value check: each updated object individually present.
	for i := 0; i < numWindowUpdates; i++ {
		ids := rangeableDocIDs(t, bucket, mark+int64(i), filters.OperatorEqual)
		require.Lenf(t, ids, 1, "updated value %d must be served by exactly one doc", mark+int64(i))
	}

	// Untouched objects must be served with their original values, and no
	// stale pre-update values may remain for the updated docIDs.
	all := rangeableDocIDs(t, bucket, 0, filters.OperatorGreaterThanEqual)
	require.Lenf(t, all, numObjects,
		"every object must be in the rangeable index exactly once — got %d of %d",
		len(all), numObjects)
}

// rangeableDocIDs reads the docIDs matching `value` under `operator` from a
// RoaringSetRange bucket, using the same key encoding the write path uses.
func rangeableDocIDs(t *testing.T, b *lsmkv.Bucket, value int64, operator filters.Operator) []uint64 {
	t.Helper()
	lex, err := entinverted.LexicographicallySortableInt64(value)
	require.NoError(t, err)
	key := binary.BigEndian.Uint64(lex)
	reader := b.ReaderRoaringSetRange()
	defer reader.Close()
	bm, release, err := reader.Read(context.Background(), key, operator)
	require.NoError(t, err)
	defer func() {
		if release != nil {
			release()
		}
	}()
	if bm == nil {
		return nil
	}
	return bm.ToArray()
}
