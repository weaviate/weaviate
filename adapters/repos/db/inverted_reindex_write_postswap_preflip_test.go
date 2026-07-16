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
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Regression tests for weaviate/0-weaviate-issues#319: the post-swap
// pre-flip write-loss window of the enable-filterable / enable-searchable
// migrations.
//
// Window mechanism: the per-shard runtimeSwap flips the canonical bucket
// pointer to the backfilled bucket and tears down the double-write
// callbacks (defer t.disableCallbacks()), while the cluster-wide schema
// flip (OnTaskCompleted → flipSemanticMigrationSchema) commits on a LATER
// scheduler tick. A write arriving in between is analyzed under the OLD
// schema: the migrating property has no enabled index, analyzeProps drops
// it (HasAnyInvertedIndex, inverted/objects.go), and nothing mirrors it —
// fresh inserts are silently missing from the new index after the flip,
// updates leave a stale ghost, deletes leave dangling postings.
//
// The tests below drive a shard into exactly that state with the
// production Run{ReindexOnly,Prepare,Swap}OnShard trio — after
// RunSwapOnShard returns, the bucket is swapped and the callbacks are
// gone, but the live (test-fixture) schema still has the index flag
// false, which IS the window. No scheduler manipulation or test seams
// needed: the phases are the deterministic ordering handle.
//
// The fix under test: maybeWirePerPropOverlaySet arms the per-shard
// force-index overlay atomically with each bucket-pointer flip, and
// Shard.AnalyzeObject applies it, so in-window writes are analyzed under
// the TARGET schema and land in the canonical (swapped) bucket via the
// ordinary inline write path.

// newNoIndexTestClass builds a class whose text properties have NO enabled
// inverted index at all (IndexFilterable=false AND IndexSearchable=false).
// This is the worst-case enable-* input: pre-fix, the analyzer drops such a
// property entirely, so even the double-write callbacks (pre-swap) and the
// inline path (post-swap) never see it. IndexNullState/IndexPropertyLength
// stay enabled, matching newTestClassWithProps, so the tests also cover the
// null/length bucket creation the migration must perform for a
// previously-unindexed property.
func newNoIndexTestClass(className string, propNames []string) *models.Class {
	vFalse := false
	props := make([]*models.Property, len(propNames))
	for i, name := range propNames {
		props[i] = &models.Property{
			Name:            name,
			DataType:        schema.DataTypeText.PropString(),
			Tokenization:    models.PropertyTokenizationWord,
			IndexFilterable: &vFalse,
			IndexSearchable: &vFalse,
		}
	}
	return &models.Class{
		Class:             className,
		VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60,
			Stopwords:              &models.StopwordConfig{Preset: "none"},
			IndexNullState:         true,
			IndexPropertyLength:    true,
			UsingBlockMaxWAND:      false,
		},
		Properties: props,
	}
}

func objWithTitle(className, id, title string) *storobj.Object {
	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:    strfmt.UUID(id),
			Class: className,
			Properties: map[string]interface{}{
				"title": title,
			},
		},
	}
}

// driveEnableFilterableToPostSwapWindow runs the production migration trio
// and then simulates the provider's overlay wiring (the onPropSwapped hook
// maybeWirePerPropOverlaySet installs before RunSwapOnShard). On return the
// shard is inside the post-swap pre-flip window: canonical bucket = NEW,
// live schema flag still false, double-write callbacks disabled.
func driveEnableFilterableToPostSwapWindow(
	t *testing.T, shard *Shard, idx *Index, className, propName string,
) *ShardReindexTaskGeneric {
	t.Helper()
	ctx := testCtx()

	task, _ := newEnableFilterableTask(t, idx, className, propName)
	payload := &ReindexTaskPayload{
		MigrationType: ReindexTypeEnableFilterable,
		Collection:    className,
		Properties:    []string{propName},
	}
	require.True(t, maybeWirePerPropOverlaySet(shard, payload, []*ShardReindexTaskGeneric{task}),
		"enable-filterable must wire the per-prop overlay hook")

	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))
	return task
}

func driveEnableSearchableToPostSwapWindow(
	t *testing.T, shard *Shard, idx *Index, className, propName, tokenization string,
) *ShardReindexTaskGeneric {
	t.Helper()
	ctx := testCtx()

	task, _ := newEnableSearchableTask(t, idx, className, propName, tokenization)
	payload := &ReindexTaskPayload{
		MigrationType:      ReindexTypeEnableSearchable,
		Collection:         className,
		Properties:         []string{propName},
		TargetTokenization: tokenization,
	}
	require.True(t, maybeWirePerPropOverlaySet(shard, payload, []*ShardReindexTaskGeneric{task}),
		"enable-searchable must wire the per-prop overlay hook")

	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))
	return task
}

func TestReindexPostSwapPreFlip_EnableFilterable_InsertNotLost(t *testing.T) {
	const propName = "title"
	ctx := testCtx()
	className := "PostSwapPreFlipEfInsert_" + uuid.NewString()[:8]
	class := newNoIndexTestClass(className, []string{propName})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	require.NoError(t, shard.PutObject(ctx, objWithTitle(className, uuid.NewString(), "alpha")))
	require.NoError(t, shard.PutObject(ctx, objWithTitle(className, uuid.NewString(), "bravo")))

	driveEnableFilterableToPostSwapWindow(t, shard, idx, className, propName)

	// In-window write: live schema still says IndexFilterable=false.
	require.NoError(t, shard.PutObject(ctx, objWithTitle(className, uuid.NewString(), "zulu")),
		"in-window insert must not error")

	fp := fingerprintRoaringSetBucket(t, shard.store.Bucket(helpers.BucketFromPropNameLSM(propName)))
	require.NotEmptyf(t, fp["zulu"],
		"weaviate/0-weaviate-issues#319: fresh insert during the post-swap pre-flip window "+
			"must be present in the swapped canonical filterable bucket; got fingerprint %v", fp)
	// The backfilled rows must still be there too.
	require.NotEmpty(t, fp["alpha"], "backfilled token must survive the in-window write")
	require.NotEmpty(t, fp["bravo"], "backfilled token must survive the in-window write")
}

func TestReindexPostSwapPreFlip_EnableFilterable_UpdateLeavesNoGhost(t *testing.T) {
	const propName = "title"
	ctx := testCtx()
	className := "PostSwapPreFlipEfUpdate_" + uuid.NewString()[:8]
	class := newNoIndexTestClass(className, []string{propName})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	targetID := uuid.NewString()
	require.NoError(t, shard.PutObject(ctx, objWithTitle(className, targetID, "ghosttoken")))
	require.NoError(t, shard.PutObject(ctx, objWithTitle(className, uuid.NewString(), "bystander")))

	driveEnableFilterableToPostSwapWindow(t, shard, idx, className, propName)

	// Sanity: the backfill indexed the pre-update value.
	bucket := shard.store.Bucket(helpers.BucketFromPropNameLSM(propName))
	require.NotEmpty(t, fingerprintRoaringSetBucket(t, bucket)["ghosttoken"],
		"backfill must have indexed the pre-update value")

	// In-window update of the same object to a new value.
	require.NoError(t, shard.PutObject(ctx, objWithTitle(className, targetID, "freshtoken")),
		"in-window update must not error")

	fp := fingerprintRoaringSetBucket(t, bucket)
	require.NotEmptyf(t, fp["freshtoken"],
		"weaviate/0-weaviate-issues#319: the updated value written during the post-swap "+
			"pre-flip window must be findable in the swapped canonical bucket; got %v", fp)
	require.Emptyf(t, fp["ghosttoken"],
		"weaviate/0-weaviate-issues#319 (ghost check): the object's PRE-update value must be "+
			"removed from the swapped canonical bucket by the in-window update's delete leg; got %v", fp)
	require.NotEmpty(t, fp["bystander"], "unrelated backfilled token must survive")
}

func TestReindexPostSwapPreFlip_EnableFilterable_DeleteRemovesPostings(t *testing.T) {
	const propName = "title"
	ctx := testCtx()
	className := "PostSwapPreFlipEfDelete_" + uuid.NewString()[:8]
	class := newNoIndexTestClass(className, []string{propName})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	targetID := uuid.NewString()
	require.NoError(t, shard.PutObject(ctx, objWithTitle(className, targetID, "deltoken")))
	require.NoError(t, shard.PutObject(ctx, objWithTitle(className, uuid.NewString(), "bystander")))

	driveEnableFilterableToPostSwapWindow(t, shard, idx, className, propName)

	bucket := shard.store.Bucket(helpers.BucketFromPropNameLSM(propName))
	require.NotEmpty(t, fingerprintRoaringSetBucket(t, bucket)["deltoken"],
		"backfill must have indexed the to-be-deleted object")

	// In-window delete.
	require.NoError(t, shard.DeleteObject(ctx, strfmt.UUID(targetID), time.Now()),
		"in-window delete must not error")

	fp := fingerprintRoaringSetBucket(t, bucket)
	require.Emptyf(t, fp["deltoken"],
		"weaviate/0-weaviate-issues#319 (delete journey): postings of an object deleted during "+
			"the post-swap pre-flip window must be removed from the swapped canonical bucket; got %v", fp)
	require.NotEmpty(t, fp["bystander"], "unrelated backfilled token must survive")
}

func TestReindexPostSwapPreFlip_EnableSearchable_InsertNotLost(t *testing.T) {
	const propName = "title"
	ctx := testCtx()
	className := "PostSwapPreFlipEsInsert_" + uuid.NewString()[:8]
	class := newNoIndexTestClass(className, []string{propName})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	require.NoError(t, shard.PutObject(ctx, objWithTitle(className, uuid.NewString(), "alpha")))

	driveEnableSearchableToPostSwapWindow(t, shard, idx, className, propName,
		models.PropertyTokenizationWord)

	require.NoError(t, shard.PutObject(ctx, objWithTitle(className, uuid.NewString(), "zulu")),
		"in-window insert must not error")

	fp := fingerprintInvertedBucket(t, shard.store.Bucket(helpers.BucketSearchableFromPropNameLSM(propName)))
	require.NotEmptyf(t, fp["zulu"],
		"weaviate/0-weaviate-issues#319: fresh insert during the post-swap pre-flip window "+
			"must be present in the swapped canonical searchable bucket; got %v", fp)
	require.NotEmpty(t, fp["alpha"], "backfilled token must survive the in-window write")
}

func TestReindexPostSwapPreFlip_EnableSearchable_UpdateLeavesNoGhost(t *testing.T) {
	const propName = "title"
	ctx := testCtx()
	className := "PostSwapPreFlipEsUpdate_" + uuid.NewString()[:8]
	class := newNoIndexTestClass(className, []string{propName})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	targetID := uuid.NewString()
	require.NoError(t, shard.PutObject(ctx, objWithTitle(className, targetID, "ghosttoken")))

	driveEnableSearchableToPostSwapWindow(t, shard, idx, className, propName,
		models.PropertyTokenizationWord)

	bucket := shard.store.Bucket(helpers.BucketSearchableFromPropNameLSM(propName))
	require.NotEmpty(t, fingerprintInvertedBucket(t, bucket)["ghosttoken"],
		"backfill must have indexed the pre-update value")

	require.NoError(t, shard.PutObject(ctx, objWithTitle(className, targetID, "freshtoken")),
		"in-window update must not error")

	fp := fingerprintInvertedBucket(t, bucket)
	require.NotEmptyf(t, fp["freshtoken"],
		"weaviate/0-weaviate-issues#319: the updated value written during the post-swap "+
			"pre-flip window must be findable in the swapped canonical searchable bucket; got %v", fp)
	require.Emptyf(t, fp["ghosttoken"],
		"weaviate/0-weaviate-issues#319 (ghost check): the object's PRE-update value must be "+
			"removed from the swapped canonical searchable bucket; got %v", fp)
}

func TestReindexPostSwapPreFlip_EnableSearchable_DeleteRemovesPostings(t *testing.T) {
	const propName = "title"
	ctx := testCtx()
	className := "PostSwapPreFlipEsDelete_" + uuid.NewString()[:8]
	class := newNoIndexTestClass(className, []string{propName})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	targetID := uuid.NewString()
	require.NoError(t, shard.PutObject(ctx, objWithTitle(className, targetID, "deltoken")))

	driveEnableSearchableToPostSwapWindow(t, shard, idx, className, propName,
		models.PropertyTokenizationWord)

	bucket := shard.store.Bucket(helpers.BucketSearchableFromPropNameLSM(propName))
	require.NotEmpty(t, fingerprintInvertedBucket(t, bucket)["deltoken"],
		"backfill must have indexed the to-be-deleted object")

	require.NoError(t, shard.DeleteObject(ctx, strfmt.UUID(targetID), time.Now()),
		"in-window delete must not error")

	fp := fingerprintInvertedBucket(t, bucket)
	require.Emptyf(t, fp["deltoken"],
		"weaviate/0-weaviate-issues#319 (delete journey): postings of an object deleted during "+
			"the post-swap pre-flip window must be removed from the swapped canonical searchable bucket; got %v", fp)
}

// TestReindexPostSwapPreFlip_EnableFilterable_MultiProp covers the
// multi-property variant: both migrating props must receive the in-window
// write, and a prop NOT covered by the migration must remain untouched.
func TestReindexPostSwapPreFlip_EnableFilterable_MultiProp(t *testing.T) {
	ctx := testCtx()
	className := "PostSwapPreFlipEfMulti_" + uuid.NewString()[:8]
	propNames := []string{"title", "subtitle"}
	class := newNoIndexTestClass(className, propNames)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	putMulti := func(id, title, subtitle string) {
		obj := &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:    strfmt.UUID(id),
				Class: className,
				Properties: map[string]interface{}{
					"title":    title,
					"subtitle": subtitle,
				},
			},
		}
		require.NoError(t, shard.PutObject(ctx, obj))
	}
	putMulti(uuid.NewString(), "alpha", "one")

	// Single task migrating BOTH props (matches production: one
	// enable-filterable submit with a multi-prop selection).
	wrapped := &testEnableFilterableStrategyWrapper{
		EnableFilterableStrategy: EnableFilterableStrategy{
			propNames:  propNames,
			generation: 1,
		},
	}
	selected := map[string]struct{}{}
	for _, p := range propNames {
		selected[p] = struct{}{}
	}
	task := NewShardReindexTaskGeneric(
		"EnableFilterable", idx.logger, wrapped,
		reindexTaskConfig{
			swapBuckets:                   true,
			tidyBuckets:                   true,
			concurrency:                   2,
			memtableOptFactor:             4,
			backupMemtableOptFactor:       1,
			processingDuration:            10 * time.Minute,
			pauseDuration:                 time.Second,
			checkProcessingEveryNoObjects: 1000,
			selectionEnabled:              true,
			selectedPropsByCollection: map[string]map[string]struct{}{
				className: selected,
			},
			selectedShardsByCollection: map[string]map[string]struct{}{
				className: nil,
			},
		},
		&UuidKeyParser{}, uuidObjectsIteratorAsync,
	)
	payload := &ReindexTaskPayload{
		MigrationType: ReindexTypeEnableFilterable,
		Collection:    className,
		Properties:    propNames,
	}
	require.True(t, maybeWirePerPropOverlaySet(shard, payload, []*ShardReindexTaskGeneric{task}))

	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))

	putMulti(uuid.NewString(), "zulu", "two")

	for prop, want := range map[string]string{"title": "zulu", "subtitle": "two"} {
		fp := fingerprintRoaringSetBucket(t, shard.store.Bucket(helpers.BucketFromPropNameLSM(prop)))
		require.NotEmptyf(t, fp[want],
			"weaviate/0-weaviate-issues#319 (multi-prop): in-window insert must reach the swapped "+
				"canonical bucket of migrating prop %q; got %v", prop, fp)
	}
}

// TestReindexPostSwapPreFlip_FlipVisible_OverlayObsolete pins the overlay's
// two teardown guarantees once the schema flip is visible locally:
//
//  1. Backstop: even WITHOUT the explicit OnTaskCompleted clear, a live
//     schema that already satisfies the entry makes the snapshot skip (and
//     self-clear) it — post-flip writes analyze identically with or
//     without the leftover entry.
//  2. Explicit clear: ClearForceIndexOverlay drops the entry, and writes
//     keep landing correctly via the live schema flag.
func TestReindexPostSwapPreFlip_FlipVisible_OverlayObsolete(t *testing.T) {
	const propName = "title"
	ctx := testCtx()
	className := "PostSwapPreFlipEfFlip_" + uuid.NewString()[:8]
	class := newNoIndexTestClass(className, []string{propName})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	require.NoError(t, shard.PutObject(ctx, objWithTitle(className, uuid.NewString(), "alpha")))
	driveEnableFilterableToPostSwapWindow(t, shard, idx, className, propName)

	// Simulate the cluster-wide flip having applied locally: the test
	// schema getter serves this exact class value, so mutating the prop
	// is what a local FSM apply would make the analyzer see.
	vTrue := true
	class.Properties[0].IndexFilterable = &vTrue

	// The snapshot must now treat the (uncleared) entry as satisfied.
	require.Nil(t, shard.SnapshotForceIndexOverlay(class.Properties),
		"a live schema that satisfies the overlay entry must disable it (backstop)")

	require.NoError(t, shard.PutObject(ctx, objWithTitle(className, uuid.NewString(), "yankee")))
	fp := fingerprintRoaringSetBucket(t, shard.store.Bucket(helpers.BucketFromPropNameLSM(propName)))
	require.NotEmpty(t, fp["yankee"], "post-flip write must be indexed via the live schema flag")

	// Explicit clear (what OnTaskCompleted does) stays a no-op-safe path.
	shard.ClearForceIndexOverlay(propName)
	require.NoError(t, shard.PutObject(ctx, objWithTitle(className, uuid.NewString(), "xray")))
	fp = fingerprintRoaringSetBucket(t, shard.store.Bucket(helpers.BucketFromPropNameLSM(propName)))
	require.NotEmpty(t, fp["xray"], "write after explicit clear must be indexed via the live schema flag")
}

// TestMaybeWirePerPropOverlaySet_EnableMigrations pins the provider wiring:
// the onPropSwapped hook must arm the force-index overlay with exactly the
// flags (and tokenization) the migration's backfill overlay used.
func TestMaybeWirePerPropOverlaySet_EnableMigrations(t *testing.T) {
	tests := []struct {
		name    string
		payload *ReindexTaskPayload
		want    inverted.PropertyOverlay
	}{
		{
			name: "enable-filterable",
			payload: &ReindexTaskPayload{
				MigrationType: ReindexTypeEnableFilterable,
				Properties:    []string{"title"},
			},
			want: inverted.PropertyOverlay{ForceFilterable: true},
		},
		{
			name: "enable-searchable carries target tokenization",
			payload: &ReindexTaskPayload{
				MigrationType:      ReindexTypeEnableSearchable,
				Properties:         []string{"title"},
				TargetTokenization: models.PropertyTokenizationField,
			},
			want: inverted.PropertyOverlay{
				ForceSearchable: true,
				Tokenization:    models.PropertyTokenizationField,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := &Shard{}
			task := &ShardReindexTaskGeneric{}
			require.True(t, maybeWirePerPropOverlaySet(s, tc.payload, []*ShardReindexTaskGeneric{task}))
			require.NotNil(t, task.onPropSwapped)

			task.onPropSwapped("title")

			vFalse := false
			props := []*models.Property{{
				Name:            "title",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWord,
				IndexFilterable: &vFalse,
				IndexSearchable: &vFalse,
			}}
			snap := s.SnapshotForceIndexOverlay(props)
			require.Equal(t, map[string]inverted.PropertyOverlay{"title": tc.want}, snap)
		})
	}
}

// TestSnapshotForceIndexOverlay_LiveSchemaSatisfaction is the table-driven
// unit test for the snapshot's satisfied-entry skip + self-clear backstop.
func TestSnapshotForceIndexOverlay_LiveSchemaSatisfaction(t *testing.T) {
	vTrue, vFalse := true, false
	textProp := func(filterable, searchable *bool, tokenization string) *models.Property {
		return &models.Property{
			Name:            "title",
			DataType:        schema.DataTypeText.PropString(),
			Tokenization:    tokenization,
			IndexFilterable: filterable,
			IndexSearchable: searchable,
		}
	}
	tests := []struct {
		name      string
		overlay   inverted.PropertyOverlay
		prop      *models.Property
		wantAlive bool
	}{
		{
			name:      "filterable pending: flag still false",
			overlay:   inverted.PropertyOverlay{ForceFilterable: true},
			prop:      textProp(&vFalse, &vFalse, models.PropertyTokenizationWord),
			wantAlive: true,
		},
		{
			name:      "filterable satisfied: flag true",
			overlay:   inverted.PropertyOverlay{ForceFilterable: true},
			prop:      textProp(&vTrue, &vFalse, models.PropertyTokenizationWord),
			wantAlive: false,
		},
		{
			name:      "searchable pending: flag still false",
			overlay:   inverted.PropertyOverlay{ForceSearchable: true, Tokenization: models.PropertyTokenizationWord},
			prop:      textProp(&vFalse, &vFalse, models.PropertyTokenizationWord),
			wantAlive: true,
		},
		{
			name:      "searchable pending: flag true but tokenization not yet flipped",
			overlay:   inverted.PropertyOverlay{ForceSearchable: true, Tokenization: models.PropertyTokenizationField},
			prop:      textProp(&vFalse, &vTrue, models.PropertyTokenizationWord),
			wantAlive: true,
		},
		{
			name:      "searchable satisfied: flag true and tokenization matches",
			overlay:   inverted.PropertyOverlay{ForceSearchable: true, Tokenization: models.PropertyTokenizationField},
			prop:      textProp(&vFalse, &vTrue, models.PropertyTokenizationField),
			wantAlive: false,
		},
		{
			name:      "rangeable pending on text prop (never satisfiable) stays alive",
			overlay:   inverted.PropertyOverlay{ForceRangeable: true},
			prop:      textProp(&vTrue, &vTrue, models.PropertyTokenizationWord),
			wantAlive: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := &Shard{}
			s.SetForceIndexOverlay(tc.prop.Name, tc.overlay)

			snap := s.SnapshotForceIndexOverlay([]*models.Property{tc.prop})
			if tc.wantAlive {
				require.Equal(t, map[string]inverted.PropertyOverlay{tc.prop.Name: tc.overlay}, snap)
				return
			}
			require.Nil(t, snap, "satisfied entry must be skipped")
			// Self-clear backstop: the satisfied entry must be gone even
			// without an explicit ClearForceIndexOverlay.
			s.forceIndexOverlayMu.RLock()
			_, stillThere := s.forceIndexOverlay[tc.prop.Name]
			s.forceIndexOverlayMu.RUnlock()
			require.False(t, stillThere, "satisfied entry must self-clear")
		})
	}
}
