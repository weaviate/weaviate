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
	"context"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Regression tests for weaviate/0-weaviate-issues#319: writes landing in
// the post-swap pre-flip window of enable-filterable / enable-searchable
// migrations must not be lost (see maybeWirePerPropOverlaySet for the fix).

// newNoIndexTestClass builds a class whose properties have no enabled
// inverted index — the worst-case enable-* input. IndexNullState/
// IndexPropertyLength stay on, so the tests also cover the null/length
// bucket creation for a previously-unindexed property.
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
// and wires the overlay hook, leaving the shard inside the post-swap
// pre-flip window (bucket swapped, live schema flag still false).
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

// postSwapPreFlipTarget bundles the enable-filterable vs enable-searchable
// specific pieces (bucket accessor, fingerprint helper, window-arming drive
// function) so the Insert/Update/Delete scenario runners below don't
// duplicate the migration-flavor dispatch. This is the structural source of
// what were 6 nearly-identical top-level test bodies.
type postSwapPreFlipTarget struct {
	label       string // "filterable" / "searchable" -- used in failure text
	bucket      func(shard *Shard, propName string) *lsmkv.Bucket
	fingerprint func(t *testing.T, b *lsmkv.Bucket) map[string][]uint64
	drive       func(t *testing.T, shard *Shard, idx *Index, className, propName string) *ShardReindexTaskGeneric
}

func filterableTarget() postSwapPreFlipTarget {
	return postSwapPreFlipTarget{
		label: "filterable",
		bucket: func(shard *Shard, propName string) *lsmkv.Bucket {
			return shard.store.Bucket(helpers.BucketFromPropNameLSM(propName))
		},
		fingerprint: fingerprintRoaringSetBucket,
		drive:       driveEnableFilterableToPostSwapWindow,
	}
}

func searchableTarget() postSwapPreFlipTarget {
	return postSwapPreFlipTarget{
		label: "searchable",
		bucket: func(shard *Shard, propName string) *lsmkv.Bucket {
			return shard.store.Bucket(helpers.BucketSearchableFromPropNameLSM(propName))
		},
		fingerprint: fingerprintInvertedBucket,
		drive: func(t *testing.T, shard *Shard, idx *Index, className, propName string) *ShardReindexTaskGeneric {
			return driveEnableSearchableToPostSwapWindow(t, shard, idx, className, propName,
				models.PropertyTokenizationWord)
		},
	}
}

// newPostSwapPreFlipShard builds the no-index test class + shard shared by
// every scenario below and registers its teardown.
func newPostSwapPreFlipShard(
	t *testing.T, ctx context.Context, classPrefix string, propNames []string,
) (*Shard, *Index, string) {
	t.Helper()
	className := classPrefix + "_" + uuid.NewString()[:8]
	class := newNoIndexTestClass(className, propNames)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	t.Cleanup(func() { shard.Shutdown(ctx) })
	return shard, idx, className
}

type insertNotLostCase struct {
	target      postSwapPreFlipTarget
	classPrefix string
	backfill    []string
}

func runPostSwapPreFlipInsertNotLost(t *testing.T, c insertNotLostCase) {
	const propName = "title"
	ctx := testCtx()
	shard, idx, className := newPostSwapPreFlipShard(t, ctx, c.classPrefix, []string{propName})

	for _, v := range c.backfill {
		require.NoError(t, shard.PutObject(ctx, objWithTitle(className, uuid.NewString(), v)))
	}

	c.target.drive(t, shard, idx, className, propName)

	// In-window write: live schema flag still says false.
	require.NoError(t, shard.PutObject(ctx, objWithTitle(className, uuid.NewString(), "zulu")),
		"in-window insert must not error")

	fp := c.target.fingerprint(t, c.target.bucket(shard, propName))
	require.NotEmptyf(t, fp["zulu"],
		"weaviate/0-weaviate-issues#319: fresh insert during the post-swap pre-flip window "+
			"must be present in the swapped canonical %s bucket; got %v", c.target.label, fp)
	for _, v := range c.backfill {
		require.NotEmpty(t, fp[v], "backfilled token must survive the in-window write")
	}
}

func TestReindexPostSwapPreFlip_EnableFilterable_InsertNotLost(t *testing.T) {
	runPostSwapPreFlipInsertNotLost(t, insertNotLostCase{
		target:      filterableTarget(),
		classPrefix: "PostSwapPreFlipEfInsert",
		backfill:    []string{"alpha", "bravo"},
	})
}

func TestReindexPostSwapPreFlip_EnableSearchable_InsertNotLost(t *testing.T) {
	runPostSwapPreFlipInsertNotLost(t, insertNotLostCase{
		target:      searchableTarget(),
		classPrefix: "PostSwapPreFlipEsInsert",
		backfill:    []string{"alpha"},
	})
}

type updateNoGhostCase struct {
	target        postSwapPreFlipTarget
	classPrefix   string
	withBystander bool
}

func runPostSwapPreFlipUpdateLeavesNoGhost(t *testing.T, c updateNoGhostCase) {
	const propName = "title"
	ctx := testCtx()
	shard, idx, className := newPostSwapPreFlipShard(t, ctx, c.classPrefix, []string{propName})

	targetID := uuid.NewString()
	require.NoError(t, shard.PutObject(ctx, objWithTitle(className, targetID, "ghosttoken")))
	if c.withBystander {
		require.NoError(t, shard.PutObject(ctx, objWithTitle(className, uuid.NewString(), "bystander")))
	}

	c.target.drive(t, shard, idx, className, propName)

	bucket := c.target.bucket(shard, propName)
	require.NotEmpty(t, c.target.fingerprint(t, bucket)["ghosttoken"],
		"backfill must have indexed the pre-update value")

	require.NoError(t, shard.PutObject(ctx, objWithTitle(className, targetID, "freshtoken")),
		"in-window update must not error")

	fp := c.target.fingerprint(t, bucket)
	require.NotEmptyf(t, fp["freshtoken"],
		"weaviate/0-weaviate-issues#319: the updated value written during the post-swap "+
			"pre-flip window must be findable in the swapped canonical %s bucket; got %v", c.target.label, fp)
	require.Emptyf(t, fp["ghosttoken"],
		"weaviate/0-weaviate-issues#319 (ghost check): the object's PRE-update value must be "+
			"removed from the swapped canonical %s bucket; got %v", c.target.label, fp)
	if c.withBystander {
		require.NotEmpty(t, fp["bystander"], "unrelated backfilled token must survive")
	}
}

func TestReindexPostSwapPreFlip_EnableFilterable_UpdateLeavesNoGhost(t *testing.T) {
	runPostSwapPreFlipUpdateLeavesNoGhost(t, updateNoGhostCase{
		target:        filterableTarget(),
		classPrefix:   "PostSwapPreFlipEfUpdate",
		withBystander: true,
	})
}

func TestReindexPostSwapPreFlip_EnableSearchable_UpdateLeavesNoGhost(t *testing.T) {
	runPostSwapPreFlipUpdateLeavesNoGhost(t, updateNoGhostCase{
		target:      searchableTarget(),
		classPrefix: "PostSwapPreFlipEsUpdate",
	})
}

type deleteRemovesPostingsCase struct {
	target        postSwapPreFlipTarget
	classPrefix   string
	withBystander bool
}

func runPostSwapPreFlipDeleteRemovesPostings(t *testing.T, c deleteRemovesPostingsCase) {
	const propName = "title"
	ctx := testCtx()
	shard, idx, className := newPostSwapPreFlipShard(t, ctx, c.classPrefix, []string{propName})

	targetID := uuid.NewString()
	require.NoError(t, shard.PutObject(ctx, objWithTitle(className, targetID, "deltoken")))
	if c.withBystander {
		require.NoError(t, shard.PutObject(ctx, objWithTitle(className, uuid.NewString(), "bystander")))
	}

	c.target.drive(t, shard, idx, className, propName)

	bucket := c.target.bucket(shard, propName)
	require.NotEmpty(t, c.target.fingerprint(t, bucket)["deltoken"],
		"backfill must have indexed the to-be-deleted object")

	require.NoError(t, shard.DeleteObject(ctx, strfmt.UUID(targetID), time.Now()),
		"in-window delete must not error")

	fp := c.target.fingerprint(t, bucket)
	require.Emptyf(t, fp["deltoken"],
		"weaviate/0-weaviate-issues#319 (delete journey): postings of an object deleted during "+
			"the post-swap pre-flip window must be removed from the swapped canonical %s bucket; got %v",
		c.target.label, fp)
	if c.withBystander {
		require.NotEmpty(t, fp["bystander"], "unrelated backfilled token must survive")
	}
}

func TestReindexPostSwapPreFlip_EnableFilterable_DeleteRemovesPostings(t *testing.T) {
	runPostSwapPreFlipDeleteRemovesPostings(t, deleteRemovesPostingsCase{
		target:        filterableTarget(),
		classPrefix:   "PostSwapPreFlipEfDelete",
		withBystander: true,
	})
}

func TestReindexPostSwapPreFlip_EnableSearchable_DeleteRemovesPostings(t *testing.T) {
	runPostSwapPreFlipDeleteRemovesPostings(t, deleteRemovesPostingsCase{
		target:      searchableTarget(),
		classPrefix: "PostSwapPreFlipEsDelete",
	})
}

// TestReindexPostSwapPreFlip_EnableFilterable_MultiProp: both migrating
// props must receive the in-window write.
func TestReindexPostSwapPreFlip_EnableFilterable_MultiProp(t *testing.T) {
	ctx := testCtx()
	propNames := []string{"title", "subtitle"}
	shard, idx, className := newPostSwapPreFlipShard(t, ctx, "PostSwapPreFlipEfMulti", propNames)

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

	// Single task migrating both props, matching a production multi-prop submit.
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
// backstop self-clear once the live schema satisfies it, plus the explicit
// ClearForceIndexOverlay path.
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

	// Simulate the flip: mutating the prop here is what a local FSM apply
	// would make the analyzer see.
	vTrue := true
	class.Properties[0].IndexFilterable = &vTrue

	require.Nil(t, shard.SnapshotForceIndexOverlay(class.Properties),
		"a live schema that satisfies the overlay entry must disable it (backstop)")

	require.NoError(t, shard.PutObject(ctx, objWithTitle(className, uuid.NewString(), "yankee")))
	fp := fingerprintRoaringSetBucket(t, shard.store.Bucket(helpers.BucketFromPropNameLSM(propName)))
	require.NotEmpty(t, fp["yankee"], "post-flip write must be indexed via the live schema flag")

	// Mirrors the explicit clear OnTaskCompleted performs.
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
			s.forceIndexOverlayMu.RLock()
			_, stillThere := s.forceIndexOverlay[tc.prop.Name]
			s.forceIndexOverlayMu.RUnlock()
			require.False(t, stillThere, "satisfied entry must self-clear")
		})
	}
}
