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
	"encoding/json"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	entschema "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// overlayFixture is a shard holding one property, plus a provider wired to a
// migration over that same property.
type overlayFixture struct {
	ctx       context.Context
	className string
	prop      string
	class     *models.Class
	idx       *Index
	shard     *Shard
	provider  *ReindexProvider
	payload   []byte
}

// newOverlayFixture opens a shard for a class whose single property is shaped
// by shape, and a provider whose payload runs migration over that property.
func newOverlayFixture(t *testing.T, prefix, prop string,
	migration ReindexMigrationType, targetTokenization string, shape func(*models.Property),
) *overlayFixture {
	t.Helper()

	ctx := testCtx()
	className := prefix + "_" + uuid.NewString()[:8]

	class := newTestClassWithProps(className, []string{prop})
	shape(class.Properties[0])

	hot, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	t.Cleanup(func() { hot.Shutdown(context.Background()) })
	shard, err := unwrapShard(ctx, hot)
	require.NoError(t, err)

	payload, err := json.Marshal(ReindexTaskPayload{
		Collection:         className,
		MigrationType:      migration,
		TargetTokenization: targetTokenization,
		Properties:         []string{prop},
		UnitToShard:        map[string]string{"u1": hot.Name()},
	})
	require.NoError(t, err)

	logger, _ := logrustest.NewNullLogger()
	return &overlayFixture{
		ctx:       ctx,
		className: className,
		prop:      prop,
		class:     class,
		idx:       idx,
		shard:     shard,
		payload:   payload,
		provider: NewReindexProvider(
			&DB{indices: map[string]*Index{indexID(entschema.ClassName(className)): idx}},
			nil, nil, logger, "n1", nil, ctx),
	}
}

// task is the completion tick the provider sees for this fixture's migration.
func (f *overlayFixture) task(id string, status distributedtask.TaskStatus) *distributedtask.Task {
	return &distributedtask.Task{
		Namespace:      ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: id, Version: 1},
		Status:         status,
		Payload:        f.payload,
	}
}

// object is a write carrying value for the fixture's property.
func (f *overlayFixture) object(value any) *storobj.Object {
	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:         strfmt.UUID(uuid.NewString()),
			Class:      f.className,
			Properties: map[string]interface{}{f.prop: value},
		},
	}
}

func (f *overlayFixture) overlay() inverted.PropertyOverlay {
	return f.shard.SnapshotPropertyOverlay([]string{f.prop})[f.prop]
}

// applyProperty is the RAFT apply of a schema update for the fixture's
// property, which is where an index type the schema turns off loses its bucket.
func (f *overlayFixture) applyProperty(t *testing.T) {
	t.Helper()
	require.NoError(t, f.idx.updateProperty(f.ctx, f.class.Properties[0]))
}

func overlayTextProp(filterable, searchable bool) func(*models.Property) {
	return func(prop *models.Property) {
		prop.IndexFilterable = boolPtr(filterable)
		prop.IndexSearchable = boolPtr(searchable)
	}
}

func overlayIntProp(rangeable bool) func(*models.Property) {
	return func(prop *models.Property) {
		prop.DataType = entschema.DataTypeInt.PropString()
		prop.Tokenization = ""
		prop.IndexFilterable = boolPtr(true)
		prop.IndexRangeFilters = boolPtr(rangeable)
	}
}

// An entry field says a bucket exists and carries the migrated content. Once
// the index delete takes that bucket away the claim is false, and a write the
// entry still steers into it fails on the missing bucket. A bucket the update
// keeps is the other half: the claim is still true, so the field stays.
func TestPropertyOverlayRetiresWithTheBucketItDescribes(t *testing.T) {
	const prop = "p"

	filterableBucket := helpers.BucketFromPropNameLSM(prop)
	searchableBucket := helpers.BucketSearchableFromPropNameLSM(prop)
	rangeableBucket := helpers.BucketRangeableFromPropNameLSM(prop)

	tests := []struct {
		name        string
		shape       func(*models.Property)
		value       any
		create      map[string]string // bucket name → LSM strategy
		wantAbsent  []string
		wantPresent []string
		overlay     inverted.PropertyOverlay
		wantAfter   inverted.PropertyOverlay
	}{
		{
			name:       "the filterable bucket goes and takes its forced flag",
			shape:      overlayTextProp(false, true),
			value:      "alpha bravo",
			create:     map[string]string{filterableBucket: lsmkv.StrategyRoaringSet},
			wantAbsent: []string{filterableBucket},
			overlay:    inverted.PropertyOverlay{ForceFilterable: true},
		},
		{
			name:       "the searchable bucket goes and takes its tokenization too",
			shape:      overlayTextProp(true, false),
			value:      "alpha bravo",
			create:     map[string]string{searchableBucket: lsmkv.StrategyMapCollection},
			wantAbsent: []string{searchableBucket},
			overlay: inverted.PropertyOverlay{
				ForceSearchable: true,
				Tokenization:    models.PropertyTokenizationField,
			},
		},
		{
			name:       "the rangeable bucket goes and takes its forced flag",
			shape:      overlayIntProp(false),
			value:      42,
			create:     map[string]string{rangeableBucket: lsmkv.StrategyRoaringSetRange},
			wantAbsent: []string{rangeableBucket},
			overlay:    inverted.PropertyOverlay{ForceRangeable: true},
		},
		{
			name:        "a bucket the update keeps holds on to its forced flag",
			shape:       overlayIntProp(false),
			value:       42,
			create:      map[string]string{rangeableBucket: lsmkv.StrategyRoaringSetRange},
			wantAbsent:  []string{rangeableBucket},
			wantPresent: []string{filterableBucket},
			overlay:     inverted.PropertyOverlay{ForceFilterable: true, ForceRangeable: true},
			wantAfter:   inverted.PropertyOverlay{ForceFilterable: true},
		},
		{
			name:        "an update that turns the index on removes no bucket, so nothing retires",
			shape:       overlayTextProp(true, true),
			value:       "alpha bravo",
			wantPresent: []string{filterableBucket, searchableBucket},
			overlay:     inverted.PropertyOverlay{ForceFilterable: true},
			wantAfter:   inverted.PropertyOverlay{ForceFilterable: true},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newOverlayFixture(t, "OverlayRetire", prop, ReindexTypeEnableFilterable, "", tc.shape)

			for name, strategy := range tc.create {
				require.NoError(t, f.shard.store.CreateOrLoadBucket(f.ctx, name,
					lsmkv.WithStrategy(strategy)))
			}
			f.shard.SetPropertyOverlay(prop, tc.overlay)

			f.applyProperty(t)

			for _, name := range tc.wantAbsent {
				require.Nil(t, f.shard.store.Bucket(name),
					"the update must remove %s, or this test proves nothing", name)
			}
			for _, name := range tc.wantPresent {
				require.NotNil(t, f.shard.store.Bucket(name),
					"the update must keep %s, or this test proves nothing", name)
			}
			assert.Equal(t, tc.wantAfter, f.overlay())
			require.NoError(t, f.shard.PutObject(f.ctx, f.object(tc.value)),
				"a write after the update must not be aimed at a removed bucket")
		})
	}
}

// The entry describes this shard's buckets, and no task status says anything
// about those. A completion tick that retired one would strand the writes it
// is placing; one that revived it would aim them at a bucket the schema no
// longer has.
func TestTaskCompletionLeavesThePropertyOverlayAlone(t *testing.T) {
	const prop = "p"

	tests := []struct {
		name      string
		status    distributedtask.TaskStatus
		migration ReindexMigrationType
		// shape is the live schema this node has applied by the time the tick
		// lands, which trails the leader's view of the task by up to an apply.
		shape        func(*models.Property)
		tokenization string
		overlay      inverted.PropertyOverlay
	}{
		{
			name:         "swapping: this node commits the cluster-wide schema flip",
			status:       distributedtask.TaskStatusSwapping,
			migration:    ReindexTypeChangeTokenization,
			shape:        overlayTextProp(true, true),
			tokenization: models.PropertyTokenizationField,
			overlay:      inverted.PropertyOverlay{Tokenization: models.PropertyTokenizationField},
		},
		{
			name:      "finished: this node's schema already carries the flip",
			status:    distributedtask.TaskStatusFinished,
			migration: ReindexTypeEnableFilterable,
			shape:     overlayTextProp(true, true),
			overlay:   inverted.PropertyOverlay{ForceFilterable: true},
		},
		{
			name:      "failed: the shard keeps whatever its swap already flipped",
			status:    distributedtask.TaskStatusFailed,
			migration: ReindexTypeEnableFilterable,
			shape:     overlayTextProp(false, true),
			overlay:   inverted.PropertyOverlay{ForceFilterable: true},
		},
		{
			name:      "cancelled: same, minus the failure guidance",
			status:    distributedtask.TaskStatusCancelled,
			migration: ReindexTypeEnableSearchable,
			shape:     overlayTextProp(true, false),
			overlay: inverted.PropertyOverlay{
				ForceSearchable: true,
				Tokenization:    models.PropertyTokenizationField,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newOverlayFixture(t, "OverlayAfterCompletion", prop, tc.migration,
				tc.tokenization, tc.shape)
			f.shard.SetPropertyOverlay(prop, tc.overlay)

			require.NoError(t, f.provider.OnTaskCompleted(f.task("T", tc.status)))

			assert.Equal(t, tc.overlay, f.overlay())
		})
	}
}

// A tokenization change that failed after a partial swap keeps its entry: it
// is the only thing aligning this shard's queries with the bucket it already
// swapped. Conflict detection admits a second migration on the same property,
// which must fold into that entry rather than replace it.
func TestSecondMigrationUnionsIntoAFailedTokenizationOverlay(t *testing.T) {
	tests := []struct {
		name string
		arm  func(*testing.T, *Shard, string, inverted.PropertyOverlay)
	}{
		{
			name: "the live swap arms through SwapBucketAndSetOverlay",
			arm: func(t *testing.T, shard *Shard, prop string, o inverted.PropertyOverlay) {
				_, err := shard.SwapBucketAndSetOverlay(prop, o,
					func() (*lsmkv.Bucket, error) { return nil, nil })
				require.NoError(t, err)
			},
		},
		{
			name: "the resume path arms through SetPropertyOverlay",
			arm: func(_ *testing.T, shard *Shard, prop string, o inverted.PropertyOverlay) {
				shard.SetPropertyOverlay(prop, o)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			const prop = "title"
			const live = models.PropertyTokenizationWord
			const swapped = models.PropertyTokenizationWhitespace

			f := newOverlayFixture(t, "OverlayMerge", prop, ReindexTypeEnableFilterable, "",
				overlayTextProp(false, true))

			f.shard.SetPropertyOverlay(prop, inverted.PropertyOverlay{Tokenization: swapped})
			require.Equal(t, swapped, f.shard.TokenizationFor(prop, live),
				"the failed tokenization change must own the overlay, or this test proves nothing")

			tc.arm(t, f.shard, prop, inverted.PropertyOverlay{ForceFilterable: true})

			assert.Equal(t, inverted.PropertyOverlay{ForceFilterable: true, Tokenization: swapped},
				f.overlay())
		})
	}
}
