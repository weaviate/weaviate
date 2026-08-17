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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestWritesInTheFlipWindowLandWhereALaterWriteWould pins the property this
// window has to satisfy: a write issued after a shard has swapped its
// migrated index into place, but before the cluster agrees to advertise it,
// leaves the index in the state the very same write would have left it in had
// it arrived one moment after the flip.
//
// Stated that way rather than as "the write is present", because presence
// alone is what an add-only fix satisfies. An update has to take the docID
// off the posting of the value the object no longer has, and a delete has to
// take it off everywhere; either omission leaves an object findable by
// something it does not say.
//
// The "after a restart" arm covers the same window across a process boundary,
// where nothing in memory survives and the routing has to come back from the
// record on disk.
func TestWritesInTheFlipWindowLandWhereALaterWriteWould(t *testing.T) {
	for _, mig := range windowWriteMigrations() {
		t.Run(mig.name, func(t *testing.T) {
			for _, op := range windowWriteOperations() {
				t.Run(op.name, func(t *testing.T) {
					for _, restart := range []struct {
						name    string
						restart bool
					}{
						{name: "in_process"},
						{name: "after_restart", restart: true},
					} {
						t.Run(restart.name, func(t *testing.T) {
							inWindow := runWindowWrite(t, mig, op, restart.restart, true)
							afterFlip := runWindowWrite(t, mig, op, restart.restart, false)

							require.NotEmpty(t, afterFlip,
								"the control wrote nothing; the fixture, not the window, is broken")
							assert.Equal(t, afterFlip, inWindow,
								"a %s issued in the flip window left the %q index in a different state "+
									"than the same %s issued after the flip",
								op.name, mig.bucketName, op.name)
						})
					}
				})
			}
		})
	}
}

// runWindowWrite drives one shard to the post-swap state and applies op
// either inside the flip window (duringWindow) or after the flip, returning
// the migrated index the two arms are compared on.
func runWindowWrite(t *testing.T, mig windowWriteMigration, op windowWriteOperation,
	restart, duringWindow bool,
) any {
	t.Helper()
	ctx := testCtx()
	className := "FlipWindowWrite_" + uuid.NewString()[:8]
	class := mig.newClass(className)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	shardName := shard.Name()

	for _, obj := range mig.seed(t, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}
	mig.drive(t, ctx, shard, mig.newTask(t, idx, className))

	if restart {
		require.NoError(t, shard.Shutdown(ctx))
		if !duringWindow {
			// The control's restart happens on the far side of the flip, so
			// the shard comes up on a schema that already advertises the
			// index and needs no record to route writes into it.
			mig.flipSchema(class)
		}
		// No task is re-dispatched: this shard's unit is complete and only
		// the cluster-wide flag is outstanding.
		idx.shardReindexer = NewShardReindexerV3Noop()
		loaded, err := idx.initShard(ctx, shardName, class, nil, true, true)
		require.NoError(t, err)
		idx.shards.Store(shardName, loaded)
		shard = loaded.(*Shard)
	} else if !duringWindow {
		mig.flipSchema(class)
	}
	defer shard.Shutdown(context.Background())

	op.apply(t, ctx, shard, mig, className)

	if duringWindow {
		mig.flipSchema(class)
	}
	return mig.fingerprint(t, shard.store.Bucket(mig.bucketName))
}

// windowWriteMigration is one family of migration that builds an index the
// schema advertises only after every replica has swapped.
type windowWriteMigration struct {
	name       string
	bucketName string
	// targetID is the object the update and delete cells act on; seed puts it
	// on the shard before the migration runs.
	targetID    strfmt.UUID
	newClass    func(className string) *models.Class
	seed        func(t *testing.T, className string) []*storobj.Object
	objectWith  func(className string, id strfmt.UUID) *storobj.Object
	newTask     func(t *testing.T, idx *Index, className string) *ShardReindexTaskGeneric
	drive       func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric)
	fingerprint func(t *testing.T, b *lsmkv.Bucket) any
	flipSchema  func(class *models.Class)
}

// windowWriteOperation is one shape of write. Insert alone is not enough: an
// add-only fix passes it and still leaves an updated object findable by the
// value it no longer holds.
type windowWriteOperation struct {
	name  string
	apply func(t *testing.T, ctx context.Context, shard *Shard, mig windowWriteMigration, className string)
}

func windowWriteOperations() []windowWriteOperation {
	return []windowWriteOperation{
		{
			name: "insert",
			apply: func(t *testing.T, ctx context.Context, shard *Shard, mig windowWriteMigration, className string) {
				require.NoError(t, shard.PutObject(ctx,
					mig.objectWith(className, strfmt.UUID(uuid.NewString()))))
			},
		},
		{
			name: "update",
			apply: func(t *testing.T, ctx context.Context, shard *Shard, mig windowWriteMigration, className string) {
				require.NoError(t, shard.PutObject(ctx, mig.objectWith(className, mig.targetID)))
			},
		},
		{
			name: "delete",
			apply: func(t *testing.T, ctx context.Context, shard *Shard, mig windowWriteMigration, className string) {
				require.NoError(t, shard.DeleteObject(ctx, mig.targetID, time.Now()))
			},
		},
	}
}

func windowWriteMigrations() []windowWriteMigration {
	const (
		seedObjects = 10
		// Distinct from every token makeConvergenceTestObjects uses, so the
		// value an update moves the object to is a posting of its own.
		windowToken = "windowtoken"
	)
	// Fixed rather than random: the two arms of a comparison must seed the
	// same docIDs in the same order for the postings to be comparable.
	targetID := strfmt.UUID("11111111-2222-3333-4444-555555555555")

	textSeed := func(t *testing.T, className string) []*storobj.Object {
		objs := makeConvergenceTestObjects(t, seedObjects, className)
		target := createTestObjectWithText(className, "alpha bravo charlie")
		target.Object.ID = targetID
		return append(objs, target)
	}
	textObject := func(className string, id strfmt.UUID) *storobj.Object {
		obj := createTestObjectWithText(className, windowToken+" delta")
		obj.Object.ID = id
		return obj
	}

	runTrio := func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric) {
		require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
		require.NoError(t, task.RunPrepareOnShard(ctx, shard))
		require.NoError(t, task.RunSwapOnShard(ctx, shard))
	}
	runInline := func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric) {
		require.NoError(t, task.OnAfterLsmInit(ctx, shard))
		for {
			rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
			require.NoError(t, err)
			if rerunAt.IsZero() {
				break
			}
		}
	}

	return []windowWriteMigration{
		{
			name:       "enable-filterable",
			bucketName: helpers.BucketFromPropNameLSM("title"),
			targetID:   targetID,
			newClass: func(className string) *models.Class {
				return newEnableFilterableTestClass(className, "title")
			},
			seed:       textSeed,
			objectWith: textObject,
			newTask: func(t *testing.T, idx *Index, className string) *ShardReindexTaskGeneric {
				task, _ := newEnableFilterableTask(t, idx, className, "title")
				return task
			},
			drive: runTrio,
			fingerprint: func(t *testing.T, b *lsmkv.Bucket) any {
				return fingerprintRoaringSetBucket(t, b)
			},
			flipSchema: func(class *models.Class) {
				class.Properties[0].IndexFilterable = boolPtr(true)
			},
		},
		{
			name:       "enable-searchable",
			bucketName: helpers.BucketSearchableFromPropNameLSM("title"),
			targetID:   targetID,
			newClass: func(className string) *models.Class {
				return newEnableSearchableTestClass(className, []string{"title"})
			},
			seed:       textSeed,
			objectWith: textObject,
			newTask: func(t *testing.T, idx *Index, className string) *ShardReindexTaskGeneric {
				task, _ := newEnableSearchableTask(t, idx, className, "title",
					models.PropertyTokenizationWord)
				return task
			},
			drive: runTrio,
			fingerprint: func(t *testing.T, b *lsmkv.Bucket) any {
				return fingerprintInvertedBucket(t, b)
			},
			flipSchema: func(class *models.Class) {
				class.Properties[0].IndexSearchable = boolPtr(true)
			},
		},
		{
			name:       "enable-rangeable",
			bucketName: helpers.BucketRangeableFromPropNameLSM(filterableToRangeablePropName),
			targetID:   targetID,
			newClass: func(className string) *models.Class {
				class := newFilterableToRangeableTestClass(className)
				// Only an explicit false puts the record in play; the
				// fixture's nil would make this row pass for another reason.
				class.Properties[0].IndexRangeFilters = boolPtr(false)
				return class
			},
			seed: func(t *testing.T, className string) []*storobj.Object {
				objs := makeFilterableToRangeableTestObjects(t, seedObjects, className)
				target := newScoreObject(className, targetID, 1)
				return append(objs, target)
			},
			objectWith: func(className string, id strfmt.UUID) *storobj.Object {
				// Inside the range the fingerprint reads, so a moved docID
				// shows up as a change on both postings rather than nowhere.
				return newScoreObject(className, id, 3)
			},
			newTask: func(t *testing.T, idx *Index, className string) *ShardReindexTaskGeneric {
				task, _ := newFilterableToRangeableTask(t, idx, className,
					filterableToRangeablePropName)
				return task
			},
			drive: runInline,
			fingerprint: func(t *testing.T, b *lsmkv.Bucket) any {
				return filterableToRangeableFingerprint(t, b)
			},
			flipSchema: func(class *models.Class) {
				class.Properties[0].IndexRangeFilters = boolPtr(true)
			},
		},
	}
}

func newScoreObject(className string, id strfmt.UUID, score int64) *storobj.Object {
	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:         id,
			Class:      className,
			Properties: map[string]interface{}{filterableToRangeablePropName: score},
		},
	}
}
