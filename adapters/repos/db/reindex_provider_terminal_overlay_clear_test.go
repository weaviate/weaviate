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
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	entschema "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// overlayFixture is a shard holding one property, plus a provider wired to an
// enable-filterable migration over that same property.
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

// newOverlayFixture opens a shard whose single property has its filterable
// index off. searchable is applied as given, so nil leaves the searchable
// index at the schema default.
func newOverlayFixture(t *testing.T, prefix, prop string, searchable *bool) *overlayFixture {
	t.Helper()

	ctx := testCtx()
	className := prefix + "_" + uuid.NewString()[:8]

	class := newTestClassWithProps(className, []string{prop})
	class.Properties[0].IndexFilterable = boolPtr(false)
	class.Properties[0].IndexSearchable = searchable

	hot, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	t.Cleanup(func() { hot.Shutdown(context.Background()) })
	shard, err := unwrapShard(ctx, hot)
	require.NoError(t, err)

	payload, err := json.Marshal(ReindexTaskPayload{
		Collection:    className,
		MigrationType: ReindexTypeEnableFilterable,
		Properties:    []string{prop},
		UnitToShard:   map[string]string{"u1": hot.Name()},
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

// object is a write carrying text for the fixture's property.
func (f *overlayFixture) object() *storobj.Object {
	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:         strfmt.UUID(uuid.NewString()),
			Class:      f.className,
			Properties: map[string]interface{}{f.prop: "alpha bravo"},
		},
	}
}

func (f *overlayFixture) overlay() map[string]inverted.PropertyOverlay {
	return f.shard.SnapshotPropertyOverlay([]string{f.prop})
}

// After a FAILED migration no schema flip follows, so a later index delete
// sweeps the very bucket the overlay forces writes into. The mutation guard
// opens the moment the task fails, so the overlay has to be gone before the
// terminal cleanup walks disk under its own multi-second budget.
func TestWriteAfterFailedMigrationSweepsOverlaidBucket(t *testing.T) {
	tests := []struct {
		name         string
		holdsCleanup bool
	}{
		{name: "the terminal cleanup has already finished"},
		{name: "the terminal cleanup is still in flight", holdsCleanup: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			const prop = "p"
			f := newOverlayFixture(t, "FailedOverlaySweep", prop, boolPtr(true))

			require.NoError(t, f.shard.store.CreateOrLoadBucket(f.ctx,
				helpers.BucketFromPropNameLSM(prop), lsmkv.WithStrategy(lsmkv.StrategyRoaringSet)))
			f.shard.SetPropertyOverlay(prop, inverted.PropertyOverlay{ForceFilterable: true})

			task := f.task("T_failed", distributedtask.TaskStatusFailed)

			if tc.holdsCleanup {
				// A worker still draining parks the cleanup on its first step.
				handle := &reindexTaskHandle{cancel: func() {}, doneCh: make(chan struct{})}
				f.provider.mu.Lock()
				f.provider.runningHandles[task.TaskDescriptor] = handle
				f.provider.mu.Unlock()

				completed := make(chan error, 1)
				go func() { completed <- f.provider.OnTaskCompleted(task) }()
				defer func() {
					close(handle.doneCh)
					assert.NoError(t, <-completed)
				}()

				assert.Eventually(t, func() bool {
					return len(f.overlay()) == 0
				}, 5*time.Second, 5*time.Millisecond,
					"the overlay must be retired before the terminal cleanup runs, not after it")
			} else {
				require.NoError(t, f.provider.OnTaskCompleted(task))
			}

			// The delete drops every bucket the schema says is off, including the
			// one the failed migration left behind.
			f.class.Properties[0].IndexSearchable = boolPtr(false)
			eg := enterrors.NewErrorGroupWrapper(f.shard.index.logger)
			var reads atomic.Int64
			f.shard.updatePropertyBuckets(f.ctx, eg, f.class.Properties[0], &reads)
			require.NoError(t, eg.Wait())
			require.Nil(t, f.shard.store.Bucket(helpers.BucketFromPropNameLSM(prop)),
				"the sweep must remove the bucket the overlay points at")

			obj := f.object()
			require.NoError(t, f.shard.PutObject(f.ctx, obj),
				"a write after the sweep must not demand the removed bucket")

			analyzed, _, _, err := f.shard.AnalyzeObject(obj)
			require.NoError(t, err)
			names := make([]string, 0, len(analyzed))
			for _, a := range analyzed {
				names = append(names, a.Name)
			}
			require.NotEmpty(t, names, "analysis still runs; only this property drops out")
			assert.NotContains(t, names, prop,
				"the schema indexes the property nowhere, so the analyzer must skip it")
		})
	}
}

// A node can read FINISHED from the leader before its own schema flip
// applies, so the completion tick declines to clear. Applying the flip has to
// retire the overlay, or a later index delete revives it.
func TestOverlayOutlivingFinishedIsClearedByTheLocalFlip(t *testing.T) {
	const prop = "p"
	f := newOverlayFixture(t, "FinishedOverlayLag", prop, nil)

	f.shard.SetPropertyOverlay(prop, inverted.PropertyOverlay{ForceFilterable: true})

	require.NoError(t, f.provider.OnTaskCompleted(
		f.task("T_finished", distributedtask.TaskStatusFinished)))
	require.NotEmpty(t, f.overlay(),
		"this node's schema is still pre-flip, so the overlay must survive, "+
			"or this test proves nothing")

	f.class.Properties[0].IndexFilterable = boolPtr(true)
	require.NoError(t, f.idx.updateProperty(f.ctx, f.class.Properties[0]))
	require.Empty(t, f.overlay(),
		"applying the flip must retire the overlay the completion tick could not")

	// An index delete takes the bucket away again, which is what makes a
	// surviving entry fatal rather than inert.
	f.class.Properties[0].IndexFilterable = boolPtr(false)
	require.NoError(t, f.idx.updateProperty(f.ctx, f.class.Properties[0]))

	require.NoError(t, f.shard.PutObject(f.ctx, f.object()),
		"a write after the index delete must not demand a bucket the overlay forces")
}

// A change-tokenization that failed after a partial swap keeps its overlay on
// purpose: the entry is the only thing aligning queries on that shard with the
// bucket it already swapped. Conflict detection skips tasks that are no longer
// active, so a second migration on the same property is admitted — and must
// leave that entry standing.
func TestSecondMigrationKeepsAFailedTokenizationOverlay(t *testing.T) {
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

			f := newOverlayFixture(t, "OverlayMerge", prop, boolPtr(true))

			// change-tokenization swapped this shard's searchable bucket, then
			// the task failed elsewhere and left the entry behind.
			f.shard.SetPropertyOverlay(prop, inverted.PropertyOverlay{Tokenization: swapped})
			require.Equal(t, swapped, f.shard.TokenizationFor(prop, live),
				"the failed tokenization change must own the overlay, or this test proves nothing")

			tc.arm(t, f.shard, prop, inverted.PropertyOverlay{ForceFilterable: true})

			require.Equal(t, swapped, f.shard.TokenizationFor(prop, live),
				"enabling the filterable index must not drop the tokenization override")
			require.True(t, f.overlay()[prop].ForceFilterable,
				"and it must still arm its own flag")

			require.NoError(t, f.provider.OnTaskCompleted(
				f.task("T_second", distributedtask.TaskStatusFailed)))

			require.False(t, f.overlay()[prop].ForceFilterable,
				"the second migration's own flag goes with it")
			require.Equal(t, swapped, f.shard.TokenizationFor(prop, live),
				"but retiring it must not take the first migration's tokenization along")
		})
	}
}
