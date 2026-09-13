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
			ctx := testCtx()
			className := "FailedOverlaySweep_" + uuid.NewString()[:8]
			const prop = "p"

			class := newTestClassWithProps(className, []string{prop})
			class.Properties[0].IndexFilterable = boolPtr(false)
			class.Properties[0].IndexSearchable = boolPtr(true)

			hot, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			defer hot.Shutdown(context.Background())
			shard, err := unwrapShard(ctx, hot)
			require.NoError(t, err)

			require.NoError(t, shard.store.CreateOrLoadBucket(ctx,
				helpers.BucketFromPropNameLSM(prop), lsmkv.WithStrategy(lsmkv.StrategyRoaringSet)))
			shard.SetPropertyOverlay(prop, inverted.PropertyOverlay{ForceFilterable: true})

			payload, err := json.Marshal(ReindexTaskPayload{
				Collection:    className,
				MigrationType: ReindexTypeEnableFilterable,
				Properties:    []string{prop},
				UnitToShard:   map[string]string{"u1": hot.Name()},
			})
			require.NoError(t, err)

			logger, _ := logrustest.NewNullLogger()
			p := NewReindexProvider(
				&DB{indices: map[string]*Index{indexID(entschema.ClassName(className)): idx}},
				nil, nil, logger, "n1", nil, ctx)
			task := &distributedtask.Task{
				Namespace:      ReindexNamespace,
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_failed", Version: 1},
				Status:         distributedtask.TaskStatusFailed,
				Payload:        payload,
			}

			if tc.holdsCleanup {
				// A worker still draining parks the cleanup on its first step.
				handle := &reindexTaskHandle{cancel: func() {}, doneCh: make(chan struct{})}
				p.mu.Lock()
				p.runningHandles[task.TaskDescriptor] = handle
				p.mu.Unlock()

				completed := make(chan error, 1)
				go func() { completed <- p.OnTaskCompleted(task) }()
				defer func() {
					close(handle.doneCh)
					assert.NoError(t, <-completed)
				}()

				assert.Eventually(t, func() bool {
					return len(shard.SnapshotPropertyOverlay([]string{prop})) == 0
				}, 5*time.Second, 5*time.Millisecond,
					"the overlay must be retired before the terminal cleanup runs, not after it")
			} else {
				require.NoError(t, p.OnTaskCompleted(task))
			}

			// The delete drops every bucket the schema says is off, including the
			// one the failed migration left behind.
			class.Properties[0].IndexSearchable = boolPtr(false)
			eg := enterrors.NewErrorGroupWrapper(shard.index.logger)
			var reads atomic.Int64
			shard.updatePropertyBuckets(ctx, eg, class.Properties[0], &reads)
			require.NoError(t, eg.Wait())
			require.Nil(t, shard.store.Bucket(helpers.BucketFromPropNameLSM(prop)),
				"the sweep must remove the bucket the overlay points at")

			obj := &storobj.Object{
				MarshallerVersion: 1,
				Object: models.Object{
					ID:         strfmt.UUID(uuid.NewString()),
					Class:      className,
					Properties: map[string]interface{}{prop: "alpha bravo"},
				},
			}
			require.NoError(t, shard.PutObject(ctx, obj),
				"a write after the sweep must not demand the removed bucket")

			analyzed, _, _, err := shard.AnalyzeObject(obj)
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
	ctx := testCtx()
	className := "FinishedOverlayLag_" + uuid.NewString()[:8]
	const prop = "p"

	class := newTestClassWithProps(className, []string{prop})
	class.Properties[0].IndexFilterable = boolPtr(false)

	hot, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	defer hot.Shutdown(context.Background())
	shard, err := unwrapShard(ctx, hot)
	require.NoError(t, err)

	shard.SetPropertyOverlay(prop, inverted.PropertyOverlay{ForceFilterable: true})

	payload, err := json.Marshal(ReindexTaskPayload{
		Collection:    className,
		MigrationType: ReindexTypeEnableFilterable,
		Properties:    []string{prop},
		UnitToShard:   map[string]string{"u1": hot.Name()},
	})
	require.NoError(t, err)

	logger, _ := logrustest.NewNullLogger()
	p := NewReindexProvider(
		&DB{indices: map[string]*Index{indexID(entschema.ClassName(className)): idx}},
		nil, nil, logger, "n1", nil, ctx)
	require.NoError(t, p.OnTaskCompleted(&distributedtask.Task{
		Namespace:      ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_finished", Version: 1},
		Status:         distributedtask.TaskStatusFinished,
		Payload:        payload,
	}))
	require.NotEmpty(t, shard.SnapshotPropertyOverlay([]string{prop}),
		"this node's schema is still pre-flip, so the overlay must survive, "+
			"or this test proves nothing")

	class.Properties[0].IndexFilterable = boolPtr(true)
	require.NoError(t, idx.updateProperty(ctx, class.Properties[0]))
	require.Empty(t, shard.SnapshotPropertyOverlay([]string{prop}),
		"applying the flip must retire the overlay the completion tick could not")

	// An index delete takes the bucket away again, which is what makes a
	// surviving entry fatal rather than inert.
	class.Properties[0].IndexFilterable = boolPtr(false)
	require.NoError(t, idx.updateProperty(ctx, class.Properties[0]))

	obj := &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:         strfmt.UUID(uuid.NewString()),
			Class:      className,
			Properties: map[string]interface{}{prop: "alpha bravo"},
		},
	}
	require.NoError(t, shard.PutObject(ctx, obj),
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
			ctx := testCtx()
			className := "OverlayMerge_" + uuid.NewString()[:8]
			const prop = "title"
			const live = models.PropertyTokenizationWord
			const swapped = models.PropertyTokenizationWhitespace

			class := newTestClassWithProps(className, []string{prop})
			class.Properties[0].IndexFilterable = boolPtr(false)
			class.Properties[0].IndexSearchable = boolPtr(true)

			hot, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			defer hot.Shutdown(context.Background())
			shard, err := unwrapShard(ctx, hot)
			require.NoError(t, err)

			// change-tokenization swapped this shard's searchable bucket, then
			// the task failed elsewhere and left the entry behind.
			shard.SetPropertyOverlay(prop, inverted.PropertyOverlay{Tokenization: swapped})
			require.Equal(t, swapped, shard.TokenizationFor(prop, live),
				"the failed tokenization change must own the overlay, or this test proves nothing")

			tc.arm(t, shard, prop, inverted.PropertyOverlay{ForceFilterable: true})

			require.Equal(t, swapped, shard.TokenizationFor(prop, live),
				"enabling the filterable index must not drop the tokenization override")
			require.True(t, shard.SnapshotPropertyOverlay([]string{prop})[prop].ForceFilterable,
				"and it must still arm its own flag")

			payload, err := json.Marshal(ReindexTaskPayload{
				Collection:    className,
				MigrationType: ReindexTypeEnableFilterable,
				Properties:    []string{prop},
				UnitToShard:   map[string]string{"u1": hot.Name()},
			})
			require.NoError(t, err)

			logger, _ := logrustest.NewNullLogger()
			p := NewReindexProvider(
				&DB{indices: map[string]*Index{indexID(entschema.ClassName(className)): idx}},
				nil, nil, logger, "n1", nil, ctx)
			require.NoError(t, p.OnTaskCompleted(&distributedtask.Task{
				Namespace:      ReindexNamespace,
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_second", Version: 1},
				Status:         distributedtask.TaskStatusFailed,
				Payload:        payload,
			}))

			require.False(t, shard.SnapshotPropertyOverlay([]string{prop})[prop].ForceFilterable,
				"the second migration's own flag goes with it")
			require.Equal(t, swapped, shard.TokenizationFor(prop, live),
				"but retiring it must not take the first migration's tokenization along")
		})
	}
}
