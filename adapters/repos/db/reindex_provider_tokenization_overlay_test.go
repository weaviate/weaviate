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

	"github.com/google/uuid"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	entschema "github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Unit coverage for the #216 Gap B overlay set/clear lifecycle without a
// full provider+DB+index. Key invariant: the overlay is set only when
// the per-prop hook fires, never eagerly at wiring time.

// overlayTasks builds the task slice maybeWirePerPropOverlaySet inspects.
// The wiring reads each task's strategy for the flags that migration turns
// on, so a bare task struct is not enough. A nil entry stays nil.
func overlayTasks(strategies ...MigrationStrategy) []*ShardReindexTaskGeneric {
	tasks := make([]*ShardReindexTaskGeneric, len(strategies))
	for i, strategy := range strategies {
		if strategy == nil {
			continue
		}
		tasks[i] = &ShardReindexTaskGeneric{strategy: strategy}
	}
	return tasks
}

// fireAllPropHooks simulates a swap loop where every prop flipped.
func fireAllPropHooks(tasks []*ShardReindexTaskGeneric, props []string) int {
	fired := 0
	for _, task := range tasks {
		if task == nil || task.onPropSwapped == nil {
			continue
		}
		for _, propName := range props {
			task.onPropSwapped(propName)
			fired++
		}
	}
	return fired
}

// The overlay a task installs comes from its strategy, so membership of the
// [IsSemanticMigration] family is the whole gate: a new member arms here with
// no wiring change. Tokenization is the one part no strategy carries, so it
// comes off the payload.
func TestMaybeWirePerPropOverlaySet_SemanticFamilyCoverage(t *testing.T) {
	tests := []struct {
		name         string
		migration    ReindexMigrationType
		strategy     MigrationStrategy
		tokenization string
		wantWired    bool
		wantOverlay  inverted.PropertyOverlay
	}{
		{
			name:         "change-tokenization moves the tokenization only",
			migration:    ReindexTypeChangeTokenization,
			strategy:     &SearchableRetokenizeStrategy{},
			tokenization: "field",
			wantWired:    true,
			wantOverlay:  inverted.PropertyOverlay{Tokenization: "field"},
		},
		{
			name:         "change-tokenization-filterable does the same on the filterable bucket",
			migration:    ReindexTypeChangeTokenizationFilterable,
			strategy:     &FilterableRetokenizeStrategy{},
			tokenization: "word",
			wantWired:    true,
			wantOverlay:  inverted.PropertyOverlay{Tokenization: "word"},
		},
		{
			name:        "enable-filterable forces the filterable flag",
			migration:   ReindexTypeEnableFilterable,
			strategy:    &EnableFilterableStrategy{},
			wantWired:   true,
			wantOverlay: inverted.PropertyOverlay{ForceFilterable: true},
		},
		{
			name:        "enable-searchable forces the searchable flag and its tokenization",
			migration:   ReindexTypeEnableSearchable,
			strategy:    &EnableSearchableStrategy{tokenization: "field"},
			wantWired:   true,
			wantOverlay: inverted.PropertyOverlay{ForceSearchable: true, Tokenization: "field"},
		},
		{
			name:        "enable-rangeable forces the rangeable flag",
			migration:   ReindexTypeEnableRangeable,
			strategy:    &FilterableToRangeableStrategy{},
			wantWired:   true,
			wantOverlay: inverted.PropertyOverlay{ForceRangeable: true},
		},
		{
			name:      "map-to-blockmax is semantic but changes no analyzer input",
			migration: ReindexTypeChangeAlgorithm,
			strategy:  &MapToBlockmaxStrategy{},
			wantWired: false,
		},
	}

	covered := map[ReindexMigrationType]bool{}
	for _, tc := range tests {
		covered[tc.migration] = true
		t.Run(tc.name, func(t *testing.T) {
			s := &Shard{}
			tasks := overlayTasks(tc.strategy)
			payload := &ReindexTaskPayload{
				MigrationType:      tc.migration,
				TargetTokenization: tc.tokenization,
				Properties:         []string{"name"},
			}
			maybeWirePerPropOverlaySet(s, payload, tasks)
			if !tc.wantWired {
				assert.Nil(t, tasks[0].onPropSwapped)
				assert.Nil(t, s.SnapshotPropertyOverlay([]string{"name"}))
				return
			}

			assert.Nil(t, s.SnapshotPropertyOverlay([]string{"name"}),
				"wiring must not pre-set the overlay; that's the bug being fixed")
			fireAllPropHooks(tasks, payload.Properties)
			assert.Equal(t, tc.wantOverlay, s.SnapshotPropertyOverlay([]string{"name"})["name"])
		})
	}

	for _, mt := range allReindexMigrationTypesForTest {
		if IsSemanticMigration(mt) {
			assert.Truef(t, covered[mt], "semantic migration %q has no row saying what overlay it installs", mt)
		}
	}
}

func TestMaybeWirePerPropOverlaySet_EmptyTargetTokenization_NoOp(t *testing.T) {
	s := &Shard{}
	tasks := overlayTasks(&SearchableRetokenizeStrategy{})
	payload := &ReindexTaskPayload{
		MigrationType:      ReindexTypeChangeTokenization,
		TargetTokenization: "", // payload missing target
		Properties:         []string{"name"},
	}
	maybeWirePerPropOverlaySet(s, payload, tasks)
	assert.Nil(t, tasks[0].onPropSwapped,
		"empty target tokenization must skip wiring — better than writing an empty override")
	assert.Equal(t, "word", s.TokenizationFor("name", "word"))
}

func TestMaybeWirePerPropOverlaySet_NilInputs_NoOp(t *testing.T) {
	// Pure guard against nil-deref under unexpected call sites; both
	// inputs are non-nil in production but defensive checks let the
	// helper be tested via unit tests without bringing up a real
	// shard.
	maybeWirePerPropOverlaySet(nil, &ReindexTaskPayload{}, nil)
	maybeWirePerPropOverlaySet(&Shard{}, nil, nil)
}

func TestMaybeWirePerPropOverlaySet_NilTaskInSlice_Skipped(t *testing.T) {
	// A nil task entry must not panic — defensive, mirrors the
	// production loop's nil guard.
	s := &Shard{}
	tasks := overlayTasks(nil, &SearchableRetokenizeStrategy{})
	payload := &ReindexTaskPayload{
		MigrationType:      ReindexTypeChangeTokenization,
		TargetTokenization: "field",
		Properties:         []string{"name"},
	}
	maybeWirePerPropOverlaySet(s, payload, tasks)
	require.NotNil(t, tasks[1].onPropSwapped, "non-nil task must get the hook")
	fireAllPropHooks(tasks, payload.Properties)
	assert.Equal(t, "field", s.TokenizationFor("name", "word"))
}

// The overlay is in-memory state the swap hook set on shards this node
// loaded to run the migration on. A shard that is not loaded holds none,
// so reaching into one to clear nothing loads a cold tenant on the success
// path of every change-tokenization migration.
func TestOnTaskCompletedOverlayClearLeavesUnloadedShardsAlone(t *testing.T) {
	const (
		prop   = "title"
		tenant = "cold-tenant"
	)

	ctx := testCtx()
	className := "OverlayClear_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{prop})
	hot, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	defer hot.Shutdown(context.Background())

	loaded, err := unwrapShard(ctx, hot)
	require.NoError(t, err)
	loaded.SetPropertyOverlay(prop, inverted.PropertyOverlay{Tokenization: "field"})

	cold := NewLazyLoadShard(ctx, nil, tenant, idx, class, idx.centralJobQueue,
		idx.indexCheckpoints, idx.allocChecker, idx.shardLoadLimiter, idx.shardReindexer,
		false, idx.bitmapBufPool)
	idx.shards.Store(tenant, cold)
	defer func() {
		if cold.isLoaded() {
			require.NoError(t, cold.Shutdown(context.Background()))
		}
	}()

	payload, err := json.Marshal(ReindexTaskPayload{
		Collection:         className,
		MigrationType:      ReindexTypeChangeTokenization,
		TargetTokenization: "field",
		Properties:         []string{prop},
		UnitToShard:        map[string]string{"u1": hot.Name()},
	})
	require.NoError(t, err)

	logger, _ := logrustest.NewNullLogger()
	p := NewReindexProvider(
		&DB{indices: map[string]*Index{indexID(entschema.ClassName(className)): idx}},
		nil, nil, logger, "n1", nil, ctx)

	require.NoError(t, p.OnTaskCompleted(&distributedtask.Task{
		Namespace:      ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_swap", Version: 1},
		Status:         distributedtask.TaskStatusSwapping,
		Payload:        payload,
	}))

	assert.Equal(t, "word", loaded.TokenizationFor(prop, "word"),
		"the shard the migration ran on holds the overlay, so its clear is the point of the walk")
	require.False(t, cold.isLoaded(),
		"an unloaded shard holds no in-memory overlay; loading one to clear nothing is "+
			"what the cutover path cannot afford")
}
