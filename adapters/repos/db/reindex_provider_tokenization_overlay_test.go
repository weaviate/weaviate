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
	"github.com/weaviate/weaviate/cluster/distributedtask"
	entschema "github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Unit coverage for the #216 Gap B overlay set/clear lifecycle without a
// full provider+DB+index. Key invariant: the overlay is set only when
// the per-prop hook fires, never eagerly at wiring time. The all-failed
// case (orig. Copilot finding, PR https://github.com/weaviate/weaviate/pull/11322 review comment 3254170106)
// is the subtle one; see [maybeClearOverlayOnAllFailed].

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

func TestMaybeWirePerPropOverlaySet_TokenizationChange_WiresAndSets(t *testing.T) {
	s := &Shard{}
	tasks := []*ShardReindexTaskGeneric{{}}
	payload := &ReindexTaskPayload{
		MigrationType:      ReindexTypeChangeTokenization,
		TargetTokenization: "field",
		Properties:         []string{"name", "description"},
	}
	require.True(t, maybeWirePerPropOverlaySet(s, payload, tasks),
		"change-tokenization migration with non-empty target must wire the per-prop hook")

	// Wiring alone must NOT set the overlay; it is established only when
	// the hook fires. Setting it before the flip is the bug being fixed.
	assert.Equal(t, "word", s.TokenizationFor("name", "word"),
		"wiring must not pre-set the overlay; that's the bug being fixed")

	require.Equal(t, len(payload.Properties), fireAllPropHooks(tasks, payload.Properties),
		"hook must be wired on the task")
	assert.Equal(t, "field", s.TokenizationFor("name", "word"),
		"after the per-prop hook fires, the overlay overrides the live schema value")
	assert.Equal(t, "field", s.TokenizationFor("description", "word"))
}

func TestMaybeWirePerPropOverlaySet_FilterableVariant_WiresAndSets(t *testing.T) {
	s := &Shard{}
	tasks := []*ShardReindexTaskGeneric{{}}
	payload := &ReindexTaskPayload{
		MigrationType:      ReindexTypeChangeTokenizationFilterable,
		TargetTokenization: "word",
		Properties:         []string{"name"},
	}
	require.True(t, maybeWirePerPropOverlaySet(s, payload, tasks),
		"change-tokenization-filterable migration must also wire the hook")
	fireAllPropHooks(tasks, payload.Properties)
	assert.Equal(t, "word", s.TokenizationFor("name", "field"))
}

func TestMaybeWirePerPropOverlaySet_MigrationsWithoutAnOverlay_NoOp(t *testing.T) {
	for _, mt := range []ReindexMigrationType{
		ReindexTypeEnableFilterable,
		ReindexTypeEnableSearchable,
		ReindexTypeRepairRangeable,
	} {
		t.Run(string(mt), func(t *testing.T) {
			s := &Shard{}
			tasks := []*ShardReindexTaskGeneric{{}}
			payload := &ReindexTaskPayload{
				MigrationType:      mt,
				TargetTokenization: "field",
				Properties:         []string{"name"},
			}
			require.False(t, maybeWirePerPropOverlaySet(s, payload, tasks),
				"migration without a swap-window overlay must NOT wire the hook")
			assert.Nil(t, tasks[0].onPropSwapped,
				"no hook should be installed")
			assert.Equal(t, "word", s.TokenizationFor("name", "word"),
				"no overlay set → fall back to live schema")
		})
	}
}

// TestMaybeWirePerPropOverlaySet_EnableRangeable_WiresAndSets pins that once
// a property's bucket flips, writes must keep reaching the rangeable bucket
// until the cluster-wide flip lands (weaviate/0-weaviate-issues#464).
func TestMaybeWirePerPropOverlaySet_EnableRangeable_WiresAndSets(t *testing.T) {
	s := &Shard{}
	tasks := []*ShardReindexTaskGeneric{{}}
	payload := &ReindexTaskPayload{
		MigrationType: ReindexTypeEnableRangeable,
		Properties:    []string{"price", "amount"},
	}
	require.True(t, maybeWirePerPropOverlaySet(s, payload, tasks),
		"enable-rangeable must wire the per-prop hook")
	require.Nil(t, s.SnapshotRangeableWriteOverlay(payload.Properties),
		"wiring alone must not set the overlay; only a flip may")

	require.Equal(t, len(payload.Properties), fireAllPropHooks(tasks, payload.Properties))
	assert.Equal(t, map[string]struct{}{"price": {}, "amount": {}},
		s.SnapshotRangeableWriteOverlay(payload.Properties))

	const wasSet, anySwapped = true, true
	require.False(t, maybeClearOverlayOnAllFailed(s, payload, wasSet, anySwapped),
		"a shard that flipped keeps its overlay until the cluster-wide flip")
	assert.NotNil(t, s.SnapshotRangeableWriteOverlay(payload.Properties))

	require.True(t, maybeClearOverlayOnAllFailed(s, payload, wasSet, false),
		"a shard where every swap failed must drop the overlay")
	assert.Nil(t, s.SnapshotRangeableWriteOverlay(payload.Properties))
}

// TestClearSwapWindowOverlaysRoutesByMigrationType pins the routing every
// clear site shares: the post-flip walk, the all-failed path, and terminal
// cleanup all drop the overlay their own migration set, and no other.
func TestClearSwapWindowOverlaysRoutesByMigrationType(t *testing.T) {
	for _, tc := range []struct {
		mt              ReindexMigrationType
		wantTokCleared  bool
		wantRangeClears bool
	}{
		{mt: ReindexTypeChangeTokenization, wantTokCleared: true},
		{mt: ReindexTypeChangeTokenizationFilterable, wantTokCleared: true},
		{mt: ReindexTypeEnableRangeable, wantRangeClears: true},
		{mt: ReindexTypeEnableFilterable},
	} {
		t.Run(string(tc.mt), func(t *testing.T) {
			s := &Shard{}
			s.SetTokenizationOverlay("name", "field")
			s.SetRangeableWriteOverlay("name")

			clearSwapWindowOverlays(s, tc.mt, []string{"name"})

			gotTok := s.TokenizationFor("name", "word") == "word"
			assert.Equal(t, tc.wantTokCleared, gotTok, "tokenization overlay")
			assert.Equal(t, tc.wantRangeClears,
				s.SnapshotRangeableWriteOverlay([]string{"name"}) == nil, "rangeable write overlay")
		})
	}
}

func TestMaybeWirePerPropOverlaySet_EmptyTargetTokenization_NoOp(t *testing.T) {
	s := &Shard{}
	tasks := []*ShardReindexTaskGeneric{{}}
	payload := &ReindexTaskPayload{
		MigrationType:      ReindexTypeChangeTokenization,
		TargetTokenization: "", // payload missing target
		Properties:         []string{"name"},
	}
	require.False(t, maybeWirePerPropOverlaySet(s, payload, tasks),
		"empty target tokenization must skip wiring — better than writing an empty override")
	assert.Nil(t, tasks[0].onPropSwapped)
	assert.Equal(t, "word", s.TokenizationFor("name", "word"))
}

func TestMaybeWirePerPropOverlaySet_NilInputs_NoOp(t *testing.T) {
	// Pure guard against nil-deref under unexpected call sites; both
	// inputs are non-nil in production but defensive checks let the
	// helper be tested via unit tests without bringing up a real
	// shard.
	require.False(t, maybeWirePerPropOverlaySet(nil, &ReindexTaskPayload{}, nil))
	require.False(t, maybeWirePerPropOverlaySet(&Shard{}, nil, nil))
}

func TestMaybeWirePerPropOverlaySet_NilTaskInSlice_Skipped(t *testing.T) {
	// A nil task entry must not panic — defensive, mirrors the
	// production loop's nil guard.
	s := &Shard{}
	tasks := []*ShardReindexTaskGeneric{nil, {}}
	payload := &ReindexTaskPayload{
		MigrationType:      ReindexTypeChangeTokenization,
		TargetTokenization: "field",
		Properties:         []string{"name"},
	}
	require.True(t, maybeWirePerPropOverlaySet(s, payload, tasks))
	require.NotNil(t, tasks[1].onPropSwapped, "non-nil task must get the hook")
	fireAllPropHooks(tasks, payload.Properties)
	assert.Equal(t, "field", s.TokenizationFor("name", "word"))
}

func TestMaybeClearTokenizationOverlayOnAllFailed_AllFailed_Clears(t *testing.T) {
	// #216 Gap B regression. Under per-prop-atomic wiring the all-failed
	// path never fires the hook, so the overlay is never set. The
	// defensive clear is an idempotent backstop that must leave the
	// shard aligned with the live (OLD) schema either way.
	s := &Shard{}
	tasks := []*ShardReindexTaskGeneric{{}}
	payload := &ReindexTaskPayload{
		MigrationType:      ReindexTypeChangeTokenization,
		TargetTokenization: "field",
		Properties:         []string{"name", "description"},
	}
	wasSet := maybeWirePerPropOverlaySet(s, payload, tasks)
	require.True(t, wasSet)

	// All swaps failed → no hook fired → overlay never set.
	require.Equal(t, "word", s.TokenizationFor("name", "word"),
		"all-failed: overlay must not have been set (no flip → no hook)")

	const anySwapped = false
	require.True(t, maybeClearOverlayOnAllFailed(s, payload, wasSet, anySwapped),
		"defensive clear must apply when wasSet=true and anySwapped=false")

	assert.Equal(t, "word", s.TokenizationFor("name", "word"),
		"after all-failed clear: TokenizationFor must return live (OLD) value")
	assert.Equal(t, "word", s.TokenizationFor("description", "word"))
}

func TestMaybeClearTokenizationOverlayOnAllFailed_AnySwapped_NoOp(t *testing.T) {
	// Partial success path: at least one per-task swap returned nil
	// → at least one bucket pointer flipped → its hook fired and set
	// the overlay. The overlay must STAY set so the swapped index
	// type's bucket content stays aligned with the query analyzer.
	s := &Shard{}
	tasks := []*ShardReindexTaskGeneric{{}}
	payload := &ReindexTaskPayload{
		MigrationType:      ReindexTypeChangeTokenization,
		TargetTokenization: "field",
		Properties:         []string{"name"},
	}
	require.True(t, maybeWirePerPropOverlaySet(s, payload, tasks))
	fireAllPropHooks(tasks, payload.Properties) // a swap succeeded → hook fired

	const wasSet, anySwapped = true, true
	require.False(t, maybeClearOverlayOnAllFailed(s, payload, wasSet, anySwapped),
		"clear must NOT apply when at least one swap succeeded")

	assert.Equal(t, "field", s.TokenizationFor("name", "word"),
		"partial-success path: overlay must remain set")
}

func TestMaybeClearTokenizationOverlayOnAllFailed_WasNotSet_NoOp(t *testing.T) {
	// Symmetric to the wiring helper's no-op cases (non-tokenization
	// migrations, empty target): if wiring was skipped, CLEAR must also
	// be a no-op regardless of anySwapped — there's nothing to clear.
	s := &Shard{}
	payload := &ReindexTaskPayload{
		MigrationType: ReindexTypeEnableFilterable, // non-tokenization
		Properties:    []string{"name"},
	}

	for _, anySwapped := range []bool{true, false} {
		require.False(t, maybeClearOverlayOnAllFailed(s, payload, false, anySwapped),
			"wasSet=false: clear must be a no-op (anySwapped=%v)", anySwapped)
	}
}

func TestMaybeClearTokenizationOverlayOnAllFailed_NilInputs_NoOp(t *testing.T) {
	require.False(t, maybeClearOverlayOnAllFailed(nil, &ReindexTaskPayload{}, true, false))
	require.False(t, maybeClearOverlayOnAllFailed(&Shard{}, nil, true, false))
}

// TestTokenizationOverlay_AllFailedSwap_EndToEndLifecycle pins the
// end-to-end behavior on the shard for the canonical #216 Gap B
// failure scenario: per-prop hook wired, every swap fails (so no hook
// fires), post-loop CLEAR. Mirrors runShardSwapPhase's per-shard branch.
func TestTokenizationOverlay_AllFailedSwap_EndToEndLifecycle(t *testing.T) {
	s := &Shard{}
	tasks := []*ShardReindexTaskGeneric{{}}
	payload := &ReindexTaskPayload{
		MigrationType:      ReindexTypeChangeTokenization,
		TargetTokenization: "field",
		Properties:         []string{"name"},
	}

	wasSet := maybeWirePerPropOverlaySet(s, payload, tasks)
	require.True(t, wasSet)

	// No flip happens, so the hook never fires.
	const anySwapped = false

	cleared := maybeClearOverlayOnAllFailed(s, payload, wasSet, anySwapped)
	require.True(t, cleared,
		"end-to-end: defensive clear must fire on all-failed path")

	// Overlay cleared, so the query returns the untouched OLD value
	// rather than the misalignment the FAILED migration would leave.
	assert.Equal(t, "word", s.TokenizationFor("name", "word"),
		"end-to-end: post all-failed clear, TokenizationFor returns live (untouched OLD) value")
}

// TestTokenizationOverlay_AnySwapped_EndToEndLifecycle: a partial
// success (≥1 flip) must leave the overlay set, so the defensive clear
// is a no-op.
func TestTokenizationOverlay_AnySwapped_EndToEndLifecycle(t *testing.T) {
	s := &Shard{}
	tasks := []*ShardReindexTaskGeneric{{}}
	payload := &ReindexTaskPayload{
		MigrationType:      ReindexTypeChangeTokenization,
		TargetTokenization: "field",
		Properties:         []string{"name"},
	}

	wasSet := maybeWirePerPropOverlaySet(s, payload, tasks)
	require.True(t, wasSet)

	fireAllPropHooks(tasks, payload.Properties)
	const anySwapped = true

	cleared := maybeClearOverlayOnAllFailed(s, payload, wasSet, anySwapped)
	require.False(t, cleared,
		"partial-success path: defensive clear must NOT fire")

	// Overlay stays set; queries tokenize input for the NEW value
	// (matching the swapped bucket).
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
	loaded.SetTokenizationOverlay(prop, "field")

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
