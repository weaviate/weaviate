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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	entschema "github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestLogOperatorRepairGuidanceOnTornSemanticMigration_* pin the
// operator-actionable-error half of #221: when a semantic-migration
// task transitions to FAILED, OnTaskCompleted logs the exact REST
// command an operator should issue to repair the partial-completion
// bucket↔schema inversion.
//
// We assert on the log entry's structured fields (so the message text
// can drift without breaking the test) and on the embedded
// repair_command field (so the operator's copy-pasteable command stays
// stable).

func TestLogOperatorRepairGuidanceOnTornSemanticMigration_ChangeTokenizationBothIndexes(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()

	payload := &ReindexTaskPayload{
		Collection:         "Products",
		MigrationType:      ReindexTypeChangeTokenization,
		Properties:         []string{"name"},
		TargetTokenization: "field",
	}
	logOperatorRepairGuidanceOnPartialSwap(logger.WithField("taskID", "T1"), payload, distributedtask.TaskStatusFailed)

	require.Len(t, hook.Entries, 1, "expected one error entry per property")
	entry := hook.Entries[0]
	require.Equal(t, logrus.ErrorLevel, entry.Level)
	require.Equal(t, "name", entry.Data["property"])
	require.Equal(t, ReindexTypeChangeTokenization, entry.Data["migration_type"])
	// change-tokenization can tear either inverted index; guidance must
	// instruct the operator to rebuild both.
	require.Equal(t,
		`PUT /v1/schema/Products/indexes/name {"filterable":{"rebuild":true},"searchable":{"rebuild":true}}`,
		entry.Data["repair_command"])
	require.Contains(t, entry.Message, "FAILED")
	require.Contains(t, entry.Message, "bucket")
}

func TestLogOperatorRepairGuidanceOnTornSemanticMigration_ChangeTokenizationFilterableOnly(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()

	payload := &ReindexTaskPayload{
		Collection:         "Products",
		MigrationType:      ReindexTypeChangeTokenizationFilterable,
		Properties:         []string{"category"},
		TargetTokenization: "field",
	}
	logOperatorRepairGuidanceOnPartialSwap(logger.WithField("taskID", "T2"), payload, distributedtask.TaskStatusFailed)

	require.Len(t, hook.Entries, 1)
	entry := hook.Entries[0]
	// change-tokenization-filterable touches ONLY the filterable bucket;
	// guidance must scope to that.
	require.Equal(t,
		`PUT /v1/schema/Products/indexes/category {"filterable":{"rebuild":true}}`,
		entry.Data["repair_command"])
}

func TestLogOperatorRepairGuidanceOnTornSemanticMigration_MultipleProperties(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()

	payload := &ReindexTaskPayload{
		Collection:    "Products",
		MigrationType: ReindexTypeEnableFilterable,
		Properties:    []string{"a", "b", "c"},
	}
	logOperatorRepairGuidanceOnPartialSwap(logger.WithField("taskID", "T3"), payload, distributedtask.TaskStatusFailed)

	// One entry per property — easier for log scrapers to alert per-prop.
	require.Len(t, hook.Entries, 3)
	gotProps := make([]string, len(hook.Entries))
	for i, entry := range hook.Entries {
		gotProps[i] = entry.Data["property"].(string)
	}
	require.ElementsMatch(t, []string{"a", "b", "c"}, gotProps)
}

func TestLogOperatorRepairGuidanceOnTornSemanticMigration_FormatOnlyMigrationIsNoOp(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()

	// Format-only migrations must not emit operator guidance.
	for _, mt := range []ReindexMigrationType{
		ReindexTypeRepairFilterable,
		ReindexTypeRepairRangeable,
	} {
		t.Run(string(mt), func(t *testing.T) {
			hook.Reset()
			payload := &ReindexTaskPayload{
				Collection:    "Products",
				MigrationType: mt,
				Properties:    []string{"name"},
			}
			logOperatorRepairGuidanceOnPartialSwap(logger.WithField("taskID", "T"), payload, distributedtask.TaskStatusFailed)
			require.Empty(t, hook.Entries,
				"format-only migration %s must not produce repair guidance", mt)
		})
	}
}

func TestLogOperatorRepairGuidanceOnTornSemanticMigration_EmptyPropertiesEmitsGenericGuidance(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()

	payload := &ReindexTaskPayload{
		Collection:    "Products",
		MigrationType: ReindexTypeChangeTokenization,
		Properties:    nil, // reserved for future whole-collection rebuild
	}
	logOperatorRepairGuidanceOnPartialSwap(logger.WithField("taskID", "T4"), payload, distributedtask.TaskStatusFailed)

	require.Len(t, hook.Entries, 1, "empty Properties → one generic guidance entry")
	require.Contains(t, hook.Entries[0].Message, "empty Properties")
}

// Pins: repair guidance on a CANCELLED task follows the evidence that a
// node got past its units — either ack map, since PREP acks land in a
// different one from post-swap acks.
func TestOnTaskCompleted_CancelledLogsRepairGuidanceOnlyWhenASwapRan(t *testing.T) {
	payload, err := json.Marshal(ReindexTaskPayload{
		MigrationType: ReindexTypeChangeTokenization,
		Collection:    "C",
		Properties:    []string{"title"},
	})
	require.NoError(t, err)

	acked := map[string]distributedtask.PostCompletionAck{"n1": {Success: true}}

	for _, tc := range []struct {
		name         string
		postAcks     map[string]distributedtask.PostCompletionAck
		prepAcks     map[string]distributedtask.PostCompletionAck
		wantGuidance bool
	}{
		{name: "no node acked anything", wantGuidance: false},
		{name: "one node acked a swap", postAcks: acked, wantGuidance: true},
		// PREP writes merged.mig, which arms the next restart to promote
		// the ingest dir to the canonical bucket name — the tear is
		// already possible before any swap ack exists.
		{name: "one node acked PREP only", prepAcks: acked, wantGuidance: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()
			p := &ReindexProvider{
				logger:    logger,
				serverCtx: context.Background(),
				// Terminal-status cleanup needs a DB; an empty one is a no-op.
				db: &DB{},
			}

			require.NoError(t, p.OnTaskCompleted(&distributedtask.Task{
				Namespace:                 ReindexNamespace,
				TaskDescriptor:            distributedtask.TaskDescriptor{ID: "T_cancel", Version: 1},
				Status:                    distributedtask.TaskStatusCancelled,
				Payload:                   payload,
				PostCompletionAcks:        tc.postAcks,
				PreparationCompletionAcks: tc.prepAcks,
			}))

			var guided bool
			var guidance string
			for _, e := range hook.AllEntries() {
				if _, ok := e.Data["repair_command"]; ok {
					guided = true
					guidance = e.Message
				}
			}
			require.Equal(t, tc.wantGuidance, guided,
				"repair guidance on a CANCELLED task must follow the swap evidence")
			if tc.wantGuidance {
				// The guidance is written so the operator can match it to
				// the task, which only works if it names the status the
				// task actually ended in.
				require.Contains(t, guidance, string(distributedtask.TaskStatusCancelled))
				require.NotContains(t, guidance, string(distributedtask.TaskStatusFailed))
			}
		})
	}
}

// Pins the disk evidence the ack maps cannot carry: a cancel landing
// while the task is still STARTED, after this node wrote merged.mig,
// leaves no ack anywhere — the late ack hits an already-CANCELLED task
// and is dropped. merged.mig is what arms the next restart to promote
// the ingest dir, so its presence is what the guidance has to key on.
func TestHasCompletedMigrationTracker(t *testing.T) {
	const prop = "title"
	perProp := postMergeTrackerDir(t, prop)
	classLevel := MigrationDirSearchableMapToBlockmax + "_1"

	for _, tc := range []struct {
		name          string
		migrationType ReindexMigrationType
		tracker       string
		sentinel      string
		want          bool
	}{
		{
			name: "started but not merged", migrationType: ReindexTypeChangeTokenization,
			tracker: perProp, sentinel: "started.mig", want: false,
		},
		{
			name: "merged, awaiting the next restart", migrationType: ReindexTypeChangeTokenization,
			tracker: perProp, sentinel: "merged.mig", want: true,
		},
		{
			name: "tidied", migrationType: ReindexTypeChangeTokenization,
			tracker: perProp, sentinel: "tidied.mig", want: true,
		},
		{name: "no tracker dir at all", migrationType: ReindexTypeChangeTokenization, want: false},
		// change-algorithm keeps one tracker for the whole class, which the
		// per-property scope never looks at.
		{
			name: "a merged class-level tracker", migrationType: ReindexTypeChangeAlgorithm,
			tracker: classLevel, sentinel: "merged.mig", want: true,
		},
		{
			name: "a started class-level tracker", migrationType: ReindexTypeChangeAlgorithm,
			tracker: classLevel, sentinel: "started.mig", want: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lsmPath := t.TempDir()
			if tc.tracker != "" {
				mkTrackerDir(t, lsmPath, tc.tracker, tc.sentinel)
			}

			require.Equal(t, tc.want,
				hasCompletedMigrationTracker(lsmPath, tc.migrationType, []string{prop}))
		})
	}
}

// postMergeTrackerDir is the tracker dir name a searchable migration of
// propName leaves behind, generation suffix included.
func postMergeTrackerDir(t *testing.T, propName string) string {
	t.Helper()
	prefixes := migrationDirPrefixesForIndexType("searchable")
	require.NotEmpty(t, prefixes)
	return migrationDirWithProps(prefixes[0], []string{propName}) + "_1"
}

// postMergeEvidenceFixture stands up a one-shard collection carrying the
// on-disk signature of a swap this node got far enough into: a tracker
// generation with merged.mig.
func postMergeEvidenceFixture(t *testing.T, ctx context.Context) (*ReindexProvider, *ReindexTaskPayload, string) {
	t.Helper()
	shard, idx := testShard(t, ctx, "C")
	concrete, err := unwrapShard(ctx, shard)
	require.NoError(t, err)

	trackerDir := filepath.Join(concrete.pathLSM(), ".migrations", postMergeTrackerDir(t, "title"))
	require.NoError(t, os.MkdirAll(trackerDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(trackerDir, "merged.mig"), nil, 0o600))

	payload := &ReindexTaskPayload{
		MigrationType: ReindexTypeChangeTokenization,
		Collection:    "C",
		Properties:    []string{"title"},
		UnitToShard:   map[string]string{"u1": shard.Name()},
	}
	p := NewReindexProvider(
		&DB{indices: map[string]*Index{indexID(entschema.ClassName("C")): idx}},
		nil, logrus.New(), "n1", nil, ctx)
	return p, payload, trackerDir
}

// Pins that the evidence probe answers to a context. It reads a
// directory per shard the payload names, so on a multi-tenant collection
// an unbounded one is a per-cancel fan-out of disk walks that blocks the
// scheduler tick and outlives shutdown.
func TestHasLocalPostMergeState_GivesUpOnAFinishedContext(t *testing.T) {
	ctx := context.Background()
	p, payload, _ := postMergeEvidenceFixture(t, ctx)

	require.True(t, p.hasLocalPostMergeState(ctx, payload),
		"merged.mig is on disk, so a live context must find it")

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	require.False(t, p.hasLocalPostMergeState(cancelled, payload),
		"a shut-down node must not walk the task's shards")
}

// Pins what makes the cancel repair guidance reliable: the terminal
// cleanup leaves the evidence the probe reads. Both sides key on
// completedMigrationGens — the cleanup preserves merged/tidied
// generations because wiping them out from under the live bucket pointer
// is the #10675 data loss, and the probe reads them because they are the
// signature of a swap this node armed.
//
// So a cleanup that stopped preserving them would silence this guidance
// and re-open that data loss at the same time. That shared predicate is
// also why the probe's position relative to the cleanup does not change
// the answer.
func TestAutoCleanupAfterTerminal_PreservesTheEvidenceTheProbeReads(t *testing.T) {
	ctx := context.Background()
	p, payload, trackerDir := postMergeEvidenceFixture(t, ctx)

	p.autoCleanupAfterTerminal(&distributedtask.Task{
		Namespace:      ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_cancel", Version: 1},
		Status:         distributedtask.TaskStatusCancelled,
		Payload:        []byte("{}"),
	}, payload, logrus.New())

	require.DirExists(t, trackerDir,
		"a merged generation is live deferred-finalize state, not stale partial state")
	require.True(t, p.hasLocalPostMergeState(ctx, payload),
		"the guidance would go silent for every cancel that ran the cleanup first")
}

// Pins the wiring the ack maps cannot cover: a cancel that lands while
// the task is still STARTED leaves both maps empty, so the only thing
// that can raise the alarm is this node's own disk.
func TestOnTaskCompleted_CancelledLogsRepairGuidanceFromDiskEvidence(t *testing.T) {
	ctx := context.Background()
	shard, idx := testShard(t, ctx, "C")
	concrete, err := unwrapShard(ctx, shard)
	require.NoError(t, err)
	mkTrackerDir(t, concrete.pathLSM(), postMergeTrackerDir(t, "title"), "merged.mig")

	payload, err := json.Marshal(ReindexTaskPayload{
		MigrationType: ReindexTypeChangeTokenization,
		Collection:    "C",
		Properties:    []string{"title"},
		UnitToShard:   map[string]string{"u1": shard.Name()},
	})
	require.NoError(t, err)

	logger, hook := logrustest.NewNullLogger()
	p := NewReindexProvider(
		&DB{indices: map[string]*Index{indexID(entschema.ClassName("C")): idx}},
		nil, logger, "n1", nil, ctx)

	require.NoError(t, p.OnTaskCompleted(&distributedtask.Task{
		Namespace:      ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_cancel_disk", Version: 1},
		Status:         distributedtask.TaskStatusCancelled,
		Payload:        payload,
	}))

	require.True(t, loggedRepairGuidance(hook),
		"merged.mig on this node is the only evidence of the tear; the guidance has to fire off it")
}

// Pins where the post-merge probe sits relative to the drain: a drain that
// does not finish skips the cleanup, but the tear it leaves behind is still
// the operator's to repair, so the guidance has to fire anyway.
func TestOnTaskCompleted_CancelledLogsRepairGuidanceWhenTheDrainTimesOut(t *testing.T) {
	className := "DrainTimeout_" + uuid.NewString()[:8]
	shard, idx := testShard(t, testCtx(), className)
	concrete, err := unwrapShard(testCtx(), shard)
	require.NoError(t, err)
	mkTrackerDir(t, concrete.pathLSM(), postMergeTrackerDir(t, "title"), "merged.mig")

	payload, err := json.Marshal(ReindexTaskPayload{
		MigrationType: ReindexTypeChangeTokenization,
		Collection:    className,
		Properties:    []string{"title"},
		UnitToShard:   map[string]string{"u1": shard.Name()},
	})
	require.NoError(t, err)

	// The server context expires shortly, so the drain's bounded child ends
	// on that deadline instead of reindexTerminalCleanupDrainTimeout — ahead
	// of, not behind, the probe (which runs first, off the same context), so
	// the probe still gets to read the disk before the drain times out.
	expired, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	logger, hook := logrustest.NewNullLogger()
	p := NewReindexProvider(
		&DB{indices: map[string]*Index{indexID(entschema.ClassName(className)): idx}},
		nil, logger, "n1", nil, expired)

	desc := distributedtask.TaskDescriptor{ID: "T_cancel_drain", Version: 1}
	// A worker that never exits, so the deadline is what ends the drain
	// rather than the "nothing is running here" short-circuit.
	structuralInvariantInjectHandle(p, desc)

	require.NoError(t, p.OnTaskCompleted(&distributedtask.Task{
		Namespace:      ReindexNamespace,
		TaskDescriptor: desc,
		Status:         distributedtask.TaskStatusCancelled,
		Payload:        payload,
	}))

	var drainTimedOut bool
	for _, e := range hook.AllEntries() {
		if strings.Contains(e.Message, "drain did not finish") {
			drainTimedOut = true
		}
	}
	require.True(t, drainTimedOut,
		"the fixture has to reach the drain-timeout arm for the rest to mean anything")
	require.True(t, loggedRepairGuidance(hook),
		"the cleanup is skipped on this arm, but merged.mig is still a tear only an operator can repair")
}

// The terminal-cleanup path runs on every node of a cancelled or failed
// migration, with the collection's tenants as cold as the operator left
// them. The post-merge probe is the one thing on it that reads a shard, and
// reading it must not load it.
func TestHasLocalPostMergeStateLeavesUnloadedShardsAlone(t *testing.T) {
	const (
		prop   = "title"
		tenant = "cold-tenant"
	)

	for _, tc := range []struct {
		name          string
		migrationType ReindexMigrationType
		postMerge     bool
		// absentFromShardMap leaves the shard out of this node's map while
		// the payload still names it.
		absentFromShardMap bool
		want               bool
	}{
		{
			name:          "a cold tenant carrying merged state",
			migrationType: ReindexTypeChangeTokenization,
			postMerge:     true,
			want:          true,
		},
		{
			name:          "a cold tenant carrying nothing",
			migrationType: ReindexTypeChangeTokenization,
		},
		// A format-only migration reports nothing even with a searchable
		// tracker on disk, because that tracker belongs to no tuple it owns.
		// The IsSemanticMigration early return is a short-circuit on top of
		// that, not what produces the answer.
		{
			name:          "a format-only migration",
			migrationType: ReindexTypeRebuildSearchable,
			postMerge:     true,
		},
		// This walk applies no node filter, so it reaches every shard the
		// payload names, including another node's. Membership in this node's
		// shard map is the only thing keeping it out of that node's path.
		{
			name:               "a shard the payload names that this node's map does not hold",
			migrationType:      ReindexTypeChangeTokenization,
			postMerge:          true,
			absentFromShardMap: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "PostMergeProbe_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{prop})
			hot, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			defer hot.Shutdown(context.Background())

			if tc.postMerge {
				mkTrackerDir(t, shardPathLSM(idx.path(), tenant),
					postMergeTrackerDir(t, prop), "merged.mig")
			}
			cold := NewLazyLoadShard(ctx, nil, tenant, idx, class, idx.centralJobQueue,
				idx.indexCheckpoints, idx.allocChecker, idx.shardLoadLimiter, idx.shardReindexer,
				false, idx.bitmapBufPool)
			if !tc.absentFromShardMap {
				idx.shards.Store(tenant, cold)
			}
			defer func() {
				if cold.isLoaded() {
					require.NoError(t, cold.Shutdown(context.Background()))
				}
			}()

			logger, _ := logrustest.NewNullLogger()
			p := NewReindexProvider(
				&DB{indices: map[string]*Index{indexID(entschema.ClassName(className)): idx}},
				nil, logger, "n1", nil, ctx)

			got := p.hasLocalPostMergeState(ctx, &ReindexTaskPayload{
				Collection:    className,
				MigrationType: tc.migrationType,
				Properties:    []string{prop},
				UnitToShard:   map[string]string{"u1": tenant},
			})

			require.Equal(t, tc.want, got)
			require.False(t, cold.isLoaded(),
				"the tracker dir sits at a path this node can join; loading a tenant to "+
					"ask it for that path is what the terminal path cannot afford")
		})
	}
}

// loggedRepairGuidance reports whether any entry carries the operator's
// copy-pasteable repair command.
func loggedRepairGuidance(hook *logrustest.Hook) bool {
	for _, e := range hook.AllEntries() {
		if _, ok := e.Data["repair_command"]; ok {
			return true
		}
	}
	return false
}
