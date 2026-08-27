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
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	entschema "github.com/weaviate/weaviate/entities/schema"
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

	for _, tc := range []struct {
		name string
		// migrationType defaults to change-tokenization.
		migrationType ReindexMigrationType
		// trackerDir defaults to the property's searchable dir.
		trackerDir string
		sentinel   string
		want       bool
	}{
		{name: "started but not merged", sentinel: "started.mig", want: false},
		{name: "merged, awaiting the next restart", sentinel: "merged.mig", want: true},
		{name: "tidied", sentinel: "tidied.mig", want: true},
		{name: "no tracker dir at all", want: false},
		{
			// map→blockmax keeps its tracker at the class level, which
			// the per-property dirs omit by design, so the class-level
			// arm is the only thing that can read this evidence — and
			// the cleanup never removes it, so it is what a cancelled
			// map→blockmax leaves behind.
			name:          "change-algorithm merged at the class level",
			migrationType: ReindexTypeChangeAlgorithm,
			trackerDir:    MigrationDirSearchableMapToBlockmax,
			sentinel:      "merged.mig",
			want:          true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			migrationType := tc.migrationType
			if migrationType == "" {
				migrationType = ReindexTypeChangeTokenization
			}

			lsmPath := t.TempDir()
			if tc.sentinel != "" {
				dir := tc.trackerDir
				if dir == "" {
					dirs := migrationDirsForPropertyIndex(prop, "searchable")
					require.NotEmpty(t, dirs)
					dir = dirs[0]
				}
				trackerDir := filepath.Join(lsmPath, ".migrations", dir+"_1")
				require.NoError(t, os.MkdirAll(trackerDir, 0o755))
				require.NoError(t, os.WriteFile(filepath.Join(trackerDir, tc.sentinel), nil, 0o600))
			}

			require.Equal(t, tc.want,
				hasCompletedMigrationTracker(lsmPath, migrationType, []string{prop}))
		})
	}
}

// postMergeEvidenceFixture stands up a one-shard collection carrying the
// on-disk signature of a swap this node got far enough into: a tracker
// generation with merged.mig.
func postMergeEvidenceFixture(t *testing.T, ctx context.Context) (*ReindexProvider, *ReindexTaskPayload, string) {
	t.Helper()
	shard, idx := testShard(t, ctx, "C")
	concrete, err := unwrapShard(ctx, shard)
	require.NoError(t, err)

	dirs := migrationDirsForPropertyIndex("title", "searchable")
	require.NotEmpty(t, dirs)
	trackerDir := filepath.Join(concrete.pathLSM(), ".migrations", dirs[0]+"_1")
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

// Pins that the evidence probe answers to a context. It force-loads
// every shard the payload names, so on a multi-tenant collection an
// unbounded one is a per-cancel fan-out of lazy-shard loads that blocks
// the scheduler tick and outlives shutdown.
func TestHasLocalPostMergeState_GivesUpOnAFinishedContext(t *testing.T) {
	ctx := context.Background()
	p, payload, _ := postMergeEvidenceFixture(t, ctx)

	require.True(t, p.hasLocalPostMergeState(ctx, payload),
		"merged.mig is on disk, so a live context must find it")

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	require.False(t, p.hasLocalPostMergeState(cancelled, payload),
		"a shut-down node must not walk and load the task's shards")
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

	dirs := migrationDirsForPropertyIndex("title", "searchable")
	require.NotEmpty(t, dirs)
	trackerDir := filepath.Join(concrete.pathLSM(), ".migrations", dirs[0]+"_1")
	require.NoError(t, os.MkdirAll(trackerDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(trackerDir, "merged.mig"), nil, 0o600))

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

	var guided bool
	for _, e := range hook.AllEntries() {
		if _, ok := e.Data["repair_command"]; ok {
			guided = true
		}
	}
	require.True(t, guided,
		"merged.mig on this node is the only evidence of the tear; the guidance has to fire off it")
}
