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
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	entschema "github.com/weaviate/weaviate/entities/schema"
)

// TestLogOperatorRepairGuidanceOnTornSemanticMigration_* pin the
// operator-actionable-error half of #221: when a semantic-migration
// task reaches a terminal status (FAILED or CANCELLED), OnTaskCompleted
// logs the exact REST command an operator should issue to repair the
// partial-completion bucket↔schema inversion.
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
	// The repair re-submits the migration, not a bare rebuild — the schema
	// flip was skipped, so searchable.rebuild would 400 on the stale bit.
	require.Equal(t,
		`PUT /v1/schema/Products/indexes/name {"searchable":{"tokenization":"field"}}`,
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
		`PUT /v1/schema/Products/indexes/category {"filterable":{"tokenization":"field"}}`,
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
		// Needed for a repair command to be renderable at all; without it the
		// guidance still fires but carries no repair_command field, which is
		// what TestLogOperatorRepairGuidanceOnTornSemanticMigration_NoTarget-
		// TokenizationStillWarns pins.
		TargetTokenization: "field",
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

			require.Equal(t, tc.wantGuidance, loggedRepairGuidance(hook),
				"repair guidance on a CANCELLED task must follow the swap evidence")
		})
	}
}

// The repair command must be callable by the only person who reads the
// server log: an operator with cluster-wide reach, who has to type the
// namespace prefix for the request to land on the right collection.
func TestLogOperatorRepairGuidanceOnTornSemanticMigration_QualifiedCollectionKeepsItsPrefix(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()

	payload := &ReindexTaskPayload{
		Collection:         "customer1:Products",
		MigrationType:      ReindexTypeChangeTokenization,
		Properties:         []string{"name"},
		TargetTokenization: "field",
	}
	logOperatorRepairGuidanceOnPartialSwap(logger.WithField("taskID", "T5"), payload, distributedtask.TaskStatusFailed)

	require.Len(t, hook.Entries, 1)
	require.Equal(t,
		`PUT /v1/schema/customer1:Products/indexes/name {"searchable":{"tokenization":"field"}}`,
		hook.Entries[0].Data["repair_command"])
}

// CANCELLED reaches the same bucket↔schema end state as FAILED on a node
// that already committed its swap, so it gets the same guidance.
func TestLogOperatorRepairGuidanceOnTornSemanticMigration_OutcomeAppearsInMessage(t *testing.T) {
	for _, outcome := range []distributedtask.TaskStatus{
		distributedtask.TaskStatusFailed, distributedtask.TaskStatusCancelled,
	} {
		t.Run(string(outcome), func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()
			payload := &ReindexTaskPayload{
				Collection:         "Products",
				MigrationType:      ReindexTypeChangeTokenization,
				Properties:         []string{"name"},
				TargetTokenization: "field",
			}
			logOperatorRepairGuidanceOnPartialSwap(
				logger.WithField("taskID", "T6"), payload, outcome)

			require.Len(t, hook.Entries, 1)
			require.Contains(t, hook.Entries[0].Message, string(outcome))
		})
	}
}

// A tokenization change submitted by an older binary carries no target, so
// no repair command can be rendered. The operator still has to hear that the
// buckets may be inverted — a silent terminal is the worse failure.
func TestLogOperatorRepairGuidanceOnTornSemanticMigration_NoTargetTokenizationStillWarns(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()

	payload := &ReindexTaskPayload{
		Collection:    "Products",
		MigrationType: ReindexTypeChangeTokenization,
		Properties:    []string{"name"},
	}
	logOperatorRepairGuidanceOnPartialSwap(logger.WithField("taskID", "T7"), payload, distributedtask.TaskStatusFailed)

	require.Len(t, hook.Entries, 1)
	require.NotContains(t, hook.Entries[0].Data, "repair_command")
	require.Contains(t, hook.Entries[0].Message, "cannot name the request")
}

// postMergeTrackerDir is the tracker dir name a searchable migration of
// propName leaves behind, generation suffix included.
func postMergeTrackerDir(t *testing.T, propName string) string {
	t.Helper()
	prefixes := migrationDirPrefixesForIndexType("searchable")
	require.NotEmpty(t, prefixes)
	return migrationDirWithProps(prefixes[0], []string{propName}) + "_1"
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
		// Needed for a repair command to be renderable at all; without it the
		// guidance still fires but carries no repair_command field, which is
		// what TestLogOperatorRepairGuidanceOnTornSemanticMigration_NoTarget-
		// TokenizationStillWarns pins.
		TargetTokenization: "field",
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
		// Needed for a repair command to be renderable at all; without it the
		// guidance still fires but carries no repair_command field, which is
		// what TestLogOperatorRepairGuidanceOnTornSemanticMigration_NoTarget-
		// TokenizationStillWarns pins.
		TargetTokenization: "field",
	})
	require.NoError(t, err)

	// A server context that expires shortly: the drain's bounded child
	// inherits it, so the wait ends on that deadline instead of after
	// reindexTerminalCleanupDrainTimeout. The deadline is ahead rather than
	// behind so the probe, which runs before the drain and is bounded off
	// the same context, still gets to read the disk — that ordering is what
	// this test pins.
	expired, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
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
