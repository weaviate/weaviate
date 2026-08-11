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

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/distributedtask"
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
	logOperatorRepairGuidanceOnTornSemanticMigration(logger.WithField("taskID", "T1"), payload, distributedtask.TaskStatusFailed)

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
	logOperatorRepairGuidanceOnTornSemanticMigration(logger.WithField("taskID", "T2"), payload, distributedtask.TaskStatusFailed)

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
	logOperatorRepairGuidanceOnTornSemanticMigration(logger.WithField("taskID", "T3"), payload, distributedtask.TaskStatusFailed)

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
			logOperatorRepairGuidanceOnTornSemanticMigration(logger.WithField("taskID", "T"), payload, distributedtask.TaskStatusFailed)
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
	logOperatorRepairGuidanceOnTornSemanticMigration(logger.WithField("taskID", "T4"), payload, distributedtask.TaskStatusFailed)

	require.Len(t, hook.Entries, 1, "empty Properties → one generic guidance entry")
	require.Contains(t, hook.Entries[0].Message, "empty Properties")
}

// Pins: repair guidance on a CANCELLED task fires only when a
// PostCompletionAck proves a node ran its swap.
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

	for _, tc := range []struct {
		name        string
		acks        map[string]distributedtask.PostCompletionAck
		wantGuiance bool
	}{
		{name: "no node acked a swap", acks: nil, wantGuiance: false},
		{
			name:        "one node acked a swap",
			acks:        map[string]distributedtask.PostCompletionAck{"n1": {Success: true}},
			wantGuiance: true,
		},
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
				Namespace:          ReindexNamespace,
				TaskDescriptor:     distributedtask.TaskDescriptor{ID: "T_cancel", Version: 1},
				Status:             distributedtask.TaskStatusCancelled,
				Payload:            payload,
				PostCompletionAcks: tc.acks,
			}))

			var guided bool
			for _, e := range hook.AllEntries() {
				if _, ok := e.Data["repair_command"]; ok {
					guided = true
				}
			}
			require.Equal(t, tc.wantGuiance, guided,
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
	logOperatorRepairGuidanceOnTornSemanticMigration(logger.WithField("taskID", "T5"), payload, distributedtask.TaskStatusFailed)

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
			logOperatorRepairGuidanceOnTornSemanticMigration(
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
	logOperatorRepairGuidanceOnTornSemanticMigration(logger.WithField("taskID", "T7"), payload, distributedtask.TaskStatusFailed)

	require.Len(t, hook.Entries, 1)
	require.NotContains(t, hook.Entries[0].Data, "repair_command")
	require.Contains(t, hook.Entries[0].Message, "cannot name the request")
}
