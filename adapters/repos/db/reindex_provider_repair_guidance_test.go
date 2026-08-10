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
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// TestLogOperatorRepairGuidanceOnTerminalSemanticMigration_* pin the
// operator-actionable-error half of #221: when a semantic-migration
// task transitions to FAILED, OnTaskCompleted logs the exact REST
// command an operator should issue to repair the partial-completion
// bucket↔schema inversion.
//
// We assert on the log entry's structured fields (so the message text
// can drift without breaking the test) and on the embedded
// repair_command field (so the operator's copy-pasteable command stays
// stable).

func TestLogOperatorRepairGuidanceOnTerminalSemanticMigration_ChangeTokenizationBothIndexes(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()

	payload := &ReindexTaskPayload{
		Collection:         "Products",
		MigrationType:      ReindexTypeChangeTokenization,
		Properties:         []string{"name"},
		TargetTokenization: "field",
	}
	logOperatorRepairGuidanceOnTerminalSemanticMigration(logger.WithField("taskID", "T1"), payload, "FAILED")

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

func TestLogOperatorRepairGuidanceOnTerminalSemanticMigration_ChangeTokenizationFilterableOnly(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()

	payload := &ReindexTaskPayload{
		Collection:         "Products",
		MigrationType:      ReindexTypeChangeTokenizationFilterable,
		Properties:         []string{"category"},
		TargetTokenization: "field",
	}
	logOperatorRepairGuidanceOnTerminalSemanticMigration(logger.WithField("taskID", "T2"), payload, "FAILED")

	require.Len(t, hook.Entries, 1)
	entry := hook.Entries[0]
	// change-tokenization-filterable touches ONLY the filterable bucket;
	// guidance must scope to that.
	require.Equal(t,
		`PUT /v1/schema/Products/indexes/category {"filterable":{"rebuild":true}}`,
		entry.Data["repair_command"])
}

func TestLogOperatorRepairGuidanceOnTerminalSemanticMigration_MultipleProperties(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()

	payload := &ReindexTaskPayload{
		Collection:    "Products",
		MigrationType: ReindexTypeEnableFilterable,
		Properties:    []string{"a", "b", "c"},
	}
	logOperatorRepairGuidanceOnTerminalSemanticMigration(logger.WithField("taskID", "T3"), payload, "FAILED")

	// One entry per property — easier for log scrapers to alert per-prop.
	require.Len(t, hook.Entries, 3)
	gotProps := make([]string, len(hook.Entries))
	for i, entry := range hook.Entries {
		gotProps[i] = entry.Data["property"].(string)
	}
	require.ElementsMatch(t, []string{"a", "b", "c"}, gotProps)
}

func TestLogOperatorRepairGuidanceOnTerminalSemanticMigration_FormatOnlyMigrationIsNoOp(t *testing.T) {
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
			logOperatorRepairGuidanceOnTerminalSemanticMigration(logger.WithField("taskID", "T"), payload, "FAILED")
			require.Empty(t, hook.Entries,
				"format-only migration %s must not produce repair guidance", mt)
		})
	}
}

func TestLogOperatorRepairGuidanceOnTerminalSemanticMigration_EmptyPropertiesEmitsGenericGuidance(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()

	payload := &ReindexTaskPayload{
		Collection:    "Products",
		MigrationType: ReindexTypeChangeTokenization,
		Properties:    nil, // reserved for future whole-collection rebuild
	}
	logOperatorRepairGuidanceOnTerminalSemanticMigration(logger.WithField("taskID", "T4"), payload, "FAILED")

	require.Len(t, hook.Entries, 1, "empty Properties → one generic guidance entry")
	require.Contains(t, hook.Entries[0].Message, "empty Properties")
}

// The repair command must be callable. Collection is stored
// namespace-qualified, and PUT /v1/schema/{className}/... rejects a
// qualified name with a 400 for namespace-confined callers.
func TestLogOperatorRepairGuidanceOnTerminalSemanticMigration_QualifiedCollectionRendersShort(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()

	payload := &ReindexTaskPayload{
		Collection:    "customer1:Products",
		MigrationType: ReindexTypeChangeTokenization,
		Properties:    []string{"name"},
	}
	logOperatorRepairGuidanceOnTerminalSemanticMigration(logger.WithField("taskID", "T5"), payload, "FAILED")

	require.Len(t, hook.Entries, 1)
	require.Equal(t,
		`PUT /v1/schema/Products/indexes/name {"filterable":{"rebuild":true},"searchable":{"rebuild":true}}`,
		hook.Entries[0].Data["repair_command"])
}

// CANCELLED reaches the same bucket↔schema end state as FAILED on a node
// that already committed its swap, so it gets the same guidance.
func TestLogOperatorRepairGuidanceOnTerminalSemanticMigration_OutcomeAppearsInMessage(t *testing.T) {
	for _, outcome := range []string{"FAILED", "CANCELLED"} {
		t.Run(outcome, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()
			payload := &ReindexTaskPayload{
				Collection:    "Products",
				MigrationType: ReindexTypeChangeTokenization,
				Properties:    []string{"name"},
			}
			logOperatorRepairGuidanceOnTerminalSemanticMigration(
				logger.WithField("taskID", "T6"), payload, outcome)

			require.Len(t, hook.Entries, 1)
			require.Contains(t, hook.Entries[0].Message, outcome)
		})
	}
}
