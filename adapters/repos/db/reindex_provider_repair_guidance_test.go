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

// TestLogOperatorRepairGuidanceOnFailedSemanticMigration_* pin the
// operator-actionable-error half of #221: when a semantic-migration
// task transitions to FAILED, OnTaskCompleted logs the exact REST
// command an operator should issue to repair the partial-completion
// bucket↔schema inversion.
//
// We assert on the log entry's structured fields (so the message text
// can drift without breaking the test) and on the embedded
// repair_command field (so the operator's copy-pasteable command stays
// stable).

func TestLogOperatorRepairGuidanceOnFailedSemanticMigration_ChangeTokenizationBothIndexes(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()

	payload := &ReindexTaskPayload{
		Collection:         "Products",
		MigrationType:      ReindexTypeChangeTokenization,
		Properties:         []string{"name"},
		TargetTokenization: "field",
	}
	logOperatorRepairGuidanceOnFailedSemanticMigration(logger.WithField("taskID", "T1"), payload)

	require.Len(t, hook.Entries, 1, "expected one error entry per property")
	entry := hook.Entries[0]
	require.Equal(t, logrus.ErrorLevel, entry.Level)
	require.Equal(t, "name", entry.Data["property"])
	require.Equal(t, ReindexTypeChangeTokenization, entry.Data["migration_type"])
	// change-tokenization can tear either inverted index; guidance must
	// instruct the operator to rebuild both via the GA rebuild route.
	require.Equal(t,
		`POST /v1/schema/Products/properties/name/index/filterable/rebuild && POST /v1/schema/Products/properties/name/index/searchable/rebuild`,
		entry.Data["repair_command"])
	require.Contains(t, entry.Message, "FAILED")
	require.Contains(t, entry.Message, "bucket")
}

func TestLogOperatorRepairGuidanceOnFailedSemanticMigration_ChangeTokenizationFilterableOnly(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()

	payload := &ReindexTaskPayload{
		Collection:         "Products",
		MigrationType:      ReindexTypeChangeTokenizationFilterable,
		Properties:         []string{"category"},
		TargetTokenization: "field",
	}
	logOperatorRepairGuidanceOnFailedSemanticMigration(logger.WithField("taskID", "T2"), payload)

	require.Len(t, hook.Entries, 1)
	entry := hook.Entries[0]
	// change-tokenization-filterable touches ONLY the filterable bucket;
	// guidance must scope to that.
	require.Equal(t,
		`POST /v1/schema/Products/properties/category/index/filterable/rebuild`,
		entry.Data["repair_command"])
}

func TestLogOperatorRepairGuidanceOnFailedSemanticMigration_MultipleProperties(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()

	payload := &ReindexTaskPayload{
		Collection:    "Products",
		MigrationType: ReindexTypeEnableFilterable,
		Properties:    []string{"a", "b", "c"},
	}
	logOperatorRepairGuidanceOnFailedSemanticMigration(logger.WithField("taskID", "T3"), payload)

	// One entry per property — easier for log scrapers to alert per-prop.
	require.Len(t, hook.Entries, 3)
	gotProps := make([]string, len(hook.Entries))
	for i, entry := range hook.Entries {
		gotProps[i] = entry.Data["property"].(string)
	}
	require.ElementsMatch(t, []string{"a", "b", "c"}, gotProps)
}

// TestRepairCommandsForFailedMigration_EnableAndAlgorithmUsePut pins that
// enable-*/change-algorithm emit the re-run PUT (a /rebuild would 400: no
// index, or still WAND). Retokenize migrations keep /rebuild.
func TestRepairCommandsForFailedMigration_EnableAndAlgorithmUsePut(t *testing.T) {
	cases := []struct {
		name        string
		payload     *ReindexTaskPayload
		wantCommand string
	}{
		{
			name: "enable-searchable -> PUT re-enable with target tokenization",
			payload: &ReindexTaskPayload{
				Collection: "Products", MigrationType: ReindexTypeEnableSearchable,
				Properties: []string{"name"}, TargetTokenization: "word",
			},
			wantCommand: `PUT /v1/schema/Products/properties/name/index/searchable -d '{"tokenization":"word"}'`,
		},
		{
			name: "enable-filterable -> PUT re-enable with empty body",
			payload: &ReindexTaskPayload{
				Collection: "Products", MigrationType: ReindexTypeEnableFilterable,
				Properties: []string{"name"},
			},
			wantCommand: `PUT /v1/schema/Products/properties/name/index/filterable -d '{}'`,
		},
		{
			name: "change-algorithm -> PUT re-run with algorithm body",
			payload: &ReindexTaskPayload{
				Collection: "Products", MigrationType: ReindexTypeChangeAlgorithm,
				Properties: []string{"name"},
			},
			wantCommand: `PUT /v1/schema/Products/properties/name/index/searchable -d '{"algorithm":"blockmax"}'`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()
			logOperatorRepairGuidanceOnFailedSemanticMigration(logger.WithField("taskID", "T"), tc.payload)
			require.Len(t, hook.Entries, 1)
			got := hook.Entries[0].Data["repair_command"].(string)
			require.Equal(t, tc.wantCommand, got)
			require.NotContains(t, got, "/rebuild",
				"enable-*/change-algorithm recovery must not use /rebuild (it 400s on the reverted flag)")
		})
	}
}

func TestLogOperatorRepairGuidanceOnFailedSemanticMigration_FormatOnlyMigrationIsNoOp(t *testing.T) {
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
			logOperatorRepairGuidanceOnFailedSemanticMigration(logger.WithField("taskID", "T"), payload)
			require.Empty(t, hook.Entries,
				"format-only migration %s must not produce repair guidance", mt)
		})
	}
}

func TestLogOperatorRepairGuidanceOnFailedSemanticMigration_EmptyPropertiesEmitsGenericGuidance(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()

	payload := &ReindexTaskPayload{
		Collection:    "Products",
		MigrationType: ReindexTypeChangeTokenization,
		Properties:    nil, // reserved for future whole-collection rebuild
	}
	logOperatorRepairGuidanceOnFailedSemanticMigration(logger.WithField("taskID", "T4"), payload)

	require.Len(t, hook.Entries, 1, "empty Properties → one generic guidance entry")
	require.Contains(t, hook.Entries[0].Message, "empty Properties")
}
