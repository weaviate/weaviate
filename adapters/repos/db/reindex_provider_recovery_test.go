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
	"time"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// TestIsSemanticMigration pins the semantic/format-only classification
// (weaviate/0-weaviate-issues#254 promoted change-algorithm to semantic).
func TestIsSemanticMigration(t *testing.T) {
	semantic := []ReindexMigrationType{
		ReindexTypeChangeTokenization,
		ReindexTypeChangeTokenizationFilterable,
		ReindexTypeEnableFilterable,
		ReindexTypeEnableSearchable,
		ReindexTypeEnableRangeable,
		ReindexTypeChangeAlgorithm,
	}
	formatOnly := []ReindexMigrationType{
		ReindexTypeRebuildSearchable,
		ReindexTypeRepairFilterable,
		ReindexTypeRepairRangeable,
	}
	for _, mt := range semantic {
		t.Run(string(mt)+" → semantic", func(t *testing.T) {
			require.True(t, IsSemanticMigration(mt))
		})
	}
	for _, mt := range formatOnly {
		t.Run(string(mt)+" → format-only", func(t *testing.T) {
			require.False(t, IsSemanticMigration(mt))
		})
	}
}

func TestBuildRecoveryTasksStampsTheIdentity(t *testing.T) {
	require.Len(t, allKnownMigrationTypes(), len(dirOwnershipCases()),
		"a new migration type belongs in this table")

	p, _ := newTestProvider(t)
	logger, _ := logrustest.NewNullLogger()
	desc := taskDescAt(11)

	for _, tc := range dirOwnershipCases() {
		minted, err := p.createReindexTasks(desc, dirOwnershipUnit, tc.payload())
		require.NoError(t, err)
		for _, original := range minted {
			subject := original.migrationSubject(nil, []string{dirOwnershipProp}, time.Time{})
			t.Run(tc.name+"/"+string(subject.Key.StrategyCode), func(t *testing.T) {
				tasks, err := buildRecoveryTasks(subject, "shard-1", logger, nil)
				require.NoError(t, err)
				require.Len(t, tasks, 1)

				require.Equal(t, subject.Key, tasks[0].migrationRecordKey())
				handles := subject.Props[dirOwnershipProp]
				require.Equal(t, []string{handles.Sidecar, handles.Staged}, workingCopyDirs(tasks))
			})
		}
	}
}
