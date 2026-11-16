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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
)

func TestALoadFinishesAHalfWrittenSwapDecision(t *testing.T) {
	decided := []MigrationState{MigrationStatePromoted, MigrationStatePromoted}
	tests := []struct {
		name       string
		decided    int
		stampOther bool
		wantCounts [3]int
		wantStates []MigrationState
	}{
		{name: "the searchable half is decided", decided: 0, wantCounts: [3]int{3, 3, 3}, wantStates: decided},
		{name: "the filterable half is decided", decided: 1, wantCounts: [3]int{3, 3, 3}, wantStates: decided},
		{
			// A half that missed writes is never promoted; the operator repair is the remedy.
			name: "the undecided half missed writes", decided: 0, stampOther: true,
			wantCounts: [3]int{0, 0, 3},
			wantStates: []MigrationState{MigrationStatePromoted, MigrationStateMerged},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newTokenizationChange(t)
			f.decide(t, f.tasks[tc.decided])
			if tc.stampOther {
				rec, ok := f.tasks[1-tc.decided].migrationRecord(f.shard)
				require.True(t, ok)
				stamped, ok := migrationRecordStampedUnmirrored(rec)
				require.True(t, ok)
				require.NoError(t, f.shard.migrationRecords.Put(stamped))
			}

			f.reload(t)
			assert.Equal(t, models.PropertyTokenizationWord, f.tokenization())
			assert.Equal(t, tc.wantCounts, f.counts(t), "Equal, Like and BM25 before any swap tick")
			assert.Equal(t, tc.wantStates, f.states(t))
			if tc.stampOther {
				return
			}

			require.Empty(t, f.swap(f.freshTasks(t)).Errs)
			assert.Equal(t, tc.wantCounts, f.counts(t))
		})
	}
}
