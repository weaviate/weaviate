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

	"github.com/stretchr/testify/require"
)

// testQuestionRecords builds the same migration in each of the five states:
// two properties, both flipped once the flip is decided.
func testQuestionRecords() (iterating, iterated, merged, swapped, promoted MigrationRecord) {
	subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title", "body")
	flipped := []string{"title", "body"}
	displaced := map[string]string{"title": "property_title", "body": "property_body"}

	return NewMigrationRecordIterating(subject, MigrationCheckpoint{ProcessedCount: 10}),
		NewMigrationRecordIterated(subject),
		NewMigrationRecordMerged(subject),
		NewMigrationRecordSwapped(subject, flipped, displaced),
		NewMigrationRecordPromoted(subject, flipped, displaced)
}

// TestMigrationRecordQuestions is the RFC's first acceptance test at its
// re-derived target: every one of the 5x4 cells asserted directly on a
// record, with no reader re-interpreting a state at its call site.
func TestMigrationRecordQuestions(t *testing.T) {
	iterating, iterated, merged, swapped, promoted := testQuestionRecords()

	tests := []struct {
		name               string
		record             MigrationRecord
		wantState          MigrationState
		wantDataCommitted  bool
		wantPointerSwapped bool
		wantIterationDone  bool
		wantOwnsStagedDir  bool
	}{
		{
			name:               "iterating: rebuilding into staging, canonical still primary",
			record:             iterating,
			wantState:          MigrationStateIterating,
			wantDataCommitted:  false,
			wantPointerSwapped: false,
			wantIterationDone:  false,
			wantOwnsStagedDir:  true,
		},
		{
			name:               "iterated: rebuild durable, still discardable",
			record:             iterated,
			wantState:          MigrationStateIterated,
			wantDataCommitted:  false,
			wantPointerSwapped: false,
			wantIterationDone:  true,
			wantOwnsStagedDir:  true,
		},
		{
			name:               "merged: the committed boolean flips, the flip decision has not",
			record:             merged,
			wantState:          MigrationStateMerged,
			wantDataCommitted:  true,
			wantPointerSwapped: false,
			wantIterationDone:  true,
			wantOwnsStagedDir:  true,
		},
		{
			name:               "swapped: the flip decision is durable, so the migration is irreversible",
			record:             swapped,
			wantState:          MigrationStateSwapped,
			wantDataCommitted:  true,
			wantPointerSwapped: true,
			wantIterationDone:  true,
			wantOwnsStagedDir:  true,
		},
		{
			name:               "promoted: committed and swapped, same answers as swapped",
			record:             promoted,
			wantState:          MigrationStatePromoted,
			wantDataCommitted:  true,
			wantPointerSwapped: true,
			wantIterationDone:  true,
			wantOwnsStagedDir:  true,
		},
	}

	require.Len(t, tests, 5, "the representation admits five states and the machine reaches five")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.wantState, tt.record.State())
			require.Equal(t, tt.wantDataCommitted, tt.record.DataCommitted())
			require.Equal(t, tt.wantPointerSwapped, tt.record.PointerSwapped())
			require.Equal(t, tt.wantIterationDone, tt.record.IterationComplete())
			require.Equal(t, tt.wantOwnsStagedDir, tt.record.OwnsBucket("m_42_title"))
		})
	}
}

func TestMigrationRecordStateSet(t *testing.T) {
	iterating, iterated, merged, swapped, promoted := testQuestionRecords()

	var reached []MigrationState
	for _, rec := range []MigrationRecord{iterating, iterated, merged, swapped, promoted} {
		require.NotContains(t, reached, rec.State(), "two variants report the same state")
		reached = append(reached, rec.State())
	}

	require.ElementsMatch(t, []MigrationState{
		MigrationStateIterating, MigrationStateIterated, MigrationStateMerged,
		MigrationStateSwapped, MigrationStatePromoted,
	}, reached)
}

func TestMigrationRecordOwnsBucket(t *testing.T) {
	_, _, merged, _, _ := testQuestionRecords()

	tests := []struct {
		name string
		dir  string
		want bool
	}{
		{name: "a staged directory of a covered property", dir: "m_42_title", want: true},
		{name: "a staged directory of the other covered property", dir: "m_42_body", want: true},
		{name: "a sidecar directory the migration created", dir: "m_42_sidecar", want: true},
		{
			name: "the canonical directory, which predates the migration and must never be reclaimed by it",
			dir:  "property_title", want: false,
		},
		{name: "another migration's directory", dir: "m_43_title", want: false},
		{name: "no directory at all", dir: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, merged.OwnsBucket(tt.dir))
		})
	}
}
