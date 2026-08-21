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
		wantLiveDataAt     string
		wantOwnsStagedDir  bool
	}{
		{
			name:               "iterating: rebuilding into staging, canonical still primary",
			record:             iterating,
			wantState:          MigrationStateIterating,
			wantDataCommitted:  false,
			wantPointerSwapped: false,
			wantLiveDataAt:     "property_title",
			wantOwnsStagedDir:  true,
		},
		{
			name:               "iterated: rebuild durable, still discardable",
			record:             iterated,
			wantState:          MigrationStateIterated,
			wantDataCommitted:  false,
			wantPointerSwapped: false,
			wantLiveDataAt:     "property_title",
			wantOwnsStagedDir:  true,
		},
		{
			name:               "merged: the committed boolean flips, the flip decision has not",
			record:             merged,
			wantState:          MigrationStateMerged,
			wantDataCommitted:  true,
			wantPointerSwapped: false,
			wantLiveDataAt:     "property_title",
			wantOwnsStagedDir:  true,
		},
		{
			name:               "swapped: the staged directory is serving the property",
			record:             swapped,
			wantState:          MigrationStateSwapped,
			wantDataCommitted:  true,
			wantPointerSwapped: true,
			wantLiveDataAt:     "m_42_title",
			wantOwnsStagedDir:  true,
		},
		{
			name:               "promoted: the data is back at the canonical name",
			record:             promoted,
			wantState:          MigrationStatePromoted,
			wantDataCommitted:  true,
			wantPointerSwapped: true,
			wantLiveDataAt:     "property_title",
			wantOwnsStagedDir:  true,
		},
	}

	require.Len(t, tests, 5, "the representation admits five states and the machine reaches five")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.wantState, tt.record.State())
			require.Equal(t, tt.wantDataCommitted, tt.record.DataCommitted())
			require.Equal(t, tt.wantPointerSwapped, tt.record.PointerSwapped())
			require.Equal(t, tt.wantLiveDataAt, tt.record.LiveDataAt("title"))
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

func TestMigrationRecordLiveDataAt(t *testing.T) {
	_, _, _, swapped, _ := testQuestionRecords()
	swappedRec := swapped.(MigrationRecordSwapped)

	tests := []struct {
		name    string
		record  MigrationRecord
		prop    string
		wantDir string
	}{
		{
			name:    "outside the window a loaded record reports every recorded flip as done",
			record:  swappedRec,
			prop:    "title",
			wantDir: "m_42_title",
		},
		{
			name:    "inside the window, before any property flips, the canonical bucket is still serving",
			record:  swappedRec.EnterFlipWindow(),
			prop:    "title",
			wantDir: "property_title",
		},
		{
			name:    "inside the window, a property that has flipped is served from staging",
			record:  swappedRec.EnterFlipWindow().WithPropertyFlipped("title"),
			prop:    "title",
			wantDir: "m_42_title",
		},
		{
			name:    "inside the window, a property that has not flipped yet is not dragged along",
			record:  swappedRec.EnterFlipWindow().WithPropertyFlipped("title"),
			prop:    "body",
			wantDir: "property_body",
		},
		{
			name:    "a property this migration does not cover",
			record:  swappedRec,
			prop:    "author",
			wantDir: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.wantDir, tt.record.LiveDataAt(tt.prop))
		})
	}
}

func TestMigrationRecordFlipWindowDoesNotLeakToDisk(t *testing.T) {
	_, _, _, swapped, _ := testQuestionRecords()
	inWindow := swapped.(MigrationRecordSwapped).EnterFlipWindow().WithPropertyFlipped("title")

	encoded, err := encodeMigrationRecord(inWindow)
	require.NoError(t, err)
	decoded, err := decodeMigrationRecord(encoded)
	require.NoError(t, err)

	// A partial flip set is not a state any load can observe, so the record
	// that comes back reports both properties flipped.
	require.Equal(t, "m_42_title", decoded.LiveDataAt("title"))
	require.Equal(t, "m_42_body", decoded.LiveDataAt("body"))
}
