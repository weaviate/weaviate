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

func testQuestionRecords() (iterating, iterated, merged, swapped, promoted MigrationRecord) {
	subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title", "body")
	flipped := []string{"title", "body"}
	displaced := map[string]string{"title": "property_title_searchable", "body": "property_body_searchable"}

	return NewMigrationRecordIterating(subject, MigrationCheckpoint{LastProcessedKey: []byte("halfway")}),
		NewMigrationRecordIterated(subject),
		NewMigrationRecordMerged(subject),
		NewMigrationRecordSwapped(subject, flipped, displaced),
		NewMigrationRecordPromoted(subject, flipped, displaced)
}

func TestMigrationRecordQuestions(t *testing.T) {
	iterating, iterated, merged, swapped, promoted := testQuestionRecords()

	tests := []struct {
		name               string
		record             MigrationRecord
		wantState          MigrationState
		wantStagedComplete bool
		wantFlipDecided    bool
		wantIterationDone  bool
	}{
		{
			name:               "iterating: rebuilding into staging, canonical still primary",
			record:             iterating,
			wantState:          MigrationStateIterating,
			wantStagedComplete: false,
			wantFlipDecided:    false,
			wantIterationDone:  false,
		},
		{
			name:               "iterated: rebuild durable, still discardable",
			record:             iterated,
			wantState:          MigrationStateIterated,
			wantStagedComplete: false,
			wantFlipDecided:    false,
			wantIterationDone:  true,
		},
		{
			name:               "merged: the staged data is complete, the flip decision has not been made",
			record:             merged,
			wantState:          MigrationStateMerged,
			wantStagedComplete: true,
			wantFlipDecided:    false,
			wantIterationDone:  true,
		},
		{
			name:               "swapped: the flip decision is durable, so the migration is irreversible",
			record:             swapped,
			wantState:          MigrationStateSwapped,
			wantStagedComplete: true,
			wantFlipDecided:    true,
			wantIterationDone:  true,
		},
		{
			name:               "promoted: committed and swapped, same answers as swapped",
			record:             promoted,
			wantState:          MigrationStatePromoted,
			wantStagedComplete: true,
			wantFlipDecided:    true,
			wantIterationDone:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.wantState, tt.record.State())
			require.Equal(t, tt.wantStagedComplete, tt.record.StagedDataComplete())
			require.Equal(t, tt.wantFlipDecided, tt.record.FlipDecided())
			require.Equal(t, tt.wantIterationDone, tt.record.IterationComplete())
		})
	}
}

func TestMigrationRecordOwnsBucket(t *testing.T) {
	_, _, merged, _, _ := testQuestionRecords()

	tests := []struct {
		name string
		dir  string
		want bool
	}{
		{name: "a staged directory of a covered property", dir: "property_title__g42_ingest", want: true},
		{name: "a staged directory of the other covered property", dir: "property_body__g42_ingest", want: true},
		{name: "a sidecar directory the migration created", dir: "property_title__s42_reindex", want: true},
		{
			name: "the canonical directory, which predates the migration and must never be reclaimed by it",
			dir:  "property_title_searchable", want: false,
		},
		{name: "another migration's directory", dir: "property_title__g43_ingest", want: false},
		{name: "no directory at all", dir: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, merged.OwnsBucket(tt.dir))
		})
	}
}
