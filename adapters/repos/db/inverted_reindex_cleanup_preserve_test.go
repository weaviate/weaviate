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
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// plantedTracker is one migration directory on disk plus, where the migration
// got that far, the record that says whose it is and how far it got.
type plantedTracker struct {
	dir  string
	prop string
	// state empty leaves the directory with no record, which is a migration
	// that never reached its first record write.
	state MigrationState
}

// TestCleanStaleMigrationDirsAt_PreservesCompletedGens pins the R2/R2b
// regression (#10675): the pre-submit sweep wiped a completed migration's
// directory out from under the in-memory bucket pointer, so the next
// migration picked the same generation and overwrote the previous data.
//
// What may be removed is decided by the migration's record: only a migration
// whose data is not yet committed leaves directories a sweep owns.
func TestCleanStaleMigrationDirsAt_PreservesCompletedGens(t *testing.T) {
	tests := []struct {
		name     string
		propName string
		idxType  string
		trackers []plantedTracker
		// wantSurvivors is what must still be on disk afterwards.
		wantSurvivors []string
	}{
		{
			name:     "a committed migration survives, an uncommitted one is removed",
			propName: "text",
			idxType:  "searchable",
			trackers: []plantedTracker{
				{dir: "searchable_retokenize_text_1", prop: "text", state: MigrationStateSwapped},
				{dir: "searchable_retokenize_text_2", prop: "text", state: MigrationStateIterating},
			},
			wantSurvivors: []string{"searchable_retokenize_text_1"},
		},
		{
			// Merged is where the staged data becomes the data, so it is the
			// earliest state a sweep may not touch.
			name:     "a merged migration survives",
			propName: "text",
			idxType:  "searchable",
			trackers: []plantedTracker{
				{dir: "searchable_retokenize_text_3", prop: "text", state: MigrationStateMerged},
			},
			wantSurvivors: []string{"searchable_retokenize_text_3"},
		},
		{
			name:          "a tracker with no record at all is removed",
			propName:      "text",
			idxType:       "searchable",
			trackers:      []plantedTracker{{dir: "searchable_retokenize_text_1", prop: "text"}},
			wantSurvivors: []string{},
		},
		{
			name:     "another property's tracker is not this sweep's",
			propName: "text",
			idxType:  "searchable",
			trackers: []plantedTracker{
				{dir: "searchable_retokenize_other_1", prop: "other", state: MigrationStateIterating},
				{dir: "searchable_retokenize_text_1", prop: "text", state: MigrationStateIterating},
			},
			wantSurvivors: []string{"searchable_retokenize_other_1"},
		},
		{
			name:     "another index type's tracker for the same property is not this sweep's",
			propName: "text",
			idxType:  "searchable",
			trackers: []plantedTracker{
				{dir: "filterable_retokenize_text_1", prop: "text", state: MigrationStateIterating},
				{dir: "searchable_retokenize_text_1", prop: "text", state: MigrationStateIterating},
			},
			wantSurvivors: []string{"filterable_retokenize_text_1"},
		},
		{
			// The R2 repro: two back-to-back migrations both completed, and
			// both still hold their data under their own generation's name.
			name:     "two committed generations both survive",
			propName: "text",
			idxType:  "searchable",
			trackers: []plantedTracker{
				{dir: "searchable_retokenize_text_1", prop: "text", state: MigrationStateSwapped},
				{dir: "searchable_retokenize_text_2", prop: "text", state: MigrationStateSwapped},
			},
			wantSurvivors: []string{
				"searchable_retokenize_text_1",
				"searchable_retokenize_text_2",
			},
		},
	}

	logger := logrus.New()
	logger.SetOutput(io.Discard)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lsm := t.TempDir()
			require.NoError(t, os.MkdirAll(filepath.Join(lsm, ".migrations"), 0o755))
			for _, tracker := range tc.trackers {
				mkTrackerDir(t, lsm, tracker.dir)
				if tracker.state == "" {
					continue
				}
				// Directory names are opaque to every reader of a record, so
				// the staged one only has to be this migration's own.
				mkMigrationRecord(t, lsm, tracker.dir, tracker.state,
					map[string]string{tracker.prop: "staged_" + tracker.dir})
			}

			cleanStaleMigrationDirsAt(t.Context(), lsm, tc.propName, tc.idxType, logger, nil)

			want := append([]string{}, tc.wantSurvivors...)
			sort.Strings(want)
			require.Equal(t, want, survivingTrackerDirs(t, lsm))
		})
	}
}
