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

type plantedTracker struct {
	dir   string
	prop  string
	state MigrationState
}

func TestCleanStaleMigrationDirsAt_PreservesCompletedGens(t *testing.T) {
	tests := []struct {
		name          string
		propName      string
		idxType       string
		trackers      []plantedTracker
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
				mkMigrationRecord(t, lsm, tracker.dir, tracker.state,
					map[string]string{tracker.prop: "property_" + tracker.prop + "__" + tracker.dir + "_ingest"})
			}

			cleanStaleMigrationDirsAt(t.Context(), lsm, tc.propName, tc.idxType, logger, nil)

			want := append([]string{}, tc.wantSurvivors...)
			sort.Strings(want)
			require.Equal(t, want, survivingTrackerDirs(t, lsm))
		})
	}
}

func sweepSurvivors(names []string, committed migrationPreservedState, inScope func(string) bool) []string {
	survivors := []string{}
	for _, name := range names {
		if !inScope(name) || committed.preservesTracker(name) {
			survivors = append(survivors, name)
		}
	}
	sort.Strings(survivors)
	return survivors
}

func survivingTrackerDirs(t *testing.T, lsm string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(lsm, ".migrations"))
	require.NoError(t, err)
	names := []string{}
	for _, e := range entries {
		if e.IsDir() && e.Name() != migrationRecordsDirName {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names
}
