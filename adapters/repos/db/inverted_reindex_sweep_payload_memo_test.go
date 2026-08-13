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
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// memoFixture is one tracker dir of the sweep-memo fixtures. A multi-property
// name is what forces the payload read: no name shortcut can answer it.
type memoFixture struct {
	dir       string
	props     []string
	sentinels []string
}

var sweepMemoFixtures = []memoFixture{
	{
		dir:       "enable_filterable_cat_dog_1",
		props:     []string{"cat", "dog"},
		sentinels: []string{"started.mig", "tidied.mig"},
	},
	{
		dir:       "enable_filterable_cat_dog_2",
		props:     []string{"cat", "dog"},
		sentinels: []string{"started.mig"},
	},
	{
		dir:       "filterable_retokenize_bird_cat_1",
		props:     []string{"bird_cat"},
		sentinels: []string{"started.mig"},
	},
	{
		// Another property's tracker: settled by name, so never read.
		dir:       "enable_filterable_dog_1",
		props:     []string{"dog"},
		sentinels: []string{"started.mig"},
	},
	{
		// No strategy prefix of this index type: settled by name too.
		dir:       "searchable_retokenize_cat_1",
		props:     []string{"cat"},
		sentinels: []string{"started.mig"},
	},
}

// payloadReadingFixtures is how many fixtures the sweep has to open a payload
// for, i.e. the N of the 2N→N claim.
const payloadReadingFixtures = 3

func writeSweepMemoFixtures(t *testing.T) string {
	t.Helper()
	lsm := t.TempDir()
	for _, f := range sweepMemoFixtures {
		mkTrackerDir(t, lsm, f.dir, f.sentinels...)
		mkRecoveryPayload(t, lsm, f.dir, f.props...)
	}
	return lsm
}

// TestSweepSharesOnePayloadMemoAcrossItsPasses pins that the preserve pass and
// the deletion loop of one DELETE-path sweep read each tracker payload once
// between them, not once each.
func TestSweepSharesOnePayloadMemoAcrossItsPasses(t *testing.T) {
	lsm := writeSweepMemoFixtures(t)
	logger, _ := test.NewNullLogger()

	// Each pass on its own memo is what the sweep cost before they shared one.
	preservePass := &taskPropsCache{}
	completedMigrationGens(migrationDirsOf(lsm, nil, "cat", "filterable").
		cachingProps(preservePass))
	deletionPass := &taskPropsCache{}
	deletionScope := migrationDirsOf(lsm, nil, "cat", "filterable").cachingProps(deletionPass)
	for _, f := range sweepMemoFixtures {
		deletionScope.matches(f.dir)
	}
	require.Equal(t, payloadReadingFixtures, preservePass.count(),
		"preserve pass on its own memo")
	require.Equal(t, payloadReadingFixtures, deletionPass.count(),
		"deletion pass on its own memo")
	require.Equal(t, 2*payloadReadingFixtures, preservePass.count()+deletionPass.count(),
		"unshared memos read every payload twice")

	require.Equal(t, payloadReadingFixtures,
		cleanStaleMigrationDirsAt(lsm, "cat", "filterable", logger),
		"one sweep reads each payload once")
}

// TestDeleteSweepReportsItsPayloadReads pins that the DELETE path's read count
// reaches the operator, which is the only place the sweep's cost is visible.
func TestDeleteSweepReportsItsPayloadReads(t *testing.T) {
	ctx := testCtx()
	className := "DeleteSweepReads_" + uuid.NewString()[:8]
	hookLogger, hook := test.NewNullLogger()
	shd, _ := testShardWithSettings(t, ctx, newTestClassWithProps(className, []string{"cat", "dog"}),
		enthnsw.UserConfig{Skip: true}, false, false, false,
		func(i *Index) { i.logger = hookLogger })
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for _, f := range sweepMemoFixtures {
		mkTrackerDir(t, shard.pathLSM(), f.dir, f.sentinels...)
		mkRecoveryPayload(t, shard.pathLSM(), f.dir, f.props...)
	}

	hook.Reset()
	shard.cleanStaleMigrationDirs("cat", "filterable")

	const sweepDone = "partial-reindex cleanup: migration dirs swept after index DELETE"
	var lines int
	for _, entry := range hook.AllEntries() {
		if entry.Message != sweepDone {
			continue
		}
		lines++
		require.Equal(t, payloadReadingFixtures, entry.Data["payload_reads"])
	}
	require.Equal(t, 1, lines, "one completion line per DELETE-path sweep")
}

// TestSweepMemoLeavesTheDeletedSetAlone pins the memo as pure memoization: the
// dirs the sweep removes are the ones an uncached walk says it owns.
func TestSweepMemoLeavesTheDeletedSetAlone(t *testing.T) {
	tests := []struct {
		name     string
		propName string
		idxType  string
	}{
		{name: "property named by multi-property trackers", propName: "cat", idxType: "filterable"},
		{name: "property with no tracker of its own", propName: "bird", idxType: "filterable"},
		{name: "property on the other index type", propName: "cat", idxType: "searchable"},
		{name: "underscore-carrying property", propName: "cat_dog", idxType: "filterable"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()

			// The reference walk carries no memo, so it re-reads every payload.
			refLSM := writeSweepMemoFixtures(t)
			want := survivorsOfUncachedSweep(t, refLSM, tc.propName, tc.idxType)

			lsm := writeSweepMemoFixtures(t)
			cleanStaleMigrationDirsAt(lsm, tc.propName, tc.idxType, logger)
			require.Equal(t, want, survivingTrackerDirs(t, lsm))
		})
	}
}

// survivorsOfUncachedSweep is which tracker dirs a sweep that re-reads every
// payload would leave behind.
func survivorsOfUncachedSweep(t *testing.T, lsm, propName, idxType string) []string {
	t.Helper()
	scope := migrationDirsOf(lsm, nil, propName, idxType)
	preserved := completedMigrationGens(scope)

	var survivors []string
	for _, f := range sweepMemoFixtures {
		if !scope.matches(f.dir) {
			survivors = append(survivors, f.dir)
			continue
		}
		if _, gen, ok := parseMigrationDirName(f.dir); ok && preserved[gen] {
			survivors = append(survivors, f.dir)
		}
	}
	sort.Strings(survivors)
	return survivors
}

func survivingTrackerDirs(t *testing.T, lsm string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(lsm, ".migrations"))
	require.NoError(t, err)
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names
}
