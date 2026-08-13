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
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
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
	{
		// The only rangeable strategy, so without it a sweep that skipped
		// rangeable would cost the same as one that didn't. Tidied at an
		// otherwise-unused gen, so both passes owning the prefix keep it.
		dir:       "filterable_to_rangeable_cat_dog_3",
		props:     []string{"cat", "dog"},
		sentinels: []string{"started.mig", "tidied.mig"},
	},
}

// payloadReadingFixtures is how many fixtures the filterable sweep has to open
// a payload for, i.e. the N of the 2N→N claim.
const payloadReadingFixtures = 4

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
		deletionScope.inScope(f.dir)
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
// reaches the operator once per class, summed over shards, not once per shard.
func TestDeleteSweepReportsItsPayloadReads(t *testing.T) {
	ctx := testCtx()
	className := "DeleteSweepReads_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"cat", "dog"})
	hookLogger, hook := test.NewNullLogger()
	shd, idx := testShardWithSettings(t, ctx, class,
		enthnsw.UserConfig{Skip: true}, false, false, false,
		func(i *Index) { i.logger = hookLogger })
	defer shd.Shutdown(ctx)

	// A second shard, so no single shard's count can pass for the aggregate.
	second, err := idx.initShard(ctx, "shard2", class, nil, true, true)
	require.NoError(t, err)
	idx.shards.Store("shard2", second)
	defer second.Shutdown(ctx)

	var shards int64
	require.NoError(t, idx.ForEachShard(func(_ string, s ShardLike) error {
		shards++
		for _, f := range sweepMemoFixtures {
			mkTrackerDir(t, s.(*Shard).pathLSM(), f.dir, f.sentinels...)
			mkRecoveryPayload(t, s.(*Shard).pathLSM(), f.dir, f.props...)
		}
		return nil
	}))
	require.EqualValues(t, 2, shards)

	// Text has no rangeable index, so disabling filterable also sweeps rangeable.
	disableFilterable := &models.Property{
		Name:            "cat",
		DataType:        schema.DataTypeText.PropString(),
		Tokenization:    models.PropertyTokenizationWord,
		IndexFilterable: boolPtr(false),
		IndexSearchable: boolPtr(true),
	}
	require.Equal(t, []string{"filterable", "rangeable"}, disabledIndexTypes(disableFilterable))

	logger, _ := test.NewNullLogger()
	var perShard int64
	for _, indexType := range disabledIndexTypes(disableFilterable) {
		perShard += int64(cleanStaleMigrationDirsAt(
			writeSweepMemoFixtures(t), "cat", indexType, logger))
	}
	require.Positive(t, perShard, "fixtures must cost the sweep something to report")

	hook.Reset()
	require.NoError(t, idx.updateProperty(ctx, disableFilterable))

	lines := sweepCompletionLines(hook)
	require.Len(t, lines, 1, "one completion line per sweep, whatever the shard count")
	require.Equal(t, shards*perShard, lines[0].Data["payload_reads"],
		"the reported count sums every shard and every swept index type")
	require.Equal(t, "cat", lines[0].Data["property"])
	require.Equal(t, []string{"filterable", "rangeable"}, lines[0].Data["index_types"])
}

// sweepCompletionLine is the one line per operator action the DELETE sweep
// reports its cost on.
const sweepCompletionLine = "partial-reindex cleanup: migration dirs swept for disabled index types"

func sweepCompletionLines(hook *test.Hook) []*logrus.Entry {
	var lines []*logrus.Entry
	for _, entry := range hook.AllEntries() {
		if entry.Message == sweepCompletionLine {
			lines = append(lines, entry)
		}
	}
	return lines
}

// TestSweepReportsOnlyWhatItSwept pins that the completion line reports work
// that happened rather than a property shape. Every property has some index
// type switched off, so a shape gate announces a sweep on every update — even
// one that deleted no index and cleaned nothing.
func TestSweepReportsOnlyWhatItSwept(t *testing.T) {
	textProp := func(filterable bool) *models.Property {
		return &models.Property{
			Name:            "cat",
			DataType:        schema.DataTypeText.PropString(),
			Tokenization:    models.PropertyTokenizationWord,
			IndexFilterable: boolPtr(filterable),
			IndexSearchable: boolPtr(true),
		}
	}

	tests := []struct {
		name      string
		prop      *models.Property
		stale     bool
		wantLines int
	}{
		{
			name: "update that deletes no index at all",
			prop: textProp(true),
		},
		{
			name: "index DELETE with no stale state to clean",
			prop: textProp(false),
		},
		{
			name:      "index DELETE over stale state",
			prop:      textProp(false),
			stale:     true,
			wantLines: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.NotEmpty(t, disabledIndexTypes(tc.prop),
				"no property shape leaves this empty, so shape cannot gate the line")

			ctx := testCtx()
			class := newTestClassWithProps("SweepReport_"+uuid.NewString()[:8], []string{"cat", "dog"})
			hookLogger, hook := test.NewNullLogger()
			shd, idx := testShardWithSettings(t, ctx, class,
				enthnsw.UserConfig{Skip: true}, false, false, false,
				func(i *Index) { i.logger = hookLogger })
			defer shd.Shutdown(ctx)

			if tc.stale {
				for _, f := range sweepMemoFixtures {
					mkTrackerDir(t, shd.(*Shard).pathLSM(), f.dir, f.sentinels...)
					mkRecoveryPayload(t, shd.(*Shard).pathLSM(), f.dir, f.props...)
				}
			}

			hook.Reset()
			require.NoError(t, idx.updateProperty(ctx, tc.prop))
			require.Len(t, sweepCompletionLines(hook), tc.wantLines)
		})
	}
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
			refScope := migrationDirsOf(refLSM, nil, tc.propName, tc.idxType)
			var names []string
			for _, f := range sweepMemoFixtures {
				names = append(names, f.dir)
			}
			want := sweepSurvivors(names, completedMigrationGens(refScope), refScope.inScope)

			lsm := writeSweepMemoFixtures(t)
			cleanStaleMigrationDirsAt(lsm, tc.propName, tc.idxType, logger)
			require.Equal(t, want, survivingTrackerDirs(t, lsm))
		})
	}
}

// sweepSurvivors is which of names a sweep leaves behind: the ones inScope
// rejects, plus the ones whose generation preserved holds. Both differential
// tests build their reference answer with it, so the real sweep and its
// reference can only differ where inScope or preserved does.
func sweepSurvivors(names []string, preserved map[int]bool, inScope func(string) bool) []string {
	var survivors []string
	for _, name := range names {
		if !inScope(name) {
			survivors = append(survivors, name)
			continue
		}
		if _, gen, ok := parseMigrationDirName(name); ok && preserved[gen] {
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
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names
}
