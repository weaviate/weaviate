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
// name is what forces the payload read: no name shortcut can answer it, and no
// record here names one either.
type memoFixture struct {
	dir   string
	props []string
}

var sweepMemoFixtures = []memoFixture{
	{dir: "enable_filterable_cat_dog_1", props: []string{"cat", "dog"}},
	{dir: "enable_filterable_cat_dog_2", props: []string{"cat", "dog"}},
	{dir: "filterable_retokenize_bird_cat_1", props: []string{"bird_cat"}},
	{
		// Another property's tracker: settled by name, so never read.
		dir:   "enable_filterable_dog_1",
		props: []string{"dog"},
	},
	{
		// No strategy prefix of this index type: settled by name too.
		dir:   "searchable_retokenize_cat_1",
		props: []string{"cat"},
	},
	{
		// The only rangeable strategy, so without it a sweep that skipped
		// rangeable would cost the same as one that did not.
		dir:   "filterable_to_rangeable_cat_dog_3",
		props: []string{"cat", "dog"},
	},
	{
		// Owned by both index types that share the rangeable strategy, and
		// deleted by neither: its payload names one property called
		// "bird_cat". A memo per index type opens it twice.
		dir:   "filterable_to_rangeable_bird_cat_1",
		props: []string{"bird_cat"},
	},
}

// payloadReadingFixtures is how many fixtures the filterable sweep has to open
// a payload for.
const payloadReadingFixtures = 5

func writeSweepMemoFixtures(t *testing.T) string {
	t.Helper()
	lsm := t.TempDir()
	writeSweepMemoFixturesAt(t, lsm)
	return lsm
}

func writeSweepMemoFixturesAt(t *testing.T, lsm string) {
	t.Helper()
	for _, f := range sweepMemoFixtures {
		mkTrackerDir(t, lsm, f.dir)
		mkRecoveryPayload(t, lsm, f.dir, f.props...)
	}
}

// TestSweepReadsEachTrackerPayloadAtMostOnce pins what one DELETE-path sweep
// costs. A payload parse runs to megabytes inside the RAFT apply that holds
// the FSM loop cluster-wide, so a tracker whose record already answers the
// property question must cost nothing at all.
func TestSweepReadsEachTrackerPayloadAtMostOnce(t *testing.T) {
	logger, _ := test.NewNullLogger()

	lsm := writeSweepMemoFixtures(t)
	sweep := &taskPropsCache{}
	cleanStaleMigrationDirsAt(t.Context(), lsm, "cat", "filterable", logger, sweep)
	require.Equal(t, payloadReadingFixtures, sweep.count(),
		"one sweep opens each ambiguous tracker's payload once")

	recorded := writeSweepMemoFixtures(t)
	const answeredByRecord = "enable_filterable_cat_dog_1"
	mkMigrationRecord(t, recorded, answeredByRecord, MigrationStateIterating,
		map[string]string{"cat": "staged_cat", "dog": "staged_dog"})

	withRecord := &taskPropsCache{}
	cleanStaleMigrationDirsAt(t.Context(), recorded, "cat", "filterable", logger, withRecord)
	require.Equal(t, payloadReadingFixtures-1, withRecord.count(),
		"a tracker a record names is answered from the record, not from its payload")
	require.Equal(t, survivingTrackerDirs(t, lsm), survivingTrackerDirs(t, recorded),
		"and the record answers it the same way the payload did")
}

// disabledFilterableProp is a text property with its filterable index switched
// off, which disables rangeable with it: the two index types share the
// filterable_to_rangeable strategy, so one tracker is in scope for both.
func disabledFilterableProp() *models.Property {
	return &models.Property{
		Name:            "cat",
		DataType:        schema.DataTypeText.PropString(),
		Tokenization:    models.PropertyTokenizationWord,
		IndexFilterable: boolPtr(false),
		IndexSearchable: boolPtr(true),
	}
}

// TestDeleteSweepSharesOnePayloadMemoAndReportsItsReads pins that a property
// DELETE parses each tracker payload once per shard, however many index types
// it disables, and reports the summed count once per class.
//
// Two shards, so no single shard's count can pass for the aggregate.
func TestDeleteSweepSharesOnePayloadMemoAndReportsItsReads(t *testing.T) {
	ctx := testCtx()
	className := "DeleteSweepMemo_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"cat", "dog"})
	hookLogger, hook := test.NewNullLogger()
	shd, idx := testShardWithSettings(t, ctx, class,
		enthnsw.UserConfig{Skip: true}, false, false, false,
		func(i *Index) { i.logger = hookLogger })
	defer shd.Shutdown(ctx)

	second, err := idx.initShard(ctx, "shard2", class, nil, true, true)
	require.NoError(t, err)
	idx.shards.Store("shard2", second)
	defer second.Shutdown(ctx)

	var shards int64
	require.NoError(t, idx.ForEachShard(func(_ string, s ShardLike) error {
		shards++
		writeSweepMemoFixturesAt(t, s.(*Shard).pathLSM())
		return nil
	}))
	require.EqualValues(t, 2, shards)

	// Text has no rangeable index, so disabling filterable also sweeps rangeable.
	prop := disabledFilterableProp()
	indexTypes := disabledIndexTypes(prop)
	require.Equal(t, []string{"filterable", "rangeable"}, indexTypes)

	// One memo per index type is what one shard's DELETE cost before they
	// shared one.
	logger, _ := test.NewNullLogger()
	unsharedLSM := writeSweepMemoFixtures(t)
	unshared := 0
	for _, indexType := range indexTypes {
		perIndexType := &taskPropsCache{}
		cleanStaleMigrationDirsAt(t.Context(), unsharedLSM, prop.Name, indexType, logger, perIndexType)
		unshared += perIndexType.count()
	}

	sharedLSM := writeSweepMemoFixtures(t)
	shared := &taskPropsCache{}
	for _, indexType := range indexTypes {
		cleanStaleMigrationDirsAt(t.Context(), sharedLSM, prop.Name, indexType, logger, shared)
	}
	require.Equal(t, payloadReadingFixtures, shared.count(),
		"the rangeable pass owns no tracker the filterable pass has not already read")
	require.Less(t, shared.count(), unshared,
		"the fixtures must hold a tracker both index types own, or there is nothing to share")

	hook.Reset()
	require.NoError(t, idx.updateProperty(ctx, prop))

	lines := sweepCompletionLines(hook)
	require.Len(t, lines, 1, "one completion line per sweep, whatever the shard count")
	require.Equal(t, shards*int64(shared.count()), lines[0].Data["payload_reads"],
		"the reported count sums every shard, and each shard reads a tracker payload "+
			"once whatever index types the DELETE disables")
	require.Equal(t, "cat", lines[0].Data["property"])
	require.Equal(t, []string{"filterable", "rangeable"}, lines[0].Data["index_types"])

	require.NoError(t, idx.ForEachShard(func(name string, s ShardLike) error {
		require.Equal(t, survivingTrackerDirs(t, unsharedLSM),
			survivingTrackerDirs(t, s.(*Shard).pathLSM()),
			"the memo is memoization only: sharing it across index types must not "+
				"move a dir on shard %q", name)
		return nil
	}))
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
					mkTrackerDir(t, shd.(*Shard).pathLSM(), f.dir)
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
			committed := migrationCommittedStateOf(migrationRecordsAt(refLSM, logger))
			want := sweepSurvivors(names, committed, refScope.inScope)

			lsm := writeSweepMemoFixtures(t)
			cleanStaleMigrationDirsAt(t.Context(), lsm, tc.propName, tc.idxType, logger, nil)
			require.Equal(t, want, survivingTrackerDirs(t, lsm))
		})
	}
}

// sweepSurvivors is which of names a sweep leaves behind: the ones inScope
// rejects, plus the ones a committed migration owns. It is the reference the
// real sweep is diffed against, so the two can only differ where inScope or
// the committed set does.
func sweepSurvivors(names []string, committed migrationCommittedState, inScope func(string) bool) []string {
	survivors := []string{}
	for _, name := range names {
		if !inScope(name) || committed.preservesTracker(name) {
			survivors = append(survivors, name)
		}
	}
	sort.Strings(survivors)
	return survivors
}

// survivingTrackerDirs names the migration directories left on a shard. The
// record store's own directory lives there too and belongs to no migration.
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
