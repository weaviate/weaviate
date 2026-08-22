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
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

type tracker struct {
	dir string
	// props is the payload's property list; empty writes a payload that cannot
	// be parsed.
	props []string
	// record, where set, gives this tracker's migration a record in that
	// state, staging each of props into a directory of its own.
	record MigrationState
}

// writeTrackerPayload writes the payload.mig an existing tracker dir claims its
// properties with, honoring the empty-props convention on [tracker].
func writeTrackerPayload(t *testing.T, lsm string, tr tracker) {
	t.Helper()
	if len(tr.props) == 0 {
		require.NoError(t, os.WriteFile(
			filepath.Join(lsm, ".migrations", tr.dir, reindexRecoveryPayloadFile),
			[]byte("{not json"), 0o644))
		return
	}
	mkRecoveryPayload(t, lsm, tr.dir, tr.props...)
}

// writeTracker materializes one tracker dir, its payload, and where the row
// asks for one, the record that says how far its migration got.
func writeTracker(t *testing.T, lsm string, tr tracker) {
	t.Helper()
	mkTrackerDir(t, lsm, tr.dir)
	writeTrackerPayload(t, lsm, tr)
	if tr.record == "" {
		return
	}
	staged := map[string]string{}
	for _, prop := range tr.props {
		staged[prop] = "staged_" + prop + "_" + tr.dir
	}
	mkMigrationRecord(t, lsm, tr.dir, tr.record, staged)
}

// TestSweepPayloadReadCount pins how many tracker payloads one loaded-shard
// sweep reads off disk.
func TestSweepPayloadReadCount(t *testing.T) {
	tests := []struct {
		name       string
		classProps []string
		propName   string
		indexTypes []string
		trackers   []tracker
		wantReads  int
		// wantSurvivors is the tracker dirs still on disk afterwards: the ones
		// no index type of this sweep owns, plus the ones a committed record
		// holds.
		wantSurvivors []string
		// gateFailsOpenOn is a tracker dir the unloaded-shard gate must still
		// hydrate for, however the loaded sweep answered it.
		gateFailsOpenOn string
	}{
		{
			name:       "underscore-free property name is answered by the dir name",
			classProps: []string{"category"},
			propName:   "category",
			indexTypes: []string{"searchable", "filterable"},
			trackers: []tracker{
				{dir: "searchable_retokenize_category_1", props: []string{"category"}},
				{dir: "filterable_retokenize_category_1", props: []string{"category"}},
			},
			wantReads: 0,
		},
		{
			// No list of two properties sorts to "price_cents", so the name is
			// as unforgeable as an underscore-free one.
			name:       "provably single property name is answered by the dir name",
			classProps: []string{"price_cents"},
			propName:   "price_cents",
			indexTypes: []string{"searchable", "filterable"},
			trackers: []tracker{
				{dir: "searchable_retokenize_price_cents_1", props: []string{"price_cents"}},
				{dir: "filterable_retokenize_price_cents_1", props: []string{"price_cents"}},
			},
			wantReads: 0,
		},
		{
			// ["cat","dog"] sorts to exactly this name, so only the payload can
			// say which of the two shapes the dir is.
			name:       "ambiguous property name still needs the payload",
			classProps: []string{"cat_dog"},
			propName:   "cat_dog",
			indexTypes: []string{"searchable", "filterable"},
			trackers: []tracker{
				{dir: "searchable_retokenize_cat_dog_1", props: []string{"cat_dog"}},
				{dir: "filterable_retokenize_cat_dog_1", props: []string{"cat_dog"}},
			},
			// One per tuple, not three: the memo spans the sweep's passes.
			wantReads: 2,
		},
		{
			name:       "another property's dir under the same prefix is not read",
			classProps: []string{"cat", "dog"},
			propName:   "cat",
			indexTypes: []string{"filterable"},
			trackers: []tracker{
				{dir: "enable_filterable_dog_1", props: []string{"dog"}},
				{dir: "enable_filterable_cat_1", props: []string{"cat"}},
			},
			wantReads:     0,
			wantSurvivors: []string{"enable_filterable_dog_1"},
		},
		{
			// The non-match half of the shortcut needs no unforgeable name.
			name:       "ambiguous name still skips another property's dir",
			classProps: []string{"cat_dog", "bird"},
			propName:   "cat_dog",
			indexTypes: []string{"filterable"},
			trackers: []tracker{
				{dir: "enable_filterable_bird_1", props: []string{"bird"}},
				{dir: "enable_filterable_cat_dog_1", props: []string{"cat_dog"}},
			},
			wantReads:     1,
			wantSurvivors: []string{"enable_filterable_bird_1"},
		},
		{
			name:       "multi-property dir needs the payload",
			classProps: []string{"cat", "dog"},
			propName:   "cat",
			indexTypes: []string{"filterable"},
			trackers: []tracker{
				{dir: "enable_filterable_cat_dog_1", props: []string{"cat", "dog"}},
			},
			wantReads: 1,
		},
		{
			name:       "unparseable payload under an unambiguous name",
			classProps: []string{"category"},
			propName:   "category",
			indexTypes: []string{"filterable"},
			trackers: []tracker{
				{dir: "filterable_retokenize_category_1"},
			},
			// The name settles it, so the sweep never opens the payload the
			// gate still fails open on.
			wantReads:       0,
			gateFailsOpenOn: "filterable_retokenize_category_1",
		},
		{
			// The record answers for free what the payload would have cost a
			// parse, and its migration is committed, so the sweep keeps the
			// directory the in-memory bucket pointer is on.
			name:       "a committed migration's tracker is answered by its record",
			classProps: []string{"cat", "dog"},
			propName:   "cat",
			indexTypes: []string{"filterable"},
			trackers: []tracker{
				{
					dir: "enable_filterable_cat_dog_1", props: []string{"cat", "dog"},
					record: MigrationStateSwapped,
				},
			},
			wantReads:     0,
			wantSurvivors: []string{"enable_filterable_cat_dog_1"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "PayloadReads_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, tc.classProps)
			// The sweep reports its read count on a log field.
			hookLogger, hook := test.NewNullLogger()
			shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false, func(i *Index) { i.logger = hookLogger })
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)
			lsm := shard.pathLSM()

			for _, tr := range tc.trackers {
				writeTracker(t, lsm, tr)
			}

			if tc.gateFailsOpenOn != "" {
				gateLogger, _ := test.NewNullLogger()
				gateStale, _ := hasStalePartialReindexState(
					lsm, tc.propName, tc.indexTypes[0], nil, nil, gateLogger)
				require.True(t, gateStale,
					"unloaded-shard gate must hydrate rather than report a shard with an "+
						"unreadable payload as clean")
				_, unreadable := migrationDirsOf(lsm, nil, tc.propName, tc.indexTypes[0]).
					inScopeFailingOpen(tc.gateFailsOpenOn)
				require.True(t, unreadable,
					"inScopeFailingOpen must keep reporting the unreadable payload the gate fails open on")
			}

			hook.Reset() // drop whatever shard startup logged
			returned := 0
			for _, indexType := range tc.indexTypes {
				returned += cleanSweep(t, ctx, shard, tc.propName, indexType)
			}
			require.Equal(t, tc.wantReads, loggedPayloadReads(t, hook, len(tc.indexTypes)),
				"payload.mig reads across the sweep")
			require.Equal(t, tc.wantReads, returned,
				"the count the sweep hands its caller is the one it logs")

			want := append([]string{}, tc.wantSurvivors...)
			sort.Strings(want)
			require.Equal(t, want, survivingTrackerDirs(t, lsm),
				"the sweep removes the trackers its scope owns and no others")
		})
	}
}

// The unloaded gate is not the only arm that pays for payloads: a loaded shard
// pays inside the per-shard sweep. On a collection with no cold tenants every
// shard is loaded, so a summary fed by the gate alone reports zero reads for
// every sweep the node ever runs.
func TestIndexSweepReportsLoadedShardPayloadReads(t *testing.T) {
	ctx := testCtx()
	className := "LoadedSweepReads_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"cat", "dog"})
	hookLogger, hook := test.NewNullLogger()
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false, func(i *Index) { i.logger = hookLogger })
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	// ["cat","dog"] sorts to exactly this name, so only the payload can say
	// whether the tracker belongs to the swept property.
	writeTracker(t, shard.pathLSM(),
		tracker{dir: "enable_filterable_cat_dog_1", props: []string{"cat", "dog"}})

	hook.Reset()
	require.NoError(t, idx.cleanStalePartialReindexState(ctx, "cat", "filterable", nil))

	summary := onlySweepSummary(t, hook)
	require.Equal(t, 0, summary.Data["skipped_shards"],
		"the shard is loaded, so the gate never answered for it")
	require.Equal(t, 1, summary.Data["payload_reads"],
		"the summary carries what the loaded shard's own sweep paid")
}

// loggedPayloadReads sums the payload counts the sweeps reported on their
// completion line. Requiring one line per sweep keeps a sweep that returned
// early, or a renamed message, from reading as zero reads.
func loggedPayloadReads(t *testing.T, hook *test.Hook, wantSweeps int) int {
	t.Helper()
	const sweepDone = "partial-reindex cleanup: sidecar dirs + migration dir cleaned"

	total, sweeps := 0, 0
	for _, entry := range hook.AllEntries() {
		if entry.Message != sweepDone {
			continue
		}
		reads, ok := entry.Data["payload_reads"].(int)
		require.True(t, ok, "sweep completion line carries an int payload_reads")
		total += reads
		sweeps++
	}
	require.Equal(t, wantSweeps, sweeps, "one completion line per sweep")
	return total
}

// TestMatchByNameAgreesWithMatch pins that where the name alone decides, it
// decides the same way the payload would have, over every dir a writer can
// produce. [TestMatchByNameOverridesAContradictingPayload] covers the one pair
// a writer cannot.
func TestMatchByNameAgreesWithMatch(t *testing.T) {
	writers := []struct {
		prefix string
		props  []string
		gen    int
	}{
		{MigrationDirPrefixEnableFilterable, []string{"cat"}, 1},
		{MigrationDirPrefixEnableFilterable, []string{"cat", "dog"}, 1},
		{MigrationDirPrefixEnableFilterable, []string{"cat_dog"}, 2},
		{MigrationDirPrefixEnableFilterable, []string{"category"}, 1},
		{MigrationDirPrefixEnableFilterable, []string{"dog"}, 1},
		{MigrationDirPrefixEnableFilterable, []string{"b", "c", "cat"}, 1},
		{MigrationDirPrefixFilterableRetokenize, []string{"cat"}, 1},
		{MigrationDirPrefixFilterableToRangeable, []string{"cat"}, 1},
		{MigrationDirPrefixEnableSearchable, []string{"cat"}, 1},
		{MigrationDirPrefixSearchableRetokenize, []string{"cat"}, 1},
		{MigrationDirPrefixRebuildSearchable, []string{"b", "c", "cat"}, 1},
		{MigrationDirSearchableMapToBlockmax, nil, 1},
		{MigrationDirFilterableRoaringsetRefresh, nil, 1},
	}
	payloadModes := []string{"written", "absent", "unparseable"}
	propNames := []string{"cat", "cat_dog", "category", "b"}

	// What the name shortcut decides over the fixtures below, counted per arm.
	// Without these the loop's "ask the payload" skip is also the skip a
	// matchByName that decides nothing takes, and the test asserts nothing at
	// all. matchByName never opens a payload, so the three counts hold for
	// every payload mode.
	const (
		wantInScope    = 8
		wantOutOfScope = 152
		wantUndecided  = 8
	)

	for _, mode := range payloadModes {
		lsm := t.TempDir()
		var dirs []string
		var inScope, outOfScope, undecided int
		for _, w := range writers {
			dir := migrationDirWithProps(w.prefix, w.props) + genSuffix(w.gen)
			dirs = append(dirs, dir)
			mkTrackerDir(t, lsm, dir)
			switch mode {
			case "written":
				if len(w.props) > 0 {
					mkRecoveryPayload(t, lsm, dir, w.props...)
				}
			case "unparseable":
				require.NoError(t, os.WriteFile(
					filepath.Join(lsm, ".migrations", dir, reindexRecoveryPayloadFile),
					[]byte("{not json"), 0o644))
			}
		}
		// Plus a name with no generation suffix: its base is the whole name.
		noGen := migrationDirWithProps(MigrationDirPrefixEnableFilterable, []string{"cat"})
		mkTrackerDir(t, lsm, noGen)
		dirs = append(dirs, noGen)

		for _, propName := range propNames {
			for _, indexType := range []string{"filterable", "searchable", "rangeable"} {
				scope := migrationDirsOf(lsm, nil, propName, indexType)
				for _, dir := range dirs {
					byName, decided := scope.matchByName(dir)
					if !decided {
						undecided++
						continue
					}
					if byName {
						inScope++
					} else {
						outOfScope++
					}
					fromPayload, unreadable := scope.inScopeFailingOpen(dir)
					require.Equal(t, fromPayload, byName,
						"dir %q prop %q index %q payload %s", dir, propName, indexType, mode)
					if byName {
						// A dir the name puts in scope still fails open on a
						// payload it cannot read; see gateFailsOpenOn above.
						continue
					}
					require.False(t, unreadable,
						"a name that disowns the property leaves its payload nothing to add: "+
							"dir %q prop %q index %q payload %s", dir, propName, indexType, mode)
				}
			}
		}
		require.Equal(t, wantInScope, inScope, "dirs the name alone puts in scope, payload %s", mode)
		require.Equal(t, wantOutOfScope, outOfScope, "dirs the name alone disowns, payload %s", mode)
		require.Equal(t, wantUndecided, undecided, "dirs left to the payload, payload %s", mode)
	}
}

// TestMatchByNameOverridesAContradictingPayload pins the one input where the
// name shortcut and the payload disagree: a dir named for a single property
// whose payload names two. No writer produces it — name and payload come from
// the same sorted list — so the name wins.
func TestMatchByNameOverridesAContradictingPayload(t *testing.T) {
	lsm := t.TempDir()
	dir := migrationDirWithProps(MigrationDirPrefixEnableFilterable, []string{"cat"}) + genSuffix(1)
	mkTrackerDir(t, lsm, dir)
	mkRecoveryPayload(t, lsm, dir, "cat", "dog")

	scope := migrationDirsOf(lsm, nil, "cat", "filterable")

	matched, decided := scope.matchByName(dir)
	require.True(t, decided, "an underscore-free name is decided without the payload")
	require.True(t, matched)

	fromPayload, unreadable := scope.inScopeFailingOpen(dir)
	require.False(t, fromPayload, "the payload alone would disown the dir it is stored in")
	require.False(t, unreadable)
}

// TestMigrationDirPrefixesDoNotNest guards the assumption the name shortcut
// rests on: with no prefix sitting on a "_" boundary inside another, a dir's
// property segment is unambiguous, so exact equality cannot be re-split into a
// different prefix plus a longer property list.
func TestMigrationDirPrefixesDoNotNest(t *testing.T) {
	for _, indexType := range []string{"filterable", "searchable", "rangeable"} {
		prefixes := migrationDirPrefixesForIndexType(indexType)
		for _, outer := range prefixes {
			for _, inner := range prefixes {
				if outer == inner {
					continue
				}
				require.False(t, strings.HasPrefix(outer, inner+"_"),
					"index type %q: prefix %q nests inside %q, which breaks "+
						"[migrationDirScope.matchByName]", indexType, outer, inner)
			}
		}
	}
}

// TestTaskPropsCacheReadsEachPayloadOnce pins the memo itself, independently of
// which passes a sweep happens to make.
func TestTaskPropsCacheReadsEachPayloadOnce(t *testing.T) {
	lsm := t.TempDir()
	mkTrackerDir(t, lsm, "enable_filterable_a_b_1")
	mkRecoveryPayload(t, lsm, "enable_filterable_a_b_1", "a", "b")

	cache := &taskPropsCache{}
	scope := migrationDirsOf(lsm, nil, "a", "filterable").cachingProps(cache)
	for range 3 {
		props, ok, unreadable := scope.taskProperties("enable_filterable_a_b_1")
		require.True(t, ok)
		require.False(t, unreadable)
		require.True(t, slices.Contains(props, "a"))
	}
	require.Equal(t, 1, cache.count())
}

// TestGatePayloadReadCount pins how many tracker payloads one unloaded-shard
// gate call reads. A payload parse runs to megabytes, so the gate only ever
// opens one for a tracker no record names, and only once its cheaper passes
// have failed to answer.
func TestGatePayloadReadCount(t *testing.T) {
	tests := []struct {
		name      string
		propName  string
		trackers  []tracker
		sidecars  []string
		wantStale bool
		// wantFinalizable is the gate's other half: leftovers only a load can
		// reclaim hold the shard open just as stale state does, so a row that
		// pins wantStale alone has not said whether the gate skips.
		wantFinalizable bool
		wantReads       int
	}{
		{
			name:     "a tracker its record names costs no payload read",
			propName: "price_cents",
			trackers: []tracker{
				{
					dir: "enable_filterable_price_cents_1", props: []string{"price_cents"},
					record: MigrationStateSwapped,
				},
			},
			sidecars: []string{"staged_price_cents_enable_filterable_price_cents_1"},
			// A recorded flip awaiting promotion is exactly what only a load
			// finishes, so the gate holds this shard open rather than skipping
			// it — the reason its wantStale is false without the shard being
			// clean.
			wantFinalizable: true,
			wantReads:       0,
		},
		{
			name:     "each tracker no record names is read once",
			propName: "cat",
			trackers: []tracker{
				{dir: "enable_filterable_bird_cat_1", props: []string{"bird_cat"}},
				{dir: "filterable_retokenize_bird_cat_1", props: []string{"bird_cat"}},
			},
			wantReads: 2,
		},
		{
			// The sidecar pass runs first and reads no payload at all, so a
			// shard it already condemns costs nothing more.
			name:     "an unpreserved sidecar answers before any payload is opened",
			propName: "cat",
			trackers: []tracker{
				{dir: "enable_filterable_cat_dog_1", props: []string{"cat", "dog"}},
			},
			sidecars:  []string{"property_cat__enable_filterable_ingest_1"},
			wantStale: true,
			wantReads: 0,
		},
		{
			// The dir name already disowns "category", and no writer can name a
			// dir for one property while its payload lists another.
			name:     "a corrupt payload cannot claim a dir another property is named in",
			propName: "category",
			trackers: []tracker{
				{dir: "enable_filterable_other_1"},
			},
			wantStale: false,
			wantReads: 0,
		},
		{
			// The case a sweep of 5,000 cold tenants is made of: the gate pays a
			// read and then reports the shard stale, so a count taken only off
			// the skip arm reads zero where the cost is highest.
			name:      "a read the gate paid before answering stale",
			propName:  "cat",
			trackers:  []tracker{{dir: "enable_filterable_cat_dog_1", props: []string{"cat", "dog"}}},
			wantStale: true,
			wantReads: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lsm := t.TempDir()
			for _, tr := range tc.trackers {
				writeTracker(t, lsm, tr)
			}
			for _, s := range tc.sidecars {
				mkSidecarDir(t, lsm, s)
			}
			logger, _ := test.NewNullLogger()

			props := &taskPropsCache{}
			stale, finalizable := hasStalePartialReindexState(lsm, tc.propName, "filterable", nil, props, logger)
			require.Equal(t, tc.wantStale, stale)
			require.Equal(t, tc.wantFinalizable, finalizable,
				"the skip is !stale && !finalizable, so a row pinning wantStale alone has not pinned the skip")
			require.Equal(t, tc.wantReads, props.count(), "payload.mig reads in one gate call")
		})
	}
}
