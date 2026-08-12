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
			name:       "underscore-containing property name still needs the payload",
			classProps: []string{"price_cents"},
			propName:   "price_cents",
			indexTypes: []string{"searchable", "filterable"},
			trackers: []tracker{
				{dir: "searchable_retokenize_price_cents_1", props: []string{"price_cents"}},
				{dir: "filterable_retokenize_price_cents_1", props: []string{"price_cents"}},
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
			wantReads: 0,
		},
		{
			// The non-match half of the shortcut needs no underscore-free name.
			name:       "underscore-containing name still skips another property's dir",
			classProps: []string{"price_cents", "dog"},
			propName:   "price_cents",
			indexTypes: []string{"filterable"},
			trackers: []tracker{
				{dir: "enable_filterable_dog_1", props: []string{"dog"}},
				{dir: "enable_filterable_price_cents_1", props: []string{"price_cents"}},
			},
			wantReads: 1,
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
				// tidied.mig makes these deferred-finalize state, so the sweep
				// walks all three passes and preserves rather than deletes.
				mkTrackerDir(t, lsm, tr.dir, "started.mig", "merged.mig", "tidied.mig")
				if len(tr.props) == 0 {
					require.NoError(t, os.WriteFile(
						filepath.Join(lsm, ".migrations", tr.dir, reindexRecoveryPayloadFile),
						[]byte("{not json"), 0o644))
					continue
				}
				mkRecoveryPayload(t, lsm, tr.dir, tr.props...)
			}

			if tc.gateFailsOpenOn != "" {
				gateStale, _ := hasStalePartialReindexState(lsm, tc.propName, tc.indexTypes[0], nil)
				require.True(t, gateStale,
					"unloaded-shard gate must hydrate rather than report a shard with an "+
						"unreadable payload as clean")
				_, unreadable := migrationDirsOf(lsm, nil, tc.propName, tc.indexTypes[0]).
					match(tc.gateFailsOpenOn)
				require.True(t, unreadable,
					"match must keep reporting the unreadable payload the gate fails open on")
			}

			hook.Reset() // drop whatever shard startup logged
			for _, indexType := range tc.indexTypes {
				require.NoError(t,
					shard.CleanStalePartialReindexState(ctx, tc.propName, indexType))
			}
			require.Equal(t, tc.wantReads, loggedPayloadReads(t, hook, len(tc.indexTypes)),
				"payload.mig reads across the sweep")

			for _, tr := range tc.trackers {
				require.True(t, dirExistsAt(t, lsm, filepath.Join(".migrations", tr.dir)),
					"deferred-finalize tracker %s must survive the sweep", tr.dir)
			}
		})
	}
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

	for _, mode := range payloadModes {
		lsm := t.TempDir()
		var dirs []string
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
				for _, preserve := range []bool{false, true} {
					scope := migrationDirsOf(lsm, nil, propName, indexType)
					if preserve {
						scope = scope.preserving(indexType)
					}
					for _, dir := range dirs {
						byName, decided := scope.matchByName(dir)
						if !decided {
							continue
						}
						fromPayload, _ := scope.match(dir)
						require.Equal(t, fromPayload, byName,
							"dir %q prop %q index %q preserve %v payload %s",
							dir, propName, indexType, preserve, mode)
					}
				}
			}
		}
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

	fromPayload, unreadable := scope.match(dir)
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
// gate call reads. The gate makes three passes over the same tracker dirs
// (preserved sidecars, the match loop, preserved generations), so an unmemoized
// gate reads the same payload once per pass.
func TestGatePayloadReadCount(t *testing.T) {
	tests := []struct {
		name      string
		propName  string
		trackers  []tracker
		sidecars  []string
		wantStale bool
		wantReads int
	}{
		{
			name:     "one tracker is read once, not once per pass",
			propName: "price_cents",
			trackers: []tracker{
				{dir: "enable_filterable_price_cents_1", props: []string{"price_cents"}},
			},
			sidecars: []string{
				"property_price_cents__enable_filterable_ingest_1",
				"property_price_cents__enable_filterable_backup_1",
			},
			wantReads: 1,
		},
		{
			name:     "each tracker is read once",
			propName: "price_cents",
			trackers: []tracker{
				{dir: "enable_filterable_price_cents_1", props: []string{"price_cents"}},
				{dir: "enable_filterable_price_cents_2", props: []string{"price_cents"}},
			},
			sidecars: []string{
				"property_price_cents__enable_filterable_ingest_1",
				"property_price_cents__enable_filterable_ingest_2",
			},
			wantReads: 2,
		},
		{
			// The name shortcut needs a single-property dir, not merely an
			// underscore-free property.
			name:     "a multi-property dir costs a read under an underscore-free name",
			propName: "cat",
			trackers: []tracker{
				{dir: "enable_filterable_cat_dog_1", props: []string{"cat", "dog"}},
			},
			sidecars:  []string{"property_cat__enable_filterable_ingest_1"},
			wantReads: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lsm := t.TempDir()
			for _, tr := range tc.trackers {
				mkTrackerDir(t, lsm, tr.dir, "started.mig", "merged.mig", "tidied.mig")
				mkRecoveryPayload(t, lsm, tr.dir, tr.props...)
			}
			for _, s := range tc.sidecars {
				mkSidecarDir(t, lsm, s)
			}

			stale, reads := hasStalePartialReindexState(lsm, tc.propName, "filterable", nil)
			require.Equal(t, tc.wantStale, stale)
			require.Equal(t, tc.wantReads, reads, "payload.mig reads in one gate call")
		})
	}
}
