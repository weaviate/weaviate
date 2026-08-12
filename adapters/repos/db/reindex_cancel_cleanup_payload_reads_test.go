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
	"github.com/stretchr/testify/require"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// tracker is one tracker dir a sweep will walk.
type tracker struct {
	dir string
	// props is the payload's property list; empty writes a payload that cannot
	// be parsed.
	props []string
}

// TestSweepPayloadReadCount pins what a loaded shard's sweep reads off disk.
// Every tracker dir used to cost three payload reads per (property, index
// type) — one per pass — which is what makes a sweep across many tenants
// quadratic in shard count.
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
			// The non-match half of the shortcut needs no underscore-free name,
			// so only the dir actually naming this property still costs a parse.
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
			// The name settles it, so the sweep never opens the payload — but
			// the gate below must still refuse to call the shard clean.
			wantReads:       0,
			gateFailsOpenOn: "filterable_retokenize_category_1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "PayloadReads_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, tc.classProps)
			shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
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
				require.True(t,
					hasStalePartialReindexState(lsm, tc.propName, tc.indexTypes[0], nil),
					"unloaded-shard gate must hydrate rather than report a shard with an "+
						"unreadable payload as clean")
				_, unreadable := migrationDirsOf(lsm, nil, tc.propName, tc.indexTypes[0]).
					match(tc.gateFailsOpenOn)
				require.True(t, unreadable,
					"match must keep reporting the unreadable payload the gate fails open on")
			}

			reads := 0
			for _, indexType := range tc.indexTypes {
				props := &taskPropsCache{}
				require.NoError(t,
					shard.cleanStalePartialReindexState(ctx, tc.propName, indexType, props))
				reads += props.count()
			}
			require.Equal(t, tc.wantReads, reads, "payload.mig reads across the sweep")

			for _, tr := range tc.trackers {
				require.True(t, dirExistsAt(t, lsm, filepath.Join(".migrations", tr.dir)),
					"deferred-finalize tracker %s must survive the sweep", tr.dir)
			}
		})
	}
}

// TestMatchByNameAgreesWithMatch pins that skipping the payload never changes
// the answer: wherever the name alone decides, it decides the same way the
// payload would have.
//
// Every dir here is built the way a writer builds one — name and payload from
// the same sorted property list. [TestMatchByNameOverridesAContradictingPayload]
// covers the pair a writer cannot produce.
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
		// A dir predating the generation suffix carries the prefix as its name.
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
// name shortcut answers differently from the payload: a dir named for a single
// property whose payload names two. A writer cannot produce it, since the dir
// name is that same property list sorted and joined, so the name is taken as
// the truth rather than paying a read on every well-formed dir to keep the old
// answer for this one.
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
