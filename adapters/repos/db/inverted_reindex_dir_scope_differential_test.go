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
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// narrowMatchByName is [migrationDirScope.matchByName] with the
// underscore-free gate [isProvablySingleProperty] replaced, so every fixture
// below can be put through both and the answers compared.
func narrowMatchByName(s migrationDirScope, name string) (matched, decided bool) {
	base := migrationDirBase(name)
	if s.classDir != "" && base == s.classDir {
		return true, true
	}
	if !s.hasStrategyPrefix(base) {
		return false, true
	}
	var exact, token bool
	for _, prefix := range s.prefixes {
		exact = exact || base == migrationDirWithProps(prefix, []string{s.propName})
		token = token || namesPropertyToken(base, prefix, s.propName)
	}
	switch {
	case exact && !strings.Contains(s.propName, "_"):
		return true, true
	case !exact && !token:
		return false, true
	default:
		return false, false
	}
}

func narrowMatches(s migrationDirScope, name string) bool {
	if matched, decided := narrowMatchByName(s, name); decided {
		return matched
	}
	matched, _ := s.inScopeFailingOpen(name)
	return matched
}

// diffDir is one tracker dir of the differential fixtures: its name on disk and
// the property list a payload consistent with that name records.
type diffDir struct {
	name  string
	props []string
}

func writerDir(prefix string, props []string, gen int) diffDir {
	name := migrationDirWithProps(prefix, props)
	if gen > 0 {
		name += genSuffix(gen)
	}
	return diffDir{name: name, props: props}
}

// diffDirs spans the dir-name shapes the deletion path can meet: single- and
// multi-property names, names whose property carries "_", the duplicate-token
// name, a generation-less name, class-level trackers, and one name no writer
// produces because its property list is not sorted.
func diffDirs() []diffDir {
	dirs := []diffDir{
		writerDir(MigrationDirPrefixEnableFilterable, []string{"cat"}, 1),
		writerDir(MigrationDirPrefixEnableFilterable, []string{"cat"}, 0),
		writerDir(MigrationDirPrefixEnableFilterable, []string{"price_cents"}, 1),
		writerDir(MigrationDirPrefixEnableFilterable, []string{"cat_dog"}, 3),
		writerDir(MigrationDirPrefixEnableFilterable, []string{"b_a_c"}, 2),
		writerDir(MigrationDirPrefixEnableFilterable, []string{"cat", "dog"}, 1),
		writerDir(MigrationDirPrefixEnableFilterable, []string{"a", "a"}, 1),
		writerDir(MigrationDirPrefixEnableFilterable, []string{"b_a", "c"}, 1),
		writerDir(MigrationDirPrefixEnableFilterable, []string{"cents", "price"}, 1),
		writerDir(MigrationDirPrefixEnableFilterable, []string{"price_cents", "zebra"}, 1),
		writerDir(MigrationDirPrefixFilterableRetokenize, []string{"price_cents"}, 1),
		writerDir(MigrationDirPrefixFilterableToRangeable, []string{"cat"}, 1),
		writerDir(MigrationDirPrefixEnableSearchable, []string{"price_cents"}, 1),
		writerDir(MigrationDirPrefixSearchableRetokenize, []string{"cat"}, 1),
		writerDir(MigrationDirPrefixRebuildSearchable, []string{"b", "c", "cat"}, 1),
		{name: MigrationDirSearchableMapToBlockmax + genSuffix(1)},
		{name: MigrationDirFilterableRoaringsetRefresh + genSuffix(1)},
		// Property list out of order: only a torn writer produces it.
		{name: "enable_filterable_dog_cat" + genSuffix(1), props: []string{"cat", "dog"}},
	}
	seen := map[string]bool{}
	var out []diffDir
	for _, d := range dirs {
		if seen[d.name] {
			continue
		}
		seen[d.name] = true
		out = append(out, d)
	}
	return out
}

// diffPropNames spans the property-name shapes the predicate sorts into
// provably single and ambiguous.
var diffPropNames = []string{
	"cat", "dog", "zebra", "a",
	"price_cents", "cat_dog", "a_a", "b_a_c", "b_a", "cents",
}

var diffIndexTypes = []string{"filterable", "searchable", "rangeable"}

// payload modes. Only consistentPayload is writer-producible; the rest are what
// a crash or a damaged disk leaves behind, except contradictingPayload, which
// stores a property list its own dir name disowns.
const (
	consistentPayload    = "consistent"
	absentPayload        = "absent"
	corruptPayload       = "corrupt"
	truncatedPayload     = "truncated"
	unreadablePayloadFx  = "unreadable"
	contradictingPayload = "contradicting"
)

var diffPayloadModes = []string{
	consistentPayload, absentPayload, corruptPayload,
	truncatedPayload, unreadablePayloadFx, contradictingPayload,
}

// writeDiffTree materializes every fixture dir under a fresh lsm root.
func writeDiffTree(t *testing.T, payloadMode string, completed bool) (string, []diffDir) {
	t.Helper()
	lsm := t.TempDir()
	dirs := diffDirs()
	for i, d := range dirs {
		sentinels := []string{"started.mig"}
		if completed {
			// Alternate so both sentinels the preserve pass looks for appear.
			sentinels = append(sentinels, []string{"tidied.mig", "merged.mig"}[i%2])
		}
		mkTrackerDir(t, lsm, d.name, sentinels...)
		writeDiffPayload(t, lsm, d, payloadMode)
	}
	return lsm, dirs
}

func writeDiffPayload(t *testing.T, lsm string, d diffDir, mode string) {
	t.Helper()
	path := filepath.Join(lsm, ".migrations", d.name, reindexRecoveryPayloadFile)
	switch mode {
	case absentPayload:
		return
	case corruptPayload:
		require.NoError(t, os.WriteFile(path, []byte("{not json"), 0o644))
	case truncatedPayload:
		full, err := json.Marshal(map[string]any{
			"payload": map[string]any{"properties": d.props},
		})
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, full[:len(full)*2/3], 0o644))
	case unreadablePayloadFx:
		// A directory where the payload belongs reads as unreadable for any
		// user, which chmod 0 does not manage when the tests run as root.
		require.NoError(t, os.MkdirAll(path, 0o755))
	case contradictingPayload:
		mkRecoveryPayload(t, lsm, d.name, "unrelated")
	default:
		if len(d.props) == 0 {
			return
		}
		mkRecoveryPayload(t, lsm, d.name, d.props...)
	}
}

// divergence is one (scope, dir) where the widened predicate answers
// differently from the one it replaced.
type divergence struct {
	propName, indexType, dir string
	preserve                 bool
	narrow, widened          bool
}

// Pins that the widened name shortcut selects exactly the dirs the narrower
// gate did, for every fixture a writer can produce.
func TestWidenedMatchesAgreesWithTheNarrowGate(t *testing.T) {
	for _, payloadMode := range diffPayloadModes {
		for _, completed := range []bool{false, true} {
			lsm, dirs := writeDiffTree(t, payloadMode, completed)
			var diverged []divergence

			for _, propName := range diffPropNames {
				for _, indexType := range diffIndexTypes {
					for _, preserve := range []bool{false, true} {
						scope := migrationDirsOf(lsm, nil, propName, indexType)
						if preserve {
							scope = scope.preserving(indexType)
						}
						for _, d := range dirs {
							narrow := narrowMatches(scope, d.name)
							widened := scope.inScope(d.name)
							if narrow != widened {
								diverged = append(diverged, divergence{
									propName: propName, indexType: indexType,
									dir: d.name, preserve: preserve,
									narrow: narrow, widened: widened,
								})
							}
						}
						if payloadMode != contradictingPayload {
							// What the sweep preserves is downstream of matches(),
							// so it moves only where matches() does.
							require.Equal(t,
								completedMigrationGensNarrow(t, lsm, scope, dirs),
								completedMigrationGens(scope),
								"preserved generations, prop %q index %q preserve %v payload %s",
								propName, indexType, preserve, payloadMode)
						}
					}
				}
			}

			if payloadMode != contradictingPayload {
				require.Empty(t, diverged,
					"payload %s, completed %v: the widened shortcut must not move a dir",
					payloadMode, completed)
				continue
			}
			requireOnlyTheContradictionDivergence(t, diverged)
		}
	}
}

// requireOnlyTheContradictionDivergence pins the one shape where the gates
// differ: a dir named for a provably-single property whose payload names
// another — a pair no writer produces.
func requireOnlyTheContradictionDivergence(t *testing.T, diverged []divergence) {
	t.Helper()
	require.NotEmpty(t, diverged,
		"a payload contradicting its own dir name must exercise the divergence")
	for _, d := range diverged {
		require.False(t, d.narrow)
		require.True(t, d.widened)
		require.True(t, strings.Contains(d.propName, "_"),
			"an underscore-free name was already decided by the narrow gate: %+v", d)
		require.True(t, isProvablySingleProperty(d.propName), "%+v", d)
		require.Equal(t, d.propName, propertySegmentOf(d.dir),
			"only a dir named exactly for this property can diverge: %+v", d)
	}
}

// propertySegmentOf is a tracker dir's property list, i.e. its base minus the
// strategy prefix.
func propertySegmentOf(dir string) string {
	base := migrationDirBase(dir)
	for _, indexType := range diffIndexTypes {
		for _, prefix := range migrationDirPrefixesForIndexType(indexType) {
			if props, ok := strings.CutPrefix(base, prefix+"_"); ok {
				return props
			}
		}
	}
	return ""
}

// completedMigrationGensNarrow is [completedMigrationGens] answered by the gate
// [isProvablySingleProperty] replaced.
func completedMigrationGensNarrow(
	t *testing.T, lsm string, scope migrationDirScope, dirs []diffDir,
) map[int]bool {
	t.Helper()
	out := map[int]bool{}
	for _, d := range dirs {
		_, gen, ok := parseMigrationDirName(d.name)
		if !ok || !narrowMatches(scope, d.name) {
			continue
		}
		path := filepath.Join(lsm, ".migrations", d.name)
		if fileExistsInDir(path, "tidied.mig") || fileExistsInDir(path, "merged.mig") {
			out[gen] = true
		}
	}
	return out
}

// Pins that the real deletion sweep leaves the same dirs behind as a sweep
// driven by the narrower gate.
func TestWidenedSweepLeavesTheSameDirsBehind(t *testing.T) {
	logger, _ := test.NewNullLogger()

	for _, payloadMode := range diffPayloadModes {
		if payloadMode == contradictingPayload {
			// Covered by [requireOnlyTheContradictionDivergence]; a torn
			// payload has no sweep the two gates agree on by construction.
			continue
		}
		for _, completed := range []bool{false, true} {
			for _, propName := range diffPropNames {
				for _, indexType := range diffIndexTypes {
					refLSM, dirs := writeDiffTree(t, payloadMode, completed)
					refScope := migrationDirsOf(refLSM, nil, propName, indexType)
					var names []string
					for _, d := range dirs {
						names = append(names, d.name)
					}
					want := sweepSurvivors(names,
						completedMigrationGensNarrow(t, refLSM, refScope, dirs),
						func(name string) bool { return narrowMatches(refScope, name) })

					lsm, _ := writeDiffTree(t, payloadMode, completed)
					cleanStaleMigrationDirsAt(lsm, propName, indexType, logger)

					require.Equal(t, want, survivingTrackerDirs(t, lsm),
						"payload %s completed %v prop %q index %q",
						payloadMode, completed, propName, indexType)
				}
			}
		}
	}
}
