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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// narrowMatchByName is [migrationDirScope.matchByName] with the
// underscore-free gate [isProvablySingleProperty] replaced, so every fixture
// below can be put through both and the answers compared. The widened gate is
// what production runs; this is the conservative predicate it replaced.
func narrowMatchByName(s migrationDirScope, name string) (matched, decided bool) {
	base := migrationDirBase(name)
	if !s.hasStrategyPrefix(base) {
		return false, true
	}
	exact, token := s.nameArms(base)
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
// the property list a record and payload consistent with that name carry.
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

// The attribution modes a tracker dir can be in. Only recordedProps and
// consistentPayload are writer-producible; the rest are what a crash or a
// damaged disk leaves behind, except contradictingPayload, which stores a
// property list its own dir name disowns.
const (
	recordedProps        = "record"
	consistentPayload    = "consistent"
	absentPayload        = "absent"
	corruptPayload       = "corrupt"
	truncatedPayload     = "truncated"
	unreadablePayloadFx  = "unreadable"
	contradictingPayload = "contradicting"
)

var diffAttributionModes = []string{
	recordedProps, consistentPayload, absentPayload, corruptPayload,
	truncatedPayload, unreadablePayloadFx, contradictingPayload,
}

// writeDiffTree materializes every fixture dir under a fresh lsm root.
// committed decides whether each dir's migration has committed its data,
// which is what the sweep must preserve.
func writeDiffTree(t *testing.T, mode string, committed bool) (string, []diffDir) {
	t.Helper()
	lsm := t.TempDir()
	dirs := diffDirs()
	for i, d := range dirs {
		mkTrackerDir(t, lsm, d.name)
		if mode == recordedProps || committed {
			writeDiffRecord(t, lsm, d, i, committed)
			continue
		}
		writeDiffPayload(t, lsm, d, mode)
	}
	return lsm, dirs
}

// writeDiffRecord plants the record that attributes a directory. A committed
// one owns a staged directory the sweep must not touch; an uncommitted one is
// stale state the sweep removes.
func writeDiffRecord(t *testing.T, lsm string, d diffDir, seq int, committed bool) {
	t.Helper()
	props := d.props
	if len(props) == 0 {
		// A class-level tracker names no property of its own.
		props = []string{"cat"}
	}
	staged := map[string]string{}
	canonical := map[string]string{}
	for _, prop := range props {
		staged[prop] = fmt.Sprintf("property_%s__enable_filterable_ingest_%d", prop, seq+1)
		canonical[prop] = "property_" + prop
	}
	state := MigrationStateIterating
	if committed {
		state = MigrationStateSwapped
	}
	mkMigrationRecordAt(t, lsm, d.name, staged, canonical, state)
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
	narrow, widened          bool
}

// Pins the widened name shortcut against the conservative gate it replaced,
// over the dir-name, property-name and attribution shapes below. The shortcut
// skips the record and the payload on the strength of the name alone, so a dir
// it moves is a dir the sweep either deletes or spares differently.
//
// Narrower than the whole space a writer can produce: every record here
// carries one strategy code and one index type whatever its directory's real
// strategy is, and only the Iterating and Swapped states appear, so
// migrationCommittedStateOf's promoted-tracker arm is never reached.
func TestWidenedMatchesAgreesWithTheNarrowGate(t *testing.T) {
	logger, _ := test.NewNullLogger()

	for _, mode := range diffAttributionModes {
		for _, committed := range []bool{false, true} {
			if committed && mode != recordedProps {
				// Committed state is a record fact; the payload modes have no
				// second shape here.
				continue
			}
			lsm, dirs := writeDiffTree(t, mode, committed)
			state := migrationCommittedStateOf(migrationRecordsAt(lsm, logger))
			var diverged []divergence

			for _, propName := range diffPropNames {
				for _, indexType := range diffIndexTypes {
					scope := migrationDirsOf(lsm, nil, propName, indexType).
						cachingProps(&taskPropsCache{}).knownFrom(state)
					for _, d := range dirs {
						narrow, widened := narrowMatches(scope, d.name), scope.inScope(d.name)
						if narrow != widened {
							diverged = append(diverged, divergence{
								propName: propName, indexType: indexType,
								dir: d.name, narrow: narrow, widened: widened,
							})
						}
					}
				}
			}

			if mode != contradictingPayload {
				require.Empty(t, diverged,
					"attribution %s, committed %v: the widened shortcut must not move a dir",
					mode, committed)
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

// Pins that the real deletion sweep leaves the same dirs behind as a sweep
// driven by the narrower gate. The predicate differential above compares one
// question; this compares the whole sweep, where preservation, generation
// parsing and the walk order all compose on top of it.
func TestWidenedSweepLeavesTheSameDirsBehind(t *testing.T) {
	logger, _ := test.NewNullLogger()

	for _, mode := range diffAttributionModes {
		if mode == contradictingPayload {
			// Covered by [requireOnlyTheContradictionDivergence]; a torn
			// payload has no sweep the two gates agree on by construction.
			continue
		}
		for _, committed := range []bool{false, true} {
			if committed && mode != recordedProps {
				continue
			}
			for _, propName := range diffPropNames {
				for _, indexType := range diffIndexTypes {
					refLSM, dirs := writeDiffTree(t, mode, committed)
					refState := migrationCommittedStateOf(migrationRecordsAt(refLSM, logger))
					refScope := migrationDirsOf(refLSM, nil, propName, indexType).
						cachingProps(&taskPropsCache{}).knownFrom(refState)
					var names []string
					for _, d := range dirs {
						names = append(names, d.name)
					}
					want := sweepSurvivors(names, refState,
						func(name string) bool { return narrowMatches(refScope, name) })

					lsm, _ := writeDiffTree(t, mode, committed)
					cleanStaleMigrationDirsAt(t.Context(), lsm, propName, indexType, logger, nil)

					require.Equal(t, want, survivingTrackerDirs(t, lsm),
						"attribution %s committed %v prop %q index %q",
						mode, committed, propName, indexType)
				}
			}
		}
	}
}
