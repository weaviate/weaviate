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
	"sort"
	"strings"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// This file is the differential gate on answering [readTaskProps] from a
// tracker's properties.mig instead of parsing its payload.mig: every fixture
// is put through both readers and compared, at the level of
// [migrationDirScope.matches], the preserve pass, and the sweep's survivors.
//
// One cell is allowed to differ: an unusable payload next to a sidecar that
// rebuilds the dir's own name, where the fix decides as if the payload were
// intact — see [TestSidecarAnswersAsAnIntactPayloadWouldWhereTheStoredPayloadIsUnusable].

// headTaskProps is [readTaskProps] as it stood before properties.mig could
// answer: payload.mig decides, or nothing does.
func headTaskProps(migDir string) taskProps {
	props, err := readRecoveryPropertyNames(migDir)
	if err != nil {
		return taskProps{unreadable: !os.IsNotExist(err)}
	}
	if len(props) == 0 {
		return taskProps{}
	}
	return taskProps{props: props, ok: true}
}

// headScoped is scope with a memo pre-loaded with [headTaskProps]' answer for
// every fixture, so the production matcher runs unmodified against the old
// reader. Pre-loading rather than reimplementing [migrationDirScope.match]
// keeps the two sides from drifting: only the reader differs.
func headScoped(scope migrationDirScope, fixtures []sidecarFixture) migrationDirScope {
	memo := &taskPropsCache{byDir: map[string]taskProps{}}
	for _, f := range fixtures {
		migDir := filepath.Join(scope.lsmPath, ".migrations", f.name)
		memo.byDir[migDir] = headTaskProps(migDir)
	}
	return scope.cachingProps(memo)
}

// sidecarFixture is one tracker dir: its name on disk, the property list the
// writer built both that name and its sidecar from, and the strategy prefix.
type sidecarFixture struct {
	name   string
	props  []string
	prefix string
}

// sidecarPropLists are the property lists the fixtures are built from — one per
// dir-name shape the corroboration has to sort out: a plain single property,
// one carrying "_", a multi-property list, the duplicate list whose name keeps
// a token its deduplicated form loses, and a three-property list.
var sidecarPropLists = [][]string{
	{"cat"},
	{"price_cents"},
	{"a", "b"},
	{"a", "a"},
	{"b", "c", "cat"},
}

// singlePropertyStrategyPrefixes are the strategies whose task constructor takes
// one property rather than a list, so no tracker dir of theirs can name two.
var singlePropertyStrategyPrefixes = map[string]bool{
	MigrationDirPrefixFilterableRetokenize: true,
	MigrationDirPrefixSearchableRetokenize: true,
}

// sidecarFixturesFor spans every per-property strategy of one index type, so no
// strategy reaches the reader untested.
func sidecarFixturesFor(indexType string) []sidecarFixture {
	var out []sidecarFixture
	gen := 0
	for _, prefix := range migrationDirPrefixesForIndexType(indexType) {
		for _, props := range sidecarPropLists {
			if singlePropertyStrategyPrefixes[prefix] && len(props) > 1 {
				continue
			}
			gen++
			out = append(out, sidecarFixture{
				name:   migrationDirWithProps(prefix, props) + genSuffix(gen),
				props:  props,
				prefix: prefix,
			})
		}
	}
	return out
}

// sidecarPropNames span both halves of the name gate: names
// [migrationDirScope.matchByName] settles on its own, and names it cannot,
// which are the ones that reach the payload at all.
var sidecarPropNames = []string{
	"cat", "a", "b", "c", "zebra", "price_cents",
	"a_b", "a_a", "b_c_cat",
}

// sweepPropNames is sidecarPropNames trimmed to one representative per gate
// outcome, for the matrix that materializes and sweeps a tree per cell.
var sweepPropNames = []string{"cat", "a", "price_cents", "a_b", "b_c_cat"}

// payload.mig fixture modes. Only payloadValid is writer-producible.
const (
	payloadValid      = "payload-valid"
	payloadAbsent     = "payload-absent"
	payloadCorrupt    = "payload-corrupt"
	payloadTruncated  = "payload-truncated"
	payloadUnreadable = "payload-unreadable"
)

var sidecarPayloadModes = []string{
	payloadValid, payloadAbsent, payloadCorrupt, payloadTruncated, payloadUnreadable,
}

// properties.mig fixture modes. Only sidecarValid is writer-producible;
// sidecarZeroByte and sidecarTruncated are what a kill mid-write leaves,
// produced here by truncating a written file so the fixture cannot drift
// from the state it stands for.
const (
	sidecarAbsent        = "sidecar-absent"
	sidecarZeroByte      = "sidecar-zero-byte"
	sidecarValid         = "sidecar-valid"
	sidecarTruncated     = "sidecar-truncated"
	sidecarDeduped       = "sidecar-deduped"
	sidecarContradicting = "sidecar-contradicting"
)

var sidecarPropsModes = []string{
	sidecarAbsent, sidecarZeroByte, sidecarValid,
	sidecarTruncated, sidecarDeduped, sidecarContradicting,
}

// writeSidecarTree materializes every fixture of one index type under a fresh
// lsm root. completed adds the sentinels the preserve pass looks for.
func writeSidecarTree(
	t *testing.T, indexType, payloadMode, propsMode string, completed bool,
) (string, []sidecarFixture) {
	t.Helper()
	lsm := t.TempDir()
	fixtures := sidecarFixturesFor(indexType)
	for i, f := range fixtures {
		sentinels := []string{"started.mig"}
		if completed {
			// Alternate so both sentinels the preserve pass accepts appear.
			sentinels = append(sentinels, []string{"tidied.mig", "merged.mig"}[i%2])
		}
		mkTrackerDir(t, lsm, f.name, sentinels...)
		writeSidecarPayload(t, lsm, f, payloadMode)
		writeSidecarProps(t, lsm, f, propsMode)
	}
	return lsm, fixtures
}

func writeSidecarPayload(t *testing.T, lsm string, f sidecarFixture, mode string) {
	t.Helper()
	path := filepath.Join(lsm, ".migrations", f.name, reindexRecoveryPayloadFile)
	switch mode {
	case payloadAbsent:
		return
	case payloadCorrupt:
		require.NoError(t, os.WriteFile(path, []byte("{not json"), 0o644))
	case payloadTruncated:
		full, err := json.Marshal(map[string]any{
			"payload": map[string]any{"properties": f.props},
		})
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, full, 0o644))
		// Torn by truncating what was written, not by writing the torn state.
		require.NoError(t, os.Truncate(path, int64(len(full)*2/3)))
	case payloadUnreadable:
		// A directory where the payload belongs reads as unreadable for any
		// user, which chmod 0 does not manage when the tests run as root.
		require.NoError(t, os.MkdirAll(path, 0o755))
	default:
		mkRecoveryPayload(t, lsm, f.name, f.props...)
	}
}

func writeSidecarProps(t *testing.T, lsm string, f sidecarFixture, mode string) {
	t.Helper()
	path := filepath.Join(lsm, ".migrations", f.name, "properties.mig")
	full := strings.Join(f.props, ",")
	switch mode {
	case sidecarAbsent:
		return
	case sidecarZeroByte:
		// What a kill between createFile's OpenFile and its Write leaves:
		// the file exists and holds nothing. Produced by truncating, so the
		// fixture cannot drift from the state it stands for.
		require.NoError(t, os.WriteFile(path, []byte(full), 0o644))
		require.NoError(t, os.Truncate(path, 0))
	case sidecarTruncated:
		require.NoError(t, os.WriteFile(path, []byte(full), 0o644))
		require.NoError(t, os.Truncate(path, int64(len(full)*2/3)))
	case sidecarDeduped:
		require.NoError(t, os.WriteFile(path, []byte(strings.Join(dedupedProps(f.props), ",")), 0o644))
	case sidecarContradicting:
		require.NoError(t, os.WriteFile(path, []byte("unrelated"), 0o644))
	default:
		require.NoError(t, os.WriteFile(path, []byte(full), 0o644))
	}
}

func dedupedProps(props []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, p := range props {
		if seen[p] {
			continue
		}
		seen[p] = true
		out = append(out, p)
	}
	return out
}

// sidecarRebuildsName restates, from the fixture's side and from the bytes
// actually on disk, the corroboration the reader applies: the stored list, as
// [migrationDirWithProps] would join it, is the dir's own name.
func sidecarRebuildsName(lsm string, f sidecarFixture) bool {
	data, err := os.ReadFile(filepath.Join(lsm, ".migrations", f.name, "properties.mig"))
	if err != nil {
		return false
	}
	content := strings.TrimSpace(string(data))
	if content == "" {
		return false
	}
	stored := strings.Split(content, ",")
	sorted := append([]string(nil), stored...)
	sort.Strings(sorted)
	return migrationDirBase(f.name) == f.prefix+"_"+strings.Join(sorted, "_")
}

// payloadIsUnusable is whether payload.mig is on disk but yields no property
// list, which is the only state the sidecar is allowed to answer over.
func payloadIsUnusable(mode string) bool {
	switch mode {
	case payloadCorrupt, payloadTruncated, payloadUnreadable:
		return true
	}
	return false
}

// sidecarMayDecide is the one cell the two readers are allowed to differ in.
func sidecarMayDecide(lsm string, f sidecarFixture, payloadMode string) bool {
	return payloadIsUnusable(payloadMode) && sidecarRebuildsName(lsm, f)
}

// scopesUnderTest is every scope shape the sweep builds for one (property,
// index type): the deletion scope and the widened preserve scope.
func scopesUnderTest(lsm, propName, indexType string) []migrationDirScope {
	base := migrationDirsOf(lsm, nil, propName, indexType)
	return []migrationDirScope{base, base.preserving(indexType)}
}

// Walks the full payload.mig × properties.mig × name × index-type matrix and
// requires both readers to select the same tracker dirs, except where the
// sidecar is allowed to repair an unusable payload.
func TestSidecarPropsMatchTheirPayloadOnlyReader(t *testing.T) {
	var allowedDivergences int

	for _, indexType := range diffIndexTypes {
		for _, payloadMode := range sidecarPayloadModes {
			for _, propsMode := range sidecarPropsModes {
				lsm, fixtures := writeSidecarTree(t, indexType, payloadMode, propsMode, false)

				for _, propName := range sidecarPropNames {
					for i, scope := range scopesUnderTest(lsm, propName, indexType) {
						head := headScoped(scope, fixtures)
						fixed := scope.cachingProps(&taskPropsCache{})

						for _, f := range fixtures {
							want, got := head.matches(f.name), fixed.matches(f.name)
							if want == got {
								continue
							}
							require.True(t,
								sidecarMayDecide(lsm, f, payloadMode),
								"only an unusable payload beside a corroborating sidecar may differ: "+
									"index %s payload %s props %s prop %q preserve %v dir %s (head %v, fixed %v)",
								indexType, payloadMode, propsMode, propName, i == 1, f.name, want, got)
							allowedDivergences++
						}
					}
				}
			}
		}
	}

	require.Positive(t, allowedDivergences,
		"the matrix must actually exercise the repaired-payload cell, else it proves nothing")
}

// Same claim as above, for the pass that decides which generations survive
// as deferred-finalize state — it runs off the same memo but is what stands
// between the sweep and #10675-shape data loss, so it's checked separately.
func TestSidecarPropsPreservePassMatchesItsPayloadOnlyReader(t *testing.T) {
	for _, indexType := range diffIndexTypes {
		for _, payloadMode := range sidecarPayloadModes {
			for _, propsMode := range sidecarPropsModes {
				lsm, fixtures := writeSidecarTree(t, indexType, payloadMode, propsMode, true)

				for _, propName := range sidecarPropNames {
					for _, scope := range scopesUnderTest(lsm, propName, indexType) {
						want := completedMigrationGens(headScoped(scope, fixtures))
						got := completedMigrationGens(scope.cachingProps(&taskPropsCache{}))
						if payloadIsUnusable(payloadMode) {
							// Repaired cells are pinned against an intact
							// payload instead; see the intact-payload test.
							continue
						}
						require.Equal(t, want, got,
							"preserved generations: index %s payload %s props %s prop %q",
							indexType, payloadMode, propsMode, propName)
					}
				}
			}
		}
	}
}

// Compares the survivor set of a real sweep, dir for dir, against a sweep
// driven by the payload-only reader.
func TestSidecarPropsSweepLeavesTheSameDirsBehind(t *testing.T) {
	logger, _ := test.NewNullLogger()

	for _, indexType := range diffIndexTypes {
		for _, payloadMode := range sidecarPayloadModes {
			if payloadIsUnusable(payloadMode) {
				// Pinned against an intact payload instead; see
				// TestSidecarAnswersAsAnIntactPayloadWouldWhereTheStoredPayloadIsUnusable.
				continue
			}
			for _, propsMode := range sidecarPropsModes {
				for _, completed := range []bool{false, true} {
					for _, propName := range sweepPropNames {
						headLSM, fixtures := writeSidecarTree(t, indexType, payloadMode, propsMode, completed)
						cleanStaleMigrationDirsIn(
							headScoped(migrationDirsOf(headLSM, nil, propName, indexType), fixtures),
							logger)

						lsm, _ := writeSidecarTree(t, indexType, payloadMode, propsMode, completed)
						cleanStaleMigrationDirsAt(lsm, propName, indexType, logger)

						require.Equal(t,
							survivingTrackerDirs(t, headLSM), survivingTrackerDirs(t, lsm),
							"index %s payload %s props %s completed %v prop %q",
							indexType, payloadMode, propsMode, completed, propName)
					}
				}
			}
		}
	}
}

// Pins the one approved divergence against an oracle (an intact-payload
// tree) rather than a hand-written expectation: where payload.mig is
// unusable but properties.mig rebuilds the dir's own name, the sweep now
// decides as it would with an intact payload — the payload-only reader
// falls back to guessing from the name instead, which can delete a
// multi-property tracker. The divergence can't be closed here: deciding
// whether to trust the payload means parsing it, the cost this path exists
// to avoid.
func TestSidecarAnswersAsAnIntactPayloadWouldWhereTheStoredPayloadIsUnusable(t *testing.T) {
	logger, _ := test.NewNullLogger()
	var repaired int

	for _, indexType := range diffIndexTypes {
		for _, payloadMode := range []string{payloadCorrupt, payloadTruncated, payloadUnreadable} {
			for _, completed := range []bool{false, true} {
				for _, propName := range sweepPropNames {
					// The sidecar carries the true list; the payload does not.
					lsm, fixtures := writeSidecarTree(t, indexType, payloadMode, sidecarValid, completed)
					cleanStaleMigrationDirsAt(lsm, propName, indexType, logger)

					// The same trackers, with the payload the writer meant to
					// leave behind.
					intactLSM, _ := writeSidecarTree(t, indexType, payloadValid, sidecarAbsent, completed)
					cleanStaleMigrationDirsAt(intactLSM, propName, indexType, logger)

					require.Equal(t,
						survivingTrackerDirs(t, intactLSM), survivingTrackerDirs(t, lsm),
						"a corroborating sidecar must decide as an intact payload does: "+
							"index %s payload %s completed %v prop %q",
						indexType, payloadMode, completed, propName)

					for _, f := range fixtures {
						if sidecarMayDecide(lsm, f, payloadMode) {
							repaired++
						}
					}
				}
			}
		}
	}

	require.Positive(t, repaired, "no tracker was actually repaired by its sidecar")
}

// TestSidecarIsRefusedWhereItCannotRebuildTheDirName pins the corroboration
// itself: for a sidecar the reader must reject, the answer is the payload's,
// byte for byte, whatever the payload says.
func TestSidecarIsRefusedWhereItCannotRebuildTheDirName(t *testing.T) {
	rejected := map[string]int{}

	for _, indexType := range diffIndexTypes {
		for _, propsMode := range []string{sidecarZeroByte, sidecarTruncated, sidecarContradicting, sidecarDeduped} {
			for _, payloadMode := range sidecarPayloadModes {
				lsm, fixtures := writeSidecarTree(t, indexType, payloadMode, propsMode, false)

				for _, f := range fixtures {
					if sidecarRebuildsName(lsm, f) {
						// sidecarDeduped only tears the duplicate-token dir;
						// elsewhere deduplication is a no-op.
						continue
					}
					rejected[propsMode]++
					migDir := filepath.Join(lsm, ".migrations", f.name)
					got, _ := readTaskProps(migDir, migrationDirPrefixesForIndexType(indexType))
					require.Equal(t, headTaskProps(migDir), got,
						"index %s payload %s props %s dir %s",
						indexType, payloadMode, propsMode, f.name)
				}
			}
		}
	}

	for _, mode := range []string{sidecarZeroByte, sidecarTruncated, sidecarContradicting, sidecarDeduped} {
		require.Positive(t, rejected[mode], "mode %s never produced a rejected sidecar", mode)
	}
}

// Pins the read alone: a zero-byte properties.mig still costs a payload
// read and still answers with every property the task recorded.
func TestZeroByteSidecarDoesNotNarrowTheTrackersProperties(t *testing.T) {
	const indexType = "filterable"
	lsm, fixtures := writeSidecarTree(t, indexType, payloadValid, sidecarZeroByte, false)

	var checked int
	for _, f := range fixtures {
		if len(f.props) < 2 {
			continue
		}
		checked++
		path := filepath.Join(lsm, ".migrations", f.name, "properties.mig")
		info, err := os.Stat(path)
		require.NoError(t, err, "the torn sidecar must exist")
		require.Zero(t, info.Size(), "the torn sidecar must be empty")

		migDir := filepath.Join(lsm, ".migrations", f.name)
		got, readPayload := readTaskProps(migDir, migrationDirPrefixesForIndexType(indexType))
		require.True(t, readPayload, "an empty sidecar must not spare the payload")
		require.True(t, got.ok)
		require.Equal(t, f.props, got.props,
			"the tracker still belongs to every property its task recorded")
	}
	require.Positive(t, checked)
}

// Pins the distinction the whole change rests on: a missing payload.mig and
// an unusable one are not the same state — "no payload" is what lets the
// unloaded-shard gate call a shard clean, so a sidecar may not forge that in
// either direction, and is not even read where the payload is absent.
func TestAbsentPayloadStaysAbsentHoweverGoodTheSidecarIs(t *testing.T) {
	for _, indexType := range diffIndexTypes {
		for _, propsMode := range sidecarPropsModes {
			lsm, fixtures := writeSidecarTree(t, indexType, payloadAbsent, propsMode, false)
			for _, f := range fixtures {
				migDir := filepath.Join(lsm, ".migrations", f.name)
				got, readPayload := readTaskProps(migDir, migrationDirPrefixesForIndexType(indexType))

				require.Equal(t, taskProps{}, got,
					"a tracker with no payload records nothing: index %s props %s dir %s",
					indexType, propsMode, f.name)
				require.False(t, got.ok)
				require.False(t, got.unreadable)
				require.False(t, readPayload,
					"a payload that is not on disk is never opened, so it must not count as a read")
				require.Equal(t, headTaskProps(migDir), got)
			}
		}
	}
}

// TestSidecarAnswerDoesNotDependOnWhichScopeAsked pins the assumption the memo
// rests on: it is keyed by directory alone, so a tracker must read the same
// under every scope that can ask about it. filterable_to_rangeable is in two
// index types' prefix sets, which is the case that makes this reachable.
func TestSidecarAnswerDoesNotDependOnWhichScopeAsked(t *testing.T) {
	for _, payloadMode := range sidecarPayloadModes {
		for _, propsMode := range sidecarPropsModes {
			for _, indexType := range diffIndexTypes {
				lsm, fixtures := writeSidecarTree(t, indexType, payloadMode, propsMode, false)
				for _, f := range fixtures {
					migDir := filepath.Join(lsm, ".migrations", f.name)
					want, _ := readTaskProps(migDir, migrationDirPrefixesForIndexType(indexType))
					for _, other := range diffIndexTypes {
						prefixes := migrationDirPrefixesForIndexType(other)
						if !(migrationDirScope{prefixes: prefixes}).hasStrategyPrefix(migrationDirBase(f.name)) {
							// Only a scope that owns the prefix ever asks.
							continue
						}
						got, _ := readTaskProps(migDir, prefixes)
						require.Equal(t, want, got,
							"dir %s read differently as %s than as %s", f.name, other, indexType)
					}
				}
			}
		}
	}
}
