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
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// Tests for the per-migration generation helpers added for
// https://github.com/weaviate/weaviate/issues/10675. The functions under test live in
// inverted_reindex_finalize.go and inverted_reindex_strategy_dir_names.go.

func TestParseMigrationDirName(t *testing.T) {
	cases := []struct {
		name       string
		input      string
		wantPrefix string
		wantGen    int
		wantOK     bool
	}{
		{"per-prop retokenize gen 1", "searchable_retokenize_text_1", "searchable_retokenize_text", 1, true},
		{"per-prop retokenize gen 42", "filterable_retokenize_email_42", "filterable_retokenize_email", 42, true},
		{"per-prop with underscore-y prop name", "enable_filterable_prop_with_underscores_3", "enable_filterable_prop_with_underscores", 3, true},
		{"class-level gen", "searchable_map_to_blockmax_1", "searchable_map_to_blockmax", 1, true},
		{"class-level roaringset gen", "filterable_roaringset_refresh_5", "filterable_roaringset_refresh", 5, true},
		{"missing gen suffix", "searchable_retokenize_text", "", 0, false},
		{"trailing underscore (no digit)", "searchable_retokenize_text_", "", 0, false},
		{"non-integer suffix", "searchable_retokenize_text_abc", "", 0, false},
		{"zero gen rejected", "searchable_retokenize_text_0", "", 0, false},
		{"negative gen rejected", "searchable_retokenize_text_-1", "", 0, false},
		{"empty string", "", "", 0, false},
		{"single token", "foo", "", 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			prefix, gen, ok := parseMigrationDirName(c.input)
			require.Equal(t, c.wantOK, ok, "ok mismatch")
			if c.wantOK {
				require.Equal(t, c.wantPrefix, prefix, "prefix mismatch")
				require.Equal(t, c.wantGen, gen, "gen mismatch")
			}
		})
	}
}

func TestGenSuffix(t *testing.T) {
	require.Equal(t, "_1", genSuffix(1))
	require.Equal(t, "_42", genSuffix(42))
	require.Equal(t, "_0", genSuffix(0)) // 0 is reserved (canonical) but genSuffix still emits — callers don't pass 0
}

// fakeMigrationsDir creates a temp .migrations/ tree with the given dir
// names and returns the parent lsmPath.
func fakeMigrationsDir(t *testing.T, dirs []string) string {
	t.Helper()
	lsmPath := t.TempDir()
	migsDir := filepath.Join(lsmPath, ".migrations")
	require.NoError(t, os.MkdirAll(migsDir, 0o755))
	for _, d := range dirs {
		require.NoError(t, os.MkdirAll(filepath.Join(migsDir, d), 0o755))
	}
	return lsmPath
}

// touchSentinel creates an empty file at the given path. Used to
// simulate sentinel files (started.mig, tidied.mig, etc.) in tests.
func touchSentinel(t *testing.T, path string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, nil, 0o644))
}

func TestNextMigrationGeneration_EmptyDisk(t *testing.T) {
	lsmPath := fakeMigrationsDir(t, nil)
	got := nextMigrationGeneration(lsmPath, MigrationDirPrefixSearchableRetokenize, "_text")
	require.Equal(t, 1, got, "fresh disk should pick gen 1")
}

func TestNextMigrationGeneration_NoMatchingPrefix(t *testing.T) {
	// Existing dirs for a DIFFERENT prop / strategy don't bump the
	// counter for ours.
	lsmPath := fakeMigrationsDir(t, []string{
		"searchable_retokenize_otherprop_1",
		"filterable_retokenize_text_2",
		"enable_filterable_text_5",
	})
	got := nextMigrationGeneration(lsmPath, MigrationDirPrefixSearchableRetokenize, "_text")
	require.Equal(t, 1, got, "no matching prefix means fresh gen 1")
}

func TestNextMigrationGeneration_ContiguousGens(t *testing.T) {
	lsmPath := fakeMigrationsDir(t, []string{
		"searchable_retokenize_text_1",
		"searchable_retokenize_text_2",
		"searchable_retokenize_text_3",
	})
	got := nextMigrationGeneration(lsmPath, MigrationDirPrefixSearchableRetokenize, "_text")
	require.Equal(t, 4, got, "max+1 across contiguous gens")
}

func TestNextMigrationGeneration_NonContiguousGens(t *testing.T) {
	// If gens have gaps (e.g. trim removed some but not the highest), we
	// still pick max+1 — never reuse a gap.
	lsmPath := fakeMigrationsDir(t, []string{
		"searchable_retokenize_text_1",
		"searchable_retokenize_text_5",
		"searchable_retokenize_text_7",
	})
	got := nextMigrationGeneration(lsmPath, MigrationDirPrefixSearchableRetokenize, "_text")
	require.Equal(t, 8, got, "non-contiguous gens still pick max+1")
}

func TestNextMigrationGeneration_MixedPrefixesScopedCorrectly(t *testing.T) {
	lsmPath := fakeMigrationsDir(t, []string{
		"searchable_retokenize_text_3",
		"searchable_retokenize_other_7", // different prop in same prefix
		"filterable_retokenize_text_10", // different prefix, same prop
	})
	require.Equal(t, 4, nextMigrationGeneration(lsmPath, MigrationDirPrefixSearchableRetokenize, "_text"))
	require.Equal(t, 8, nextMigrationGeneration(lsmPath, MigrationDirPrefixSearchableRetokenize, "_other"))
	require.Equal(t, 11, nextMigrationGeneration(lsmPath, MigrationDirPrefixFilterableRetokenize, "_text"))
	require.Equal(t, 1, nextMigrationGeneration(lsmPath, MigrationDirPrefixSearchableRetokenize, "_neverused"))
}

func TestMaxMigrationGeneration_NoExisting(t *testing.T) {
	lsmPath := fakeMigrationsDir(t, nil)
	require.Equal(t, 0, maxMigrationGeneration(lsmPath, MigrationDirPrefixSearchableRetokenize, "_text"))
}

func TestMaxMigrationGeneration_Existing(t *testing.T) {
	lsmPath := fakeMigrationsDir(t, []string{
		"searchable_retokenize_text_2",
		"searchable_retokenize_text_5",
	})
	require.Equal(t, 5, maxMigrationGeneration(lsmPath, MigrationDirPrefixSearchableRetokenize, "_text"))
}

// TestFinalizeCompletedMigrations_MultiGen_PickHighestTidied verifies
// that when multiple tidied gens are on disk (e.g. a crash before the
// in-process trim could run), restart-finalize picks the highest one
// and cleans the rest.
func TestFinalizeCompletedMigrations_MultiGen_PickHighestTidied(t *testing.T) {
	lsmPath := t.TempDir()
	migsDir := filepath.Join(lsmPath, ".migrations")
	require.NoError(t, os.MkdirAll(migsDir, 0o755))

	// Set up three tidied gens for the same (prop, indexType) tuple.
	// Each carries the sentinel files finalize expects + a
	// properties.mig listing the prop.
	for _, gen := range []int{1, 2, 3} {
		dir := filepath.Join(migsDir, "searchable_retokenize_text_"+itoa(gen))
		require.NoError(t, os.MkdirAll(dir, 0o755))
		touchSentinel(t, filepath.Join(dir, "swapped.mig"))
		touchSentinel(t, filepath.Join(dir, "tidied.mig"))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "properties.mig"), []byte("text"), 0o644))
	}

	// Create the on-disk ingest dirs each gen would have. Gen 3 is the
	// "winner" — its ingest dir should be renamed to the canonical
	// `property_text_searchable`. Older gens' dirs should be removed.
	for _, gen := range []int{1, 2, 3} {
		require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, "property_text_searchable__retokenize_ingest_"+itoa(gen)), 0o755))
		require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, "property_text_searchable__retokenize_backup_"+itoa(gen)), 0o755))
	}
	// Drop a marker file in gen-3's ingest so we can confirm it survived
	// the rename to canonical.
	winnerMarker := []byte("gen-3-data")
	require.NoError(t, os.WriteFile(
		filepath.Join(lsmPath, "property_text_searchable__retokenize_ingest_3", "segment.db"),
		winnerMarker, 0o644))

	logger, _ := test.NewNullLogger()
	FinalizeCompletedMigrations(lsmPath, logger)

	// Canonical main dir should exist and contain gen-3's marker.
	canonical := filepath.Join(lsmPath, "property_text_searchable")
	got, err := os.ReadFile(filepath.Join(canonical, "segment.db"))
	require.NoError(t, err, "canonical dir should contain gen-3's segment")
	require.Equal(t, winnerMarker, got)

	// Older gen ingest dirs and all backup dirs should be gone.
	for _, gen := range []int{1, 2, 3} {
		_, err := os.Stat(filepath.Join(lsmPath, "property_text_searchable__retokenize_ingest_"+itoa(gen)))
		require.True(t, os.IsNotExist(err), "ingest dir for gen %d should be removed", gen)
		_, err = os.Stat(filepath.Join(lsmPath, "property_text_searchable__retokenize_backup_"+itoa(gen)))
		require.True(t, os.IsNotExist(err), "backup dir for gen %d should be removed", gen)
	}

	// All tracker dirs should be gone — gen 3's was promoted, older
	// gens were cleaned as stale.
	migEntries, err := os.ReadDir(migsDir)
	require.NoError(t, err)
	require.Empty(t, migEntries, "tracker dirs should all be removed")
}

// TestFinalizeCompletedMigrations_TidiedPlusInFlight verifies that a
// gen > highest-tidied is left alone (in-flight, recovery's job),
// while the highest-tidied is still promoted.
func TestFinalizeCompletedMigrations_TidiedPlusInFlight(t *testing.T) {
	lsmPath := t.TempDir()
	migsDir := filepath.Join(lsmPath, ".migrations")
	require.NoError(t, os.MkdirAll(migsDir, 0o755))

	// Gen 1: tidied (the "highest tidied").
	gen1 := filepath.Join(migsDir, "searchable_retokenize_text_1")
	require.NoError(t, os.MkdirAll(gen1, 0o755))
	touchSentinel(t, filepath.Join(gen1, "swapped.mig"))
	touchSentinel(t, filepath.Join(gen1, "tidied.mig"))
	require.NoError(t, os.WriteFile(filepath.Join(gen1, "properties.mig"), []byte("text"), 0o644))

	// Gen 2: in-flight (started but not yet tidied).
	gen2 := filepath.Join(migsDir, "searchable_retokenize_text_2")
	require.NoError(t, os.MkdirAll(gen2, 0o755))
	touchSentinel(t, filepath.Join(gen2, "started.mig"))
	require.NoError(t, os.WriteFile(filepath.Join(gen2, "properties.mig"), []byte("text"), 0o644))

	require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, "property_text_searchable__retokenize_ingest_1"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, "property_text_searchable__retokenize_ingest_2"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, "property_text_searchable__retokenize_reindex_2"), 0o755))

	logger, _ := test.NewNullLogger()
	FinalizeCompletedMigrations(lsmPath, logger)

	// Gen 1 finalized → canonical dir exists.
	_, err := os.Stat(filepath.Join(lsmPath, "property_text_searchable"))
	require.NoError(t, err, "canonical dir should exist after gen-1 finalize")
	// Gen 1 sidecar/tracker dirs gone.
	_, err = os.Stat(filepath.Join(lsmPath, "property_text_searchable__retokenize_ingest_1"))
	require.True(t, os.IsNotExist(err), "gen-1 ingest dir should be renamed away")
	_, err = os.Stat(gen1)
	require.True(t, os.IsNotExist(err), "gen-1 tracker dir should be removed")
	// Gen 2 left alone for recovery.
	_, err = os.Stat(gen2)
	require.NoError(t, err, "gen-2 tracker dir should remain for recovery")
	_, err = os.Stat(filepath.Join(lsmPath, "property_text_searchable__retokenize_ingest_2"))
	require.NoError(t, err, "gen-2 ingest dir should remain for recovery")
	_, err = os.Stat(filepath.Join(lsmPath, "property_text_searchable__retokenize_reindex_2"))
	require.NoError(t, err, "gen-2 reindex dir should remain for recovery")
}

// TestFinalizeCompletedMigrations_OnlyUntidiedIsNoOp verifies that if
// no migration has tidied yet, finalize touches nothing (recovery
// owns).
func TestFinalizeCompletedMigrations_OnlyUntidiedIsNoOp(t *testing.T) {
	lsmPath := t.TempDir()
	migsDir := filepath.Join(lsmPath, ".migrations")
	require.NoError(t, os.MkdirAll(migsDir, 0o755))

	gen1 := filepath.Join(migsDir, "searchable_retokenize_text_1")
	require.NoError(t, os.MkdirAll(gen1, 0o755))
	touchSentinel(t, filepath.Join(gen1, "started.mig"))
	require.NoError(t, os.WriteFile(filepath.Join(gen1, "properties.mig"), []byte("text"), 0o644))

	require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, "property_text_searchable__retokenize_ingest_1"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, "property_text_searchable__retokenize_reindex_1"), 0o755))

	logger, _ := test.NewNullLogger()
	FinalizeCompletedMigrations(lsmPath, logger)

	// Tracker dir still there.
	_, err := os.Stat(gen1)
	require.NoError(t, err, "untidied tracker dir should remain")
	// Sidecars still there.
	_, err = os.Stat(filepath.Join(lsmPath, "property_text_searchable__retokenize_ingest_1"))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(lsmPath, "property_text_searchable__retokenize_reindex_1"))
	require.NoError(t, err)
	// No canonical dir created.
	_, err = os.Stat(filepath.Join(lsmPath, "property_text_searchable"))
	require.True(t, os.IsNotExist(err), "canonical dir must not be created for untidied state")
}

// -----------------------------------------------------------------------------
// Recovery path: merged.mig set but tidied.mig missing
// -----------------------------------------------------------------------------
//
// These tests pin the recovery path added to FinalizeCompletedMigrations
// for the failure mode behind RestartMatrix R2 / R2b:
//
//   - T1 word→field completes successfully on all nodes (gen-1 tidied).
//   - T2 field→word starts. On some node, the in-process runtime swap
//     dies after `markMerged` but before `markTidied` (e.g. the
//     SwapBucketPointer pre-step panicked, ctx was canceled, or the
//     process was killed mid-swap by the test's rolling restart).
//   - The FSM-level task is FINISHED (the reindex iteration's unit
//     completion was reported BEFORE the swap), so the cluster-wide
//     OnTaskCompleted schema flip already RAFT-committed `tokenization
//     = "word"`.
//   - On disk this node has tracker_2 with merged.mig but NOT
//     tidied.mig; the gen-2 ingest dir holds the new word-tokenized
//     dataset; gen-1's tidied tracker is still present (because
//     T2's end-of-swap trim never ran).
//
// Without recovery, FinalizeCompletedMigrations would promote gen-1
// (highest tidied) and produce the #10675-shape divergence: schema
// says "word" but the bucket on this node has field-tokenized data, so
// "alpha" queries return 0 docs while other replicas return 6.
//
// The recovery path writes swapped.mig + tidied.mig retroactively for
// the highest merged gen so the existing ingest→canonical promotion
// runs on the correct (newest, fully-prepended) generation.

// TestFinalizeCompletedMigrations_MergedButNotTidied_Recovers is the
// minimal pin for the R2/R2b bug: gen-1 tidied, gen-2 merged-but-not-
// tidied, gen-2 must win the promotion.
func TestFinalizeCompletedMigrations_MergedButNotTidied_Recovers(t *testing.T) {
	lsmPath := t.TempDir()
	migsDir := filepath.Join(lsmPath, ".migrations")
	require.NoError(t, os.MkdirAll(migsDir, 0o755))

	// Gen 1: fully tidied (T1 succeeded).
	gen1 := filepath.Join(migsDir, "searchable_retokenize_text_1")
	require.NoError(t, os.MkdirAll(gen1, 0o755))
	touchSentinel(t, filepath.Join(gen1, "merged.mig"))
	touchSentinel(t, filepath.Join(gen1, "swapped.mig"))
	touchSentinel(t, filepath.Join(gen1, "tidied.mig"))
	require.NoError(t, os.WriteFile(filepath.Join(gen1, "properties.mig"), []byte("text"), 0o644))

	// Gen 2: merged but NOT tidied (the recovery case).
	gen2 := filepath.Join(migsDir, "searchable_retokenize_text_2")
	require.NoError(t, os.MkdirAll(gen2, 0o755))
	touchSentinel(t, filepath.Join(gen2, "started.mig"))
	touchSentinel(t, filepath.Join(gen2, "reindexed.mig"))
	touchSentinel(t, filepath.Join(gen2, "prepended.mig"))
	touchSentinel(t, filepath.Join(gen2, "merged.mig"))
	require.NoError(t, os.WriteFile(filepath.Join(gen2, "properties.mig"), []byte("text"), 0o644))

	// Gen-1 ingest dir holds the previous live-main data (field-tokenized
	// in the real bug). Gen-2 ingest holds the new merged data
	// (word-tokenized — the correct state under the cluster-wide schema).
	require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, "property_text_searchable__retokenize_ingest_1"), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(lsmPath, "property_text_searchable__retokenize_ingest_1", "segment.db"),
		[]byte("gen-1-stale-data"), 0o644))

	require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, "property_text_searchable__retokenize_ingest_2"), 0o755))
	gen2Marker := []byte("gen-2-merged-data")
	require.NoError(t, os.WriteFile(
		filepath.Join(lsmPath, "property_text_searchable__retokenize_ingest_2", "segment.db"),
		gen2Marker, 0o644))

	logger, _ := test.NewNullLogger()
	FinalizeCompletedMigrations(lsmPath, logger)

	// Canonical dir must contain gen-2's marker, NOT gen-1's stale data.
	canonical := filepath.Join(lsmPath, "property_text_searchable")
	got, err := os.ReadFile(filepath.Join(canonical, "segment.db"))
	require.NoError(t, err, "canonical dir should exist after recovery promotion")
	require.Equal(t, gen2Marker, got,
		"canonical must contain gen-2 (merged) data, not gen-1 (tidied-but-stale) data")

	// All ingest sidecars cleaned.
	for _, gen := range []int{1, 2} {
		_, err := os.Stat(filepath.Join(lsmPath, "property_text_searchable__retokenize_ingest_"+itoa(gen)))
		require.True(t, os.IsNotExist(err),
			"gen %d ingest dir must be removed after recovery (renamed or cleaned)", gen)
	}

	// Tracker dirs gone.
	migEntries, err := os.ReadDir(migsDir)
	require.NoError(t, err)
	require.Empty(t, migEntries, "both tracker dirs must be removed after recovery finalize")
}

// TestFinalizeCompletedMigrations_MergedOnly_NoPriorTidied_Recovers
// pins the fresh-cluster variant: gen-1 is the FIRST migration and its
// swap crashed post-merge. Without recovery the canonical bucket never
// gets created and the shard starts up with an empty bucket directory.
func TestFinalizeCompletedMigrations_MergedOnly_NoPriorTidied_Recovers(t *testing.T) {
	lsmPath := t.TempDir()
	migsDir := filepath.Join(lsmPath, ".migrations")
	require.NoError(t, os.MkdirAll(migsDir, 0o755))

	gen1 := filepath.Join(migsDir, "searchable_retokenize_text_1")
	require.NoError(t, os.MkdirAll(gen1, 0o755))
	touchSentinel(t, filepath.Join(gen1, "reindexed.mig"))
	touchSentinel(t, filepath.Join(gen1, "prepended.mig"))
	touchSentinel(t, filepath.Join(gen1, "merged.mig"))
	require.NoError(t, os.WriteFile(filepath.Join(gen1, "properties.mig"), []byte("text"), 0o644))

	require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, "property_text_searchable__retokenize_ingest_1"), 0o755))
	marker := []byte("gen-1-recovered")
	require.NoError(t, os.WriteFile(
		filepath.Join(lsmPath, "property_text_searchable__retokenize_ingest_1", "segment.db"),
		marker, 0o644))

	logger, _ := test.NewNullLogger()
	FinalizeCompletedMigrations(lsmPath, logger)

	canonical := filepath.Join(lsmPath, "property_text_searchable")
	got, err := os.ReadFile(filepath.Join(canonical, "segment.db"))
	require.NoError(t, err, "canonical dir should be created from gen-1 ingest")
	require.Equal(t, marker, got)
}

// TestFinalizeCompletedMigrations_TidiedHigherThanMerged_PicksTidied
// guards against a regression where the recovery path picks merged
// instead of tidied when tidied is at the same or higher gen. We never
// want to skip a successfully-completed migration in favor of an
// earlier merged-only one.
func TestFinalizeCompletedMigrations_TidiedHigherThanMerged_PicksTidied(t *testing.T) {
	lsmPath := t.TempDir()
	migsDir := filepath.Join(lsmPath, ".migrations")
	require.NoError(t, os.MkdirAll(migsDir, 0o755))

	// Gen 1: merged-only (a half-completed earlier attempt).
	gen1 := filepath.Join(migsDir, "searchable_retokenize_text_1")
	require.NoError(t, os.MkdirAll(gen1, 0o755))
	touchSentinel(t, filepath.Join(gen1, "merged.mig"))
	require.NoError(t, os.WriteFile(filepath.Join(gen1, "properties.mig"), []byte("text"), 0o644))

	// Gen 2: fully tidied.
	gen2 := filepath.Join(migsDir, "searchable_retokenize_text_2")
	require.NoError(t, os.MkdirAll(gen2, 0o755))
	touchSentinel(t, filepath.Join(gen2, "merged.mig"))
	touchSentinel(t, filepath.Join(gen2, "swapped.mig"))
	touchSentinel(t, filepath.Join(gen2, "tidied.mig"))
	require.NoError(t, os.WriteFile(filepath.Join(gen2, "properties.mig"), []byte("text"), 0o644))

	require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, "property_text_searchable__retokenize_ingest_1"), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(lsmPath, "property_text_searchable__retokenize_ingest_1", "segment.db"),
		[]byte("gen-1-stale"), 0o644))

	require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, "property_text_searchable__retokenize_ingest_2"), 0o755))
	winner := []byte("gen-2-tidied-winner")
	require.NoError(t, os.WriteFile(
		filepath.Join(lsmPath, "property_text_searchable__retokenize_ingest_2", "segment.db"),
		winner, 0o644))

	logger, _ := test.NewNullLogger()
	FinalizeCompletedMigrations(lsmPath, logger)

	got, err := os.ReadFile(filepath.Join(lsmPath, "property_text_searchable", "segment.db"))
	require.NoError(t, err)
	require.Equal(t, winner, got,
		"highest gen with tidied must win when merged-only is at a lower gen")
}

// TestFinalizeCompletedMigrations_RecoveryWritesMissingSentinels
// asserts the sentinel-writing side effect of the recovery path so a
// later restart sees a self-consistent tracker (idempotent re-finalize).
// We invoke finalize on a tracker that lacks swapped/tidied, then
// re-stat the (now-removed) tracker via the on-disk artifacts: the
// canonical dir exists, the tracker is gone, and the sentinels were
// written before tracker removal (verified indirectly by checking the
// canonical was created — without sentinel writes finalizeMigrationDir
// returns early without renaming).
func TestFinalizeCompletedMigrations_RecoveryWritesMissingSentinels(t *testing.T) {
	lsmPath := t.TempDir()
	migsDir := filepath.Join(lsmPath, ".migrations")
	require.NoError(t, os.MkdirAll(migsDir, 0o755))

	gen1 := filepath.Join(migsDir, "searchable_retokenize_text_1")
	require.NoError(t, os.MkdirAll(gen1, 0o755))
	touchSentinel(t, filepath.Join(gen1, "merged.mig")) // ONLY merged, no swapped/tidied
	require.NoError(t, os.WriteFile(filepath.Join(gen1, "properties.mig"), []byte("text"), 0o644))

	require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, "property_text_searchable__retokenize_ingest_1"), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(lsmPath, "property_text_searchable__retokenize_ingest_1", "seg.db"),
		[]byte("data"), 0o644))

	logger, _ := test.NewNullLogger()
	FinalizeCompletedMigrations(lsmPath, logger)

	// If sentinels weren't written before finalizeMigrationDir ran, the
	// canonical dir would not be created (finalizeMigrationDir returns
	// early on missing swapped/tidied). Check canonical exists.
	_, err := os.Stat(filepath.Join(lsmPath, "property_text_searchable", "seg.db"))
	require.NoError(t, err,
		"recovery path must write swapped/tidied sentinels so finalizeMigrationDir promotes")
}

// TestFinalizeCompletedMigrations_StartedOnlyNotPromoted is the
// safety guard: a tracker that's been started/reindexed/prepended but
// NOT YET merged is still in an earlier stage. The reindex iteration
// may have completed, but prepend may not have. Promoting the ingest
// dir would be unsafe because it could be missing reindex-bucket
// segments that PrependSegmentsFromBucket hasn't moved yet. Verify
// such state is left alone for the in-flight recovery path.
func TestFinalizeCompletedMigrations_StartedOnlyNotPromoted(t *testing.T) {
	lsmPath := t.TempDir()
	migsDir := filepath.Join(lsmPath, ".migrations")
	require.NoError(t, os.MkdirAll(migsDir, 0o755))

	gen1 := filepath.Join(migsDir, "searchable_retokenize_text_1")
	require.NoError(t, os.MkdirAll(gen1, 0o755))
	touchSentinel(t, filepath.Join(gen1, "started.mig"))
	touchSentinel(t, filepath.Join(gen1, "reindexed.mig"))
	touchSentinel(t, filepath.Join(gen1, "prepended.mig"))
	// NO merged.mig
	require.NoError(t, os.WriteFile(filepath.Join(gen1, "properties.mig"), []byte("text"), 0o644))

	require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, "property_text_searchable__retokenize_ingest_1"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, "property_text_searchable__retokenize_reindex_1"), 0o755))

	logger, _ := test.NewNullLogger()
	FinalizeCompletedMigrations(lsmPath, logger)

	// Tracker untouched.
	_, err := os.Stat(gen1)
	require.NoError(t, err, "started/reindexed/prepended-only tracker must be left for recovery")
	// Sidecars untouched.
	_, err = os.Stat(filepath.Join(lsmPath, "property_text_searchable__retokenize_ingest_1"))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(lsmPath, "property_text_searchable__retokenize_reindex_1"))
	require.NoError(t, err)
	// No canonical created.
	_, err = os.Stat(filepath.Join(lsmPath, "property_text_searchable"))
	require.True(t, os.IsNotExist(err),
		"no canonical promotion until merged.mig is set — otherwise we'd promote a partial ingest")
}

// TestFinalizeCompletedMigrations_RecoveryAcrossNamespaces verifies
// recovery applies independently per namespace: a filterable tracker
// in recovery state must not interfere with a searchable tracker in a
// different (tidied) state on the same property.
func TestFinalizeCompletedMigrations_RecoveryAcrossNamespaces(t *testing.T) {
	lsmPath := t.TempDir()
	migsDir := filepath.Join(lsmPath, ".migrations")
	require.NoError(t, os.MkdirAll(migsDir, 0o755))

	// Searchable: gen-1 tidied (normal path).
	s1 := filepath.Join(migsDir, "searchable_retokenize_text_1")
	require.NoError(t, os.MkdirAll(s1, 0o755))
	touchSentinel(t, filepath.Join(s1, "merged.mig"))
	touchSentinel(t, filepath.Join(s1, "swapped.mig"))
	touchSentinel(t, filepath.Join(s1, "tidied.mig"))
	require.NoError(t, os.WriteFile(filepath.Join(s1, "properties.mig"), []byte("text"), 0o644))

	// Filterable: gen-1 merged-only (recovery path).
	f1 := filepath.Join(migsDir, "filterable_retokenize_text_1")
	require.NoError(t, os.MkdirAll(f1, 0o755))
	touchSentinel(t, filepath.Join(f1, "merged.mig"))
	require.NoError(t, os.WriteFile(filepath.Join(f1, "properties.mig"), []byte("text"), 0o644))

	require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, "property_text_searchable__retokenize_ingest_1"), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(lsmPath, "property_text_searchable__retokenize_ingest_1", "s.db"),
		[]byte("searchable-data"), 0o644))

	require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, "property_text__filt_retokenize_ingest_1"), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(lsmPath, "property_text__filt_retokenize_ingest_1", "f.db"),
		[]byte("filterable-data"), 0o644))

	logger, _ := test.NewNullLogger()
	FinalizeCompletedMigrations(lsmPath, logger)

	// Both canonical dirs should now exist with their respective data.
	sBytes, err := os.ReadFile(filepath.Join(lsmPath, "property_text_searchable", "s.db"))
	require.NoError(t, err)
	require.Equal(t, []byte("searchable-data"), sBytes)

	fBytes, err := os.ReadFile(filepath.Join(lsmPath, "property_text", "f.db"))
	require.NoError(t, err)
	require.Equal(t, []byte("filterable-data"), fBytes)
}

// TestFinalizeCompletedMigrations_IdempotentAfterRecovery verifies
// running finalize a second time after a successful recovery is a
// no-op — important because the test container's restart sequence
// effectively re-invokes finalize on every node start.
func TestFinalizeCompletedMigrations_IdempotentAfterRecovery(t *testing.T) {
	lsmPath := t.TempDir()
	migsDir := filepath.Join(lsmPath, ".migrations")
	require.NoError(t, os.MkdirAll(migsDir, 0o755))

	gen1 := filepath.Join(migsDir, "searchable_retokenize_text_1")
	require.NoError(t, os.MkdirAll(gen1, 0o755))
	touchSentinel(t, filepath.Join(gen1, "merged.mig"))
	require.NoError(t, os.WriteFile(filepath.Join(gen1, "properties.mig"), []byte("text"), 0o644))

	require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, "property_text_searchable__retokenize_ingest_1"), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(lsmPath, "property_text_searchable__retokenize_ingest_1", "seg.db"),
		[]byte("data"), 0o644))

	logger, _ := test.NewNullLogger()
	FinalizeCompletedMigrations(lsmPath, logger)

	// Second call should be a complete no-op now that nothing remains in .migrations.
	FinalizeCompletedMigrations(lsmPath, logger)

	got, err := os.ReadFile(filepath.Join(lsmPath, "property_text_searchable", "seg.db"))
	require.NoError(t, err)
	require.Equal(t, []byte("data"), got)

	migEntries, err := os.ReadDir(migsDir)
	require.NoError(t, err)
	require.Empty(t, migEntries, "no trackers should remain after a successful recovery")
}

// TestFinalizeCompletedMigrations_ConcurrentMultiPropMigrations_Converge
// pins https://github.com/weaviate/0-weaviate-issues/issues/214 Test Gap unit-level item 1: a single
// shard's .migrations/ directory carrying multiple in-flight migrations
// on different properties simultaneously, each at a different stage of
// half-finalization, must converge to the same final on-disk shape as
// if each migration had finished cleanly in isolation.
//
// Failure shape this catches: a refactor of the per-namespace
// promotion loop that accidentally short-circuits on the FIRST
// namespace's recovery would leave later namespaces' merged dirs
// orphaned. The reindex provider's rehydrate path expects either a
// fully promoted canonical dir or an in-flight tracker; a half-orphaned
// in-between state silently serves stale data.
//
// Layout exercised:
//
//   - alpha: change-tokenization with both searchable+filterable
//     indexes, both at merged-but-not-tidied gen 1 (the
//     crash-during-FINALIZING-on-step-3 shape).
//
//   - beta: enable-filterable on a fresh-rangeable property, also at
//     merged-but-not-tidied gen 1 (different strategy, different
//     namespace prefix).
//
//   - gamma: enable-rangeable, merged-but-not-tidied gen 1 (format-only
//     migration; the recovery path must still write swap/tidied
//     sentinels for it because finalize doesn't distinguish format-only
//     vs semantic on the recovery path).
func TestFinalizeCompletedMigrations_ConcurrentMultiPropMigrations_Converge(t *testing.T) {
	lsmPath := t.TempDir()
	migsDir := filepath.Join(lsmPath, ".migrations")
	require.NoError(t, os.MkdirAll(migsDir, 0o755))

	// alpha — change-tokenization, both indexes at merged-but-not-tidied gen 1.
	alphaSearchable := filepath.Join(migsDir, "searchable_retokenize_alpha_1")
	require.NoError(t, os.MkdirAll(alphaSearchable, 0o755))
	touchSentinel(t, filepath.Join(alphaSearchable, "merged.mig"))
	require.NoError(t, os.WriteFile(filepath.Join(alphaSearchable, "properties.mig"), []byte("alpha"), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, "property_alpha_searchable__retokenize_ingest_1"), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(lsmPath, "property_alpha_searchable__retokenize_ingest_1", "seg.db"),
		[]byte("alpha-searchable-NEW"), 0o644))

	alphaFilterable := filepath.Join(migsDir, "filterable_retokenize_alpha_1")
	require.NoError(t, os.MkdirAll(alphaFilterable, 0o755))
	touchSentinel(t, filepath.Join(alphaFilterable, "merged.mig"))
	require.NoError(t, os.WriteFile(filepath.Join(alphaFilterable, "properties.mig"), []byte("alpha"), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, "property_alpha__filt_retokenize_ingest_1"), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(lsmPath, "property_alpha__filt_retokenize_ingest_1", "seg.db"),
		[]byte("alpha-filterable-NEW"), 0o644))

	// beta — enable-filterable, merged-but-not-tidied gen 1. Strategy
	// dir suffixes: SourceBucket = "property_beta",
	// IngestSuffix = "__enable_filterable_ingest_1".
	betaFilt := filepath.Join(migsDir, "enable_filterable_beta_1")
	require.NoError(t, os.MkdirAll(betaFilt, 0o755))
	touchSentinel(t, filepath.Join(betaFilt, "merged.mig"))
	require.NoError(t, os.WriteFile(filepath.Join(betaFilt, "properties.mig"), []byte("beta"), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, "property_beta__enable_filterable_ingest_1"), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(lsmPath, "property_beta__enable_filterable_ingest_1", "seg.db"),
		[]byte("beta-filt-NEW"), 0o644))

	// gamma — enable-rangeable (FilterableToRangeable strategy),
	// merged-but-not-tidied gen 1. Strategy dir suffixes:
	// SourceBucket = "property_gamma_rangeable",
	// IngestSuffix = "__rangeable_ingest_1".
	gammaRange := filepath.Join(migsDir, "filterable_to_rangeable_gamma_1")
	require.NoError(t, os.MkdirAll(gammaRange, 0o755))
	touchSentinel(t, filepath.Join(gammaRange, "merged.mig"))
	require.NoError(t, os.WriteFile(filepath.Join(gammaRange, "properties.mig"), []byte("gamma"), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, "property_gamma_rangeable__rangeable_ingest_1"), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(lsmPath, "property_gamma_rangeable__rangeable_ingest_1", "seg.db"),
		[]byte("gamma-range-NEW"), 0o644))

	logger, _ := test.NewNullLogger()
	FinalizeCompletedMigrations(lsmPath, logger)

	// All three property migrations must promote to their canonical
	// names with the correct data. A bug that processed only the first
	// namespace would leave beta/gamma orphaned.
	for _, c := range []struct {
		canonical string
		want      string
	}{
		{"property_alpha_searchable/seg.db", "alpha-searchable-NEW"},
		{"property_alpha/seg.db", "alpha-filterable-NEW"},
		{"property_beta/seg.db", "beta-filt-NEW"},
		{"property_gamma_rangeable/seg.db", "gamma-range-NEW"},
	} {
		got, err := os.ReadFile(filepath.Join(lsmPath, c.canonical))
		require.NoErrorf(t, err, "%s must exist after finalize", c.canonical)
		require.Equalf(t, c.want, string(got), "%s data mismatch", c.canonical)
	}

	// All four .migrations/ tracker dirs must be removed (recovery completes
	// the full promotion and trims their entries).
	migEntries, err := os.ReadDir(migsDir)
	require.NoError(t, err)
	require.Empty(t, migEntries,
		"every tracker must be cleared after multi-prop recovery — got %v", migEntries)
}

// TestFinalizeCompletedMigrations_PerShardDivergentStates_Converge
// pins https://github.com/weaviate/0-weaviate-issues/issues/214 Test Gap unit-level item 2: three shards
// of the same collection enter restart with deliberately divergent
// half-finalized states for the SAME migration. All three must
// converge to the same canonical-bucket shape after their per-shard
// FinalizeCompletedMigrations runs (it's a per-shard, not a per-node,
// path; called once per shard during shard init).
//
// Failure shape this catches: a refactor that introduced cross-shard
// state leakage in finalize (e.g. an accidental shared global
// recovered-gens cache) would let one shard's promotion influence
// another's. The pre-fix per-replica divergence Frontend Claude
// observed (`7×32, 3×30, 2×23, 6×5`) is structurally this class of
// bug if it ever resurfaces at the unit-test level.
//
// Layout exercised:
//
//   - shard-0: gen 1 fully tidied (clean post-runtime-swap state). Just
//     needs the canonical-rename pass.
//
//   - shard-1: gen 1 merged-but-not-tidied (crashed mid-swap). Needs
//     the recovery path's sentinel writes plus the canonical-rename.
//
//   - shard-2: NO migrations dir at all (the FinalizeCompletedMigrations
//     no-op path). Must remain unchanged.
func TestFinalizeCompletedMigrations_PerShardDivergentStates_Converge(t *testing.T) {
	type shardSetup struct {
		name            string
		stage           string // "tidied", "merged", "no_migrations"
		expectCanonical bool
		expectedData    string
	}

	shards := []shardSetup{
		{name: "shard-0", stage: "tidied", expectCanonical: true, expectedData: "shard0-NEW"},
		{name: "shard-1", stage: "merged", expectCanonical: true, expectedData: "shard1-NEW"},
		{name: "shard-2", stage: "no_migrations", expectCanonical: false, expectedData: ""},
	}

	root := t.TempDir()
	logger, _ := test.NewNullLogger()
	for _, sh := range shards {
		lsmPath := filepath.Join(root, sh.name, "lsm")
		require.NoError(t, os.MkdirAll(lsmPath, 0o755))
		switch sh.stage {
		case "tidied":
			migsDir := filepath.Join(lsmPath, ".migrations")
			require.NoError(t, os.MkdirAll(migsDir, 0o755))
			gen1 := filepath.Join(migsDir, "searchable_retokenize_path_1")
			require.NoError(t, os.MkdirAll(gen1, 0o755))
			touchSentinel(t, filepath.Join(gen1, "merged.mig"))
			touchSentinel(t, filepath.Join(gen1, "swapped.mig"))
			touchSentinel(t, filepath.Join(gen1, "tidied.mig"))
			require.NoError(t, os.WriteFile(filepath.Join(gen1, "properties.mig"), []byte("path"), 0o644))
			require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, "property_path_searchable__retokenize_ingest_1"), 0o755))
			require.NoError(t, os.WriteFile(
				filepath.Join(lsmPath, "property_path_searchable__retokenize_ingest_1", "seg.db"),
				[]byte(sh.expectedData), 0o644))
		case "merged":
			migsDir := filepath.Join(lsmPath, ".migrations")
			require.NoError(t, os.MkdirAll(migsDir, 0o755))
			gen1 := filepath.Join(migsDir, "searchable_retokenize_path_1")
			require.NoError(t, os.MkdirAll(gen1, 0o755))
			touchSentinel(t, filepath.Join(gen1, "merged.mig"))
			require.NoError(t, os.WriteFile(filepath.Join(gen1, "properties.mig"), []byte("path"), 0o644))
			require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, "property_path_searchable__retokenize_ingest_1"), 0o755))
			require.NoError(t, os.WriteFile(
				filepath.Join(lsmPath, "property_path_searchable__retokenize_ingest_1", "seg.db"),
				[]byte(sh.expectedData), 0o644))
		case "no_migrations":
			// Intentionally empty — finalize must be a no-op here.
		}
		FinalizeCompletedMigrations(lsmPath, logger)
	}

	for _, sh := range shards {
		lsmPath := filepath.Join(root, sh.name, "lsm")
		canonical := filepath.Join(lsmPath, "property_path_searchable", "seg.db")
		if sh.expectCanonical {
			got, err := os.ReadFile(canonical)
			require.NoErrorf(t, err, "%s: canonical dir must exist", sh.name)
			require.Equalf(t, sh.expectedData, string(got), "%s: canonical data mismatch", sh.name)
		} else {
			_, err := os.Stat(canonical)
			require.True(t, os.IsNotExist(err),
				"%s: expected no canonical dir on no-migrations shard, got %v", sh.name, err)
		}
		// The migrations dir, if it existed, must be empty after finalize.
		if sh.stage != "no_migrations" {
			migEntries, _ := os.ReadDir(filepath.Join(lsmPath, ".migrations"))
			require.Emptyf(t, migEntries, "%s: tracker dirs must be cleared after finalize", sh.name)
		}
	}
}

// itoa is the local stand-in for strconv.Itoa kept private to the test
// file to avoid touching imports needlessly.
func itoa(i int) string {
	const digits = "0123456789"
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = digits[i%10]
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
