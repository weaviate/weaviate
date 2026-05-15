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
// weaviate/weaviate#10675. The functions under test live in
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
