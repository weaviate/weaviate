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
	"strings"
)

func migrationTrackerDirNames(lsmPath string) (names []string, visible bool) {
	entries, err := os.ReadDir(filepath.Join(lsmPath, migrationsDir))
	if err != nil {
		return nil, os.IsNotExist(err)
	}
	names = make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			names = append(names, entry.Name())
		}
	}
	return names, true
}

// nextMigrationGeneration returns the per-node generation `N` a new
// migration on (migrationDirPrefix, propNamesSuffix) should use on this
// shard's LSM directory. The new migration writes to dirs suffixed
// `_<N>`; older generations (if any) still live alongside the
// canonical main bucket until reconciliation promotes them at the next
// shard load.
//
// `migrationDirPrefix` is one of the constants in
// inverted_reindex_strategy_dir_names.go (e.g. `searchable_retokenize`
// or `searchable_map_to_blockmax`). `propNamesSuffix` is the
// strategy-specific per-property tail (e.g. `_text` for the per-property
// retokenize strategies, or the sorted-joined "_p1_p2" for multi-property
// strategies — pass "" for class-level strategies). The full dir name
// pattern matched is `<migrationDirPrefix><propNamesSuffix>_<N>`.
//
// Both trackerDirs and records must be the complete set the caller
// established: a generation claimed only by a record nobody could read, or
// only by a directory nobody could list, is invisible here, and handing it
// out again collides with the very directories that claim it.
//
// Called from [ReindexProvider.buildReindexTasks] before constructing the
// strategy instance, once per shard / prop / indexType tuple. Computed
// per-node — different nodes may pick different generations for the
// same RAFT task and that's correct: generation is purely a per-node
// on-disk implementation detail of the deferred-finalize design.
func nextMigrationGeneration(trackerDirs []string, migrationDirPrefix, propNamesSuffix string,
	records []MigrationRecord,
) int {
	return highestMigrationGeneration(trackerDirs, migrationDirPrefix, propNamesSuffix, records) + 1
}

func highestMigrationGeneration(trackerDirs []string, migrationDirPrefix, propNamesSuffix string,
	records []MigrationRecord,
) int {
	highest := maxMigrationGeneration(trackerDirs, migrationDirPrefix, propNamesSuffix)
	target := migrationDirPrefix + propNamesSuffix
	for _, rec := range records {
		prefix, gen, ok := parseMigrationDirName(rec.Subject().TrackerDir)
		if ok && prefix == target && gen > highest {
			highest = gen
		}
	}
	return highest
}

func maxMigrationGeneration(trackerDirs []string, migrationDirPrefix, propNamesSuffix string) int {
	target := migrationDirPrefix + propNamesSuffix
	highest := 0
	for _, name := range trackerDirs {
		prefix, gen, ok := parseMigrationDirName(name)
		if !ok {
			continue
		}
		if prefix != target {
			continue
		}
		if gen > highest {
			highest = gen
		}
	}
	return highest
}

func reindexSuffixFor(namespace string) string {
	switch {
	case strings.HasPrefix(namespace, MigrationDirSearchableMapToBlockmax):
		return "__blockmax_reindex"
	case strings.HasPrefix(namespace, MigrationDirFilterableRoaringsetRefresh):
		return "__roaringset_reindex"
	case strings.HasPrefix(namespace, MigrationDirPrefixFilterableToRangeable):
		return "__rangeable_reindex"
	case strings.HasPrefix(namespace, MigrationDirPrefixSearchableRetokenize):
		return "__retokenize_reindex"
	case strings.HasPrefix(namespace, MigrationDirPrefixFilterableRetokenize):
		return "__filt_retokenize_reindex"
	case strings.HasPrefix(namespace, MigrationDirPrefixEnableFilterable):
		return "__enable_filterable_reindex"
	case strings.HasPrefix(namespace, MigrationDirPrefixEnableSearchable):
		return "__enable_searchable_reindex"
	case strings.HasPrefix(namespace, MigrationDirPrefixRebuildSearchable):
		return "__rebuild_searchable_reindex"
	}
	return ""
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

type migrationBucketSuffixes struct {
	sourceBucketName func(propName string) string
	ingestSuffix     string
}

func migrationSuffixes(migName string) *migrationBucketSuffixes {
	// Dir-name constants live in inverted_reindex_strategy_dir_names.go and
	// are referenced by each strategy's MigrationDirName() — reusing the same
	// constants here is what keeps this in sync with the writer side.
	//
	// Every migration dir name carries a `_<gen>` suffix appended by
	// [genSuffix]. The HasPrefix arms below match the strategy's prefix
	// regardless of it; callers compose the final sidecar dir name by
	// appending `_<gen>` to the suffix base.
	switch {
	case strings.HasPrefix(migName, MigrationDirSearchableMapToBlockmax):
		return &migrationBucketSuffixes{
			sourceBucketName: func(p string) string { return "property_" + p + "_searchable" },
			ingestSuffix:     "__blockmax_ingest",
		}
	case strings.HasPrefix(migName, MigrationDirFilterableRoaringsetRefresh):
		return &migrationBucketSuffixes{
			sourceBucketName: func(p string) string { return "property_" + p },
			ingestSuffix:     "__roaringset_ingest",
		}
	case strings.HasPrefix(migName, MigrationDirPrefixFilterableToRangeable):
		return &migrationBucketSuffixes{
			sourceBucketName: func(p string) string { return "property_" + p + "_rangeable" },
			ingestSuffix:     "__rangeable_ingest",
		}
	// Per-property dir names: "searchable_retokenize_<propName>"
	case strings.HasPrefix(migName, MigrationDirPrefixSearchableRetokenize):
		return &migrationBucketSuffixes{
			sourceBucketName: func(p string) string { return "property_" + p + "_searchable" },
			ingestSuffix:     "__retokenize_ingest",
		}
	// Per-property dir names: "filterable_retokenize_<propName>"
	case strings.HasPrefix(migName, MigrationDirPrefixFilterableRetokenize):
		return &migrationBucketSuffixes{
			sourceBucketName: func(p string) string { return "property_" + p },
			ingestSuffix:     "__filt_retokenize_ingest",
		}
	// Per-property dir names: "enable_filterable_<prop1>_<prop2>..." (see
	// EnableFilterableStrategy.MigrationDirName). The list of properties is
	// authoritative in the migration's record; the dir name is informational.
	case strings.HasPrefix(migName, MigrationDirPrefixEnableFilterable):
		return &migrationBucketSuffixes{
			sourceBucketName: func(p string) string { return "property_" + p },
			ingestSuffix:     "__enable_filterable_ingest",
		}
	// Per-property dir names: "enable_searchable_<prop1>_<prop2>..." (see
	// EnableSearchableStrategy.MigrationDirName).
	case strings.HasPrefix(migName, MigrationDirPrefixEnableSearchable):
		return &migrationBucketSuffixes{
			sourceBucketName: func(p string) string { return "property_" + p + "_searchable" },
			ingestSuffix:     "__enable_searchable_ingest",
		}
	// Per-property dir names: "rebuild_searchable_<prop1>_<prop2>..." (see
	// RebuildSearchableStrategy.MigrationDirName).
	case strings.HasPrefix(migName, MigrationDirPrefixRebuildSearchable):
		return &migrationBucketSuffixes{
			sourceBucketName: func(p string) string { return "property_" + p + "_searchable" },
			ingestSuffix:     "__rebuild_searchable_ingest",
		}
	default:
		return nil
	}
}
