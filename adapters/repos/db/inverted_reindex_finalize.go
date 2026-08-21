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
// Returns 1 when no prior generation exists. Returns max(existing)+1
// otherwise. Non-integer-suffixed dirs (i.e. pre-generation legacy
// state, which shouldn't exist on this branch but defensive code is
// cheap) are ignored.
//
// Called from [ReindexProvider.processOneUnit] before constructing the
// strategy instance, once per shard / prop / indexType tuple. Computed
// per-node — different nodes may pick different generations for the
// same RAFT task and that's correct: generation is purely a per-node
// on-disk implementation detail of the deferred-finalize design.
func nextMigrationGeneration(lsmPath, migrationDirPrefix, propNamesSuffix string) int {
	return maxMigrationGeneration(lsmPath, migrationDirPrefix, propNamesSuffix) + 1
}

// maxMigrationGeneration returns the highest existing generation on disk
// for the (prefix, propNamesSuffix) tuple, or 0 if none exists.
//
// Used by recovery / rehydrate paths that need to construct a strategy
// instance matching an existing on-disk migration. The recovery path is
// the only legitimate caller — fresh task starts should always use
// [nextMigrationGeneration] to claim a new generation.
func maxMigrationGeneration(lsmPath, migrationDirPrefix, propNamesSuffix string) int {
	migrationsDir := filepath.Join(lsmPath, ".migrations")
	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		return 0
	}
	target := migrationDirPrefix + propNamesSuffix
	highest := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		prefix, gen, ok := parseMigrationDirName(entry.Name())
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

// reindexSuffixForFinalize returns the per-strategy reindex bucket
// suffix base (e.g. `__retokenize_reindex`) used to identify older-gen
// reindex sidecar dirs in the finalize cleanup. Kept in lockstep with
// each strategy's ReindexSuffix() base — when a new strategy is added,
// extend both this switch and the strategy's ReindexSuffix() method.
func reindexSuffixForFinalize(namespace string) string {
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

func readMigrationProps(migDir string) ([]string, error) {
	data, err := os.ReadFile(filepath.Join(migDir, "properties.mig"))
	if err != nil {
		return nil, err
	}
	content := strings.TrimSpace(string(data))
	if content == "" {
		return nil, nil
	}
	return strings.Split(content, ","), nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// migrationBucketSuffixes maps a migration dir name to its bucket naming scheme.
type migrationBucketSuffixes struct {
	sourceBucketName func(propName string) string
	ingestSuffix     string
	backupSuffix     string
}

func migrationSuffixes(migName string) *migrationBucketSuffixes {
	// Dir-name constants live in inverted_reindex_strategy_dir_names.go and
	// are referenced by each strategy's MigrationDirName() — keep finalize
	// in sync with the writer side by reusing the same constants here.
	//
	// Every migration dir name carries a `_<gen>` suffix appended by
	// [genSuffix]. The HasPrefix arms below match the strategy's prefix
	// regardless of the gen suffix; finalize callers compose the final
	// gen-suffixed sidecar dir name by appending `_<gen>` to the
	// ingest/backup suffix base.
	switch {
	case strings.HasPrefix(migName, MigrationDirSearchableMapToBlockmax):
		return &migrationBucketSuffixes{
			sourceBucketName: func(p string) string { return "property_" + p + "_searchable" },
			ingestSuffix:     "__blockmax_ingest",
			backupSuffix:     "__blockmax_map",
		}
	case strings.HasPrefix(migName, MigrationDirFilterableRoaringsetRefresh):
		return &migrationBucketSuffixes{
			sourceBucketName: func(p string) string { return "property_" + p },
			ingestSuffix:     "__roaringset_ingest",
			backupSuffix:     "__roaringset_backup",
		}
	case strings.HasPrefix(migName, MigrationDirPrefixFilterableToRangeable):
		return &migrationBucketSuffixes{
			sourceBucketName: func(p string) string { return "property_" + p + "_rangeable" },
			ingestSuffix:     "__rangeable_ingest",
			backupSuffix:     "__rangeable_backup",
		}
	// Per-property dir names: "searchable_retokenize_<propName>"
	case strings.HasPrefix(migName, MigrationDirPrefixSearchableRetokenize):
		return &migrationBucketSuffixes{
			sourceBucketName: func(p string) string { return "property_" + p + "_searchable" },
			ingestSuffix:     "__retokenize_ingest",
			backupSuffix:     "__retokenize_backup",
		}
	// Per-property dir names: "filterable_retokenize_<propName>"
	case strings.HasPrefix(migName, MigrationDirPrefixFilterableRetokenize):
		return &migrationBucketSuffixes{
			sourceBucketName: func(p string) string { return "property_" + p },
			ingestSuffix:     "__filt_retokenize_ingest",
			backupSuffix:     "__filt_retokenize_backup",
		}
	// Per-property dir names: "enable_filterable_<prop1>_<prop2>..." (see
	// EnableFilterableStrategy.MigrationDirName). The list of properties is
	// authoritative in properties.mig; the dir name is informational.
	case strings.HasPrefix(migName, MigrationDirPrefixEnableFilterable):
		return &migrationBucketSuffixes{
			sourceBucketName: func(p string) string { return "property_" + p },
			ingestSuffix:     "__enable_filterable_ingest",
			backupSuffix:     "__enable_filterable_backup",
		}
	// Per-property dir names: "enable_searchable_<prop1>_<prop2>..." (see
	// EnableSearchableStrategy.MigrationDirName).
	case strings.HasPrefix(migName, MigrationDirPrefixEnableSearchable):
		return &migrationBucketSuffixes{
			sourceBucketName: func(p string) string { return "property_" + p + "_searchable" },
			ingestSuffix:     "__enable_searchable_ingest",
			backupSuffix:     "__enable_searchable_backup",
		}
	// Per-property dir names: "rebuild_searchable_<prop1>_<prop2>..." (see
	// RebuildSearchableStrategy.MigrationDirName).
	case strings.HasPrefix(migName, MigrationDirPrefixRebuildSearchable):
		return &migrationBucketSuffixes{
			sourceBucketName: func(p string) string { return "property_" + p + "_searchable" },
			ingestSuffix:     "__rebuild_searchable_ingest",
			backupSuffix:     "__rebuild_searchable_backup",
		}
	default:
		return nil
	}
}
