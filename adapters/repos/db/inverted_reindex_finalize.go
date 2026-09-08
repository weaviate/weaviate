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

// migrationTrackerDirAbsent reports whether a migration's tracker dir is
// provably missing. A stat error must not be read as absence, or a pending
// migration gets marked complete without its index ever rebuilt.
func migrationTrackerDirAbsent(lsmPath, dirName string) bool {
	info, err := os.Stat(filepath.Join(lsmPath, migrationsDir, dirName))
	if err != nil {
		return os.IsNotExist(err)
	}
	return !info.IsDir()
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
