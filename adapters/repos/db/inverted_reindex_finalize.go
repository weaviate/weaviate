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

	"github.com/sirupsen/logrus"
)

// FinalizeCompletedMigrations scans the shard's .migrations/ directory for
// completed migrations that still need filesystem cleanup. This handles the
// case where a runtime swap (via DTM) completed the in-memory swap and marked
// tidied, but the directory renames were deferred to next startup.
//
// For each migration dir that has both swapped.mig and tidied.mig:
//   - Read properties.mig to discover which properties were migrated
//   - Detect the migration type from the dir name to determine bucket suffixes
//   - Rename ingest dirs to canonical names, remove backup dirs
//
// This runs at shard startup BEFORE bucket loading, so renames are safe.
func FinalizeCompletedMigrations(lsmPath string, logger logrus.FieldLogger) {
	migrationsDir := filepath.Join(lsmPath, ".migrations")
	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		return // no migrations dir, nothing to do
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		migDir := filepath.Join(migrationsDir, entry.Name())
		finalizeMigrationDir(lsmPath, migDir, entry.Name(), logger)
	}
}

func finalizeMigrationDir(lsmPath, migDir, migName string, logger logrus.FieldLogger) {
	// Only finalize if both swapped and tidied sentinels exist.
	if !fileExists(filepath.Join(migDir, "swapped.mig")) {
		return
	}
	if !fileExists(filepath.Join(migDir, "tidied.mig")) {
		return
	}

	// Read properties from the migration.
	props, err := readMigrationProps(migDir)
	if err != nil || len(props) == 0 {
		return
	}

	// Determine bucket naming from migration dir name.
	suffixes := migrationSuffixes(migName)
	if suffixes == nil {
		return
	}

	logger = logger.WithField("migration", migName)

	for _, propName := range props {
		mainName := suffixes.sourceBucketName(propName)
		ingestDir := filepath.Join(lsmPath, mainName+suffixes.ingestSuffix)
		backupDir := filepath.Join(lsmPath, mainName+suffixes.backupSuffix)
		mainDir := filepath.Join(lsmPath, mainName)

		// Remove backup dir.
		if fileExists(backupDir) {
			if err := os.RemoveAll(backupDir); err != nil {
				logger.WithError(err).WithField("dir", backupDir).
					Error("finalize: failed to remove backup dir")
				continue
			}
			logger.WithField("dir", backupDir).Debug("finalize: removed backup dir")
		}

		// Rename ingest dir to canonical main dir.
		if fileExists(ingestDir) {
			// Remove stale main dir if it exists (shouldn't normally, but be safe).
			if fileExists(mainDir) {
				os.RemoveAll(mainDir)
			}
			if err := os.Rename(ingestDir, mainDir); err != nil {
				logger.WithError(err).WithField("from", ingestDir).WithField("to", mainDir).
					Error("finalize: failed to rename ingest dir")
				continue
			}
			logger.WithField("from", ingestDir).WithField("to", mainDir).
				Debug("finalize: renamed ingest dir to main")
		}
	}
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
	switch {
	case migName == "searchable_map_to_blockmax":
		return &migrationBucketSuffixes{
			sourceBucketName: func(p string) string { return "property_" + p + "_searchable" },
			ingestSuffix:     "__blockmax_ingest",
			backupSuffix:     "__blockmax_map",
		}
	case migName == "filterable_roaringset_refresh":
		return &migrationBucketSuffixes{
			sourceBucketName: func(p string) string { return "property_" + p },
			ingestSuffix:     "__roaringset_ingest",
			backupSuffix:     "__roaringset_backup",
		}
	case strings.HasPrefix(migName, "filterable_to_rangeable"):
		return &migrationBucketSuffixes{
			sourceBucketName: func(p string) string { return "property_" + p + "_rangeable" },
			ingestSuffix:     "__rangeable_ingest",
			backupSuffix:     "__rangeable_backup",
		}
	case migName == "searchable_retokenize":
		return &migrationBucketSuffixes{
			sourceBucketName: func(p string) string { return "property_" + p + "_searchable" },
			ingestSuffix:     "__retokenize_ingest",
			backupSuffix:     "__retokenize_backup",
		}
	case migName == "filterable_retokenize":
		return &migrationBucketSuffixes{
			sourceBucketName: func(p string) string { return "property_" + p },
			ingestSuffix:     "__filt_retokenize_ingest",
			backupSuffix:     "__filt_retokenize_backup",
		}
	default:
		return nil
	}
}
