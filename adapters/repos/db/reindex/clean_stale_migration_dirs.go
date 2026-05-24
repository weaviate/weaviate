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

package reindex

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"
)

// CleanStaleMigrationDirsAt is the pure-function form of
// the per-shard cleanup hook: takes an explicit lsmPath + logger so the
// preservation logic can be unit-tested without standing up a Shard.
//
// Every migration tracker dir carries a per-node generation suffix
// (`_<N>`); a single (prop, indexType) tuple can have multiple
// generations on disk when the last migration's trim hasn't run (e.g.
// crash before markTidied → next-restart finalize cleans up
// everything). Match by prefix and walk every entry.
//
// Tracker dirs with tidied.mig / merged.mig are PRESERVED — they are
// live deferred-finalize state, NOT stale partial state. Wiping them
// out from under the in-memory bucket pointer is what produces the
// #10675-shape silent data loss on back-to-back submits without a
// restart.
func CleanStaleMigrationDirsAt(lsmPath, propName, indexType string, logger logrus.FieldLogger) {
	migrationsRoot := filepath.Join(lsmPath, ".migrations")
	entries, err := os.ReadDir(migrationsRoot)
	if err != nil {
		if !os.IsNotExist(err) {
			logger.WithField("path", migrationsRoot).
				Error(fmt.Errorf("read migrations dir for stale-state cleanup: %w", err))
		}
		return
	}
	prefixes := MigrationDirsForPropertyIndex(propName, indexType)
	preserved := CompletedMigrationGens(lsmPath, prefixes)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		matches := false
		for _, p := range prefixes {
			if name == p || strings.HasPrefix(name, p+"_") {
				matches = true
				break
			}
		}
		if !matches {
			continue
		}
		if _, gen, ok := ParseMigrationDirName(name); ok && preserved[gen] {
			logger.WithField("path", filepath.Join(migrationsRoot, name)).
				WithField("gen", gen).
				Info("partial-reindex cleanup: preserving deferred-finalize tracker dir (tidied/merged present)")
			continue
		}
		path := filepath.Join(migrationsRoot, name)
		if err := os.RemoveAll(path); err != nil {
			logger.WithField("path", path).
				Error(fmt.Errorf("failed to clean up stale migration directory after index DELETE: %w; subsequent re-enable will fail loudly via the stale-sentinel check until this directory is removed manually", err))
		}
	}
}
