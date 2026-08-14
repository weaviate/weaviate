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

	"github.com/weaviate/weaviate/entities/backup"
)

// cleanupRootPathOnStartup removes staging/marker dirs left behind by a
// crash. The in-memory state that tracks these is rebuilt from scratch on
// startup, so any matching dir on disk is by definition orphan.
func cleanupRootPathOnStartup(rootPath string, logger logrus.FieldLogger) error {
	entries, err := os.ReadDir(rootPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // initial startup
		}
		return err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		switch {
		case strings.HasPrefix(name, backup.DeleteMarker):
			if err := os.RemoveAll(filepath.Join(rootPath, name)); err != nil {
				return err
			}
			logger.WithFields(logrus.Fields{
				"action":     "startup",
				"directory":  name,
				"index_path": filepath.Join(rootPath, name),
				"index":      name[len(backup.DeleteMarker):],
			}).Info("removed partially deleted index directory: " + name + " Did Weaviate crash?")

		case strings.HasPrefix(name, backup.BackupStagingPrefix):
			if err := os.RemoveAll(filepath.Join(rootPath, name)); err != nil {
				return err
			}
			logger.WithFields(logrus.Fields{
				"action":    "startup",
				"directory": name,
			}).Info("removed orphaned backup staging directory")

		case strings.HasPrefix(name, replicaStagingPrefix):
			// Hardlinks inside pin compaction reclamation until we remove the dir.
			if err := os.RemoveAll(filepath.Join(rootPath, name)); err != nil {
				return err
			}
			logger.WithFields(logrus.Fields{
				"action":    "startup",
				"directory": name,
			}).Info("removed orphaned replica staging directory")
		}
	}

	return nil
}
