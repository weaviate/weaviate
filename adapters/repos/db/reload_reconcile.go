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
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

// isReservedDataRootDir reports whether a directory at the data root is not a
// class index directory. Index.path() is RootPath/<lowercased class>, a class
// name starts with a letter, and a class named "raft" is rejected at parse
// time (usecases/schema/parser.go). So a class directory never starts with a
// dot and is never "raft". Dot-prefixed directories are the backup framework's
// staging (.backup-staging-*) and restore temp (.backup.tmp) directories; the
// __DELETE_ME_AFTER_BACKUP__ prefix marks an index kept for a running backup;
// .deleteme directories are already pending async removal.
func isReservedDataRootDir(name string) bool {
	return name == config.DefaultRaftDir ||
		strings.HasPrefix(name, ".") ||
		strings.HasSuffix(name, asyncDeleteSuffix) ||
		strings.HasPrefix(name, backup.DeleteMarker)
}

// dropOrphanedIndexDirectories removes class directories under the data root
// that the reloaded schema no longer names. Two RAFT paths delete a class from
// the schema without dropping its index directory, and nothing else in the
// reload path enumerates the data directory against the schema:
//
//   - a restart mid-DELETE_CLASS replays the entry schema-only, so DeleteIndex
//     never runs (0-weaviate-issues#652);
//   - a rejoin via InstallSnapshot restores a schema without the class
//     (0-weaviate-issues#651).
//
// keepClasses is the set of classes in the reloaded schema.
//
// A snapshot install on an already-running node (Store.reloadDBFromSchema with
// st.raft != nil) leaves the deleted class's *Index loaded in memory, so an
// orphan may still be live. A live index is dropped through DeleteIndex, which
// stops its cycle managers before renaming: renaming the directory out from
// under a running index would let its next commitlog or compaction write
// recreate it. An index that was never opened (both reproduced paths) has no
// running cycles, so its directory is renamed and removed directly. Both use
// the rename-then-async-delete path Index.drop uses, so a large leftover does
// not stall the RAFT goroutine this runs on.
//
// Failures are logged here as well as returned: the reload caller
// (SchemaManager.ReloadDBFromSchema) discards the error, and a silently
// surviving orphan is the symptom this function exists to remove.
func (db *DB) dropOrphanedIndexDirectories(keepClasses []string) error {
	keep := make(map[string]struct{}, len(keepClasses))
	for _, class := range keepClasses {
		keep[indexID(schema.ClassName(class))] = struct{}{}
	}

	entries, err := os.ReadDir(db.config.RootPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		err = fmt.Errorf("list data root %q for orphan class directories: %w", db.config.RootPath, err)
		db.logger.WithField("action", "reconcile_orphan_index_dir").Error(err)
		return err
	}

	ec := errorcompounder.New()
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if isReservedDataRootDir(name) {
			continue
		}
		if _, ok := keep[name]; ok {
			continue
		}

		path := filepath.Join(db.config.RootPath, name)
		log := db.logger.WithFields(logrus.Fields{
			"action": "reconcile_orphan_index_dir",
			"class":  name,
			"path":   path,
		})
		log.Info("dropping class directory absent from the schema after reload")

		// db.indices is keyed by indexID, which is also the directory name.
		db.indexLock.RLock()
		liveIndex, loaded := db.indices[name]
		db.indexLock.RUnlock()

		if loaded {
			if err := db.DeleteIndex(liveIndex.Config.ClassName); err != nil {
				err = fmt.Errorf("drop loaded orphan index: %w", err)
				log.Error(err)
				ec.Add(err)
			}
			continue
		}

		renamed, err := renameForAsyncDelete(path, db.logger)
		if err != nil {
			err = fmt.Errorf("rename orphan class directory for async delete: %w", err)
			log.Error(err)
			ec.Add(err)
			continue
		}
		if renamed == "" {
			// already gone
			continue
		}
		spawnAsyncDelete(renamed, db.logger)
	}
	return ec.ToError()
}
