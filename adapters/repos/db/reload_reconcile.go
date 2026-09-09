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
	"regexp"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

// indexDirNameRegex is schema.ClassNameRegexCore lowercased: Index.path() is
// RootPath/<indexID>, and indexID lowercases the validated class name.
var indexDirNameRegex = regexp.MustCompile(`^[a-z][_0-9a-z]{0,254}$`)

// isIndexDirName reports whether a data-root directory name can be a class
// index directory, so that anything else there is left alone: the backup
// framework's .backup-staging-* and .backup.tmp directories, an index renamed
// to __DELETE_ME_AFTER_BACKUP__* while a backup still reads it, .deleteme
// directories pending async removal, and operator-owned entries such as a
// mount point's lost+found. The raft directory matches the class pattern, but
// a class named "raft" is rejected at parse time (usecases/schema/parser.go).
func isIndexDirName(name string) bool {
	return indexDirNameRegex.MatchString(name) && name != config.DefaultRaftDir
}

// hasShardStore reports whether indexPath holds at least one shard directory:
// a child with the lsm store directory and the version file every shard
// creates when it initializes (shard_init_lsm.go). The name pattern alone also
// admits directories Weaviate does not own at the data root:
// BACKUP_FILESYSTEM_PATH accepts any absolute path, so <RootPath>/backups is
// a valid setup. A filesystem backup is laid out as
// <backupID>/<node>/<class>/chunk-N, with one file name per level
// (backup_config.json, backup.json, chunk-N) and node and class names as
// directories, so a regular file named version two levels down cannot come
// from a backup, whatever the node or class is called.
func hasShardStore(indexPath string) (bool, error) {
	entries, err := os.ReadDir(indexPath)
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		lsm, err := os.Stat(shardPathLSM(indexPath, entry.Name()))
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return false, err
		}
		version, err := os.Stat(filepath.Join(shardPath(indexPath, entry.Name()), "version"))
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return false, err
		}
		if lsm.IsDir() && version.Mode().IsRegular() {
			return true, nil
		}
	}
	return false, nil
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
// A directory is only dropped once it is positively an index: either its
// *Index is loaded in db.indices, or it holds a shard store (hasShardStore).
// A name-shaped directory with neither is left alone, which also leaves an
// index directory whose shards all live on other nodes; that leftover is
// empty.
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
		if !isIndexDirName(name) {
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

		// db.indices is keyed by indexID, which is also the directory name.
		db.indexLock.RLock()
		liveIndex, loaded := db.indices[name]
		db.indexLock.RUnlock()

		if !loaded {
			isIndex, err := hasShardStore(path)
			if err != nil {
				err = fmt.Errorf("inspect directory absent from the schema: %w", err)
				log.Error(err)
				ec.Add(err)
				continue
			}
			if !isIndex {
				continue
			}
		}
		log.Info("dropping class directory absent from the schema after reload")

		if loaded {
			if err := db.DeleteIndex(liveIndex.Config.ClassName); err != nil {
				err = fmt.Errorf("drop loaded orphan index: %w", err)
				log.Error(err)
				ec.Add(err)
				continue
			}
			// DeleteIndex logs a failed Index.drop and still returns nil (the
			// schema removal must not fail on a partial drop), so a surviving
			// directory is the only signal that the drop did not rename it.
			_, statErr := os.Stat(path)
			switch {
			case statErr == nil:
				err := fmt.Errorf("drop loaded orphan index %q left its directory in place", name)
				log.Error(err)
				ec.Add(err)
			case !os.IsNotExist(statErr):
				err := fmt.Errorf("confirm drop of loaded orphan index %q: %w", name, statErr)
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
