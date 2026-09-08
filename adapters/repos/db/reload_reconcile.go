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

	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/usecases/config"
)

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
// keepClasses is the set of classes in the reloaded schema. Index directories
// are the lowercased class name (Index.path()). The raft directory is the only
// reserved non-class entry at the data root — a class named "raft" is rejected
// at parse time (usecases/schema/parser.go) — so any other directory absent
// from keepClasses is an orphan. Files are left alone; other bookkeeping at the
// data root (schema.db, modules.db, migration flags) is files, not directories.
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
func (db *DB) dropOrphanedIndexDirectories(keepClasses []string) error {
	keep := make(map[string]struct{}, len(keepClasses))
	for _, class := range keepClasses {
		keep[strings.ToLower(class)] = struct{}{}
	}

	entries, err := os.ReadDir(db.config.RootPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	ec := errorcompounder.New()
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if name == config.DefaultRaftDir || strings.HasSuffix(name, asyncDeleteSuffix) {
			continue
		}
		if _, ok := keep[name]; ok {
			continue
		}

		db.logger.WithFields(logrus.Fields{
			"action": "reconcile_orphan_index_dir",
			"class":  name,
		}).Info("dropping class directory absent from the schema after reload")

		// db.indices is keyed by the lowercased class name, which is the on-disk
		// directory name (Index.path()).
		db.indexLock.RLock()
		liveIndex, loaded := db.indices[name]
		db.indexLock.RUnlock()

		if loaded {
			if err := db.DeleteIndex(liveIndex.Config.ClassName); err != nil {
				ec.Add(err)
			}
			continue
		}

		renamed, err := renameForAsyncDelete(filepath.Join(db.config.RootPath, name), db.logger)
		if err != nil {
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
