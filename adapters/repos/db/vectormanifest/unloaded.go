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

package vectormanifest

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/shardmeta"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/dynamic"
)

// DropUnloaded is BeginDrop and FinishDrop for the shard at shardDir, which is
// not loaded. A shard that a load holds is left to that load. In a shard
// without records, the naming rule says what name owns, and the files of
// siblings, the collection's other vectors, stay.
func DropUnloaded(shardDir, name string, siblings []string) error {
	records, initialized, err := readUnloaded(shardDir)
	if shardmeta.IsLocked(err) {
		return nil
	}
	if err != nil {
		return err
	}
	tasks, err := readUnloadedTasks(shardDir)
	if shardmeta.IsLocked(err) {
		return nil
	}
	if err != nil {
		return err
	}
	c := contents{records: records, tasks: tasks}

	var artifacts helpers.VectorIndexArtifacts
	ids := c.storageIDs(name)
	switch {
	case !initialized:
		if len(tasks) > 0 {
			return errors.New("rebuild tasks without a vector index mapping")
		}
		ids = []string{helpers.VectorIndexIDForTarget(name)}
		artifacts = helpers.VectorIndexArtifactsFor(name, siblings)
	case len(ids) == 0:
		return nil // nothing recorded, nothing owned
	default:
		artifacts, err = dropArtifacts(c, name)
		if err != nil {
			return err
		}
	}

	for _, bucket := range artifacts.LSMBuckets {
		err := os.RemoveAll(filepath.Join(shardDir, "lsm", bucket))
		if err != nil {
			return fmt.Errorf("remove bucket %q: %w", bucket, err)
		}
	}
	for _, dir := range artifacts.ShardDirs {
		err := os.RemoveAll(filepath.Join(shardDir, dir))
		if err != nil {
			return fmt.Errorf("remove directory %q: %w", dir, err)
		}
	}
	// Whatever the index type: the drop discarded it with the vector's config.
	for _, id := range ids {
		err = dynamic.RemoveStateKeyForID(shardDir, id)
		if err != nil {
			return fmt.Errorf("remove dynamic state for %q: %w", name, err)
		}
	}
	// the record goes last, so a failed drop keeps it for the retry
	key := []byte(recordKey(name))
	if _, ok := tasks[name]; ok {
		err = shardmeta.DeleteOffline(shardDir, tasksNamespace, key)
		if err != nil {
			return fmt.Errorf("remove rebuild task for %q: %w", name, err)
		}
	}
	err = shardmeta.DeleteOffline(shardDir, namespace, key)
	if err != nil {
		return fmt.Errorf("remove mapping record for %q: %w", name, err)
	}
	return nil
}

// UnloadedStorageIDs maps each of names, vectors of a shard that is not
// loaded, to the ID its files are named by: its record's, or the naming rule's
// for a vector without one. When a load holds the records (locked) or they
// cannot be read (err), the IDs are the naming rule's and may be wrong.
func UnloadedStorageIDs(shardDir string, names []string) (ids map[string]string, locked bool, err error) {
	// another vector's record must not fail the report of these
	records, _, err := readUnloaded(shardDir, names...)
	ids = make(map[string]string, len(names))
	for _, name := range names {
		ids[name] = helpers.VectorIndexIDForTarget(name)
		if rec, ok := records[name]; ok {
			ids[name] = rec.storageID()
		}
	}
	if shardmeta.IsLocked(err) {
		return ids, true, nil
	}
	return ids, false, err
}

// readUnloaded reads the records of a shard that is not loaded, refusing what
// a load refuses, or only those of names when there are any. initialized is
// false for a shard that has none, such as one restored from a backup older
// than them.
func readUnloaded(shardDir string, names ...string) (records map[string]record, initialized bool, err error) {
	records, initialized, _, err = readRecords(func(fn func(key, value []byte) error) error {
		return shardmeta.ForEachOffline(shardDir, namespace, func(key, value []byte) error {
			if len(names) > 0 && string(key) != formatVersionKey &&
				!slices.ContainsFunc(names, func(name string) bool { return recordKey(name) == string(key) }) {
				return nil
			}
			return fn(key, value)
		})
	})
	return records, initialized, err
}

// readUnloadedTasks reads the rebuild tasks of a shard that is not loaded,
// refusing what a load refuses.
func readUnloadedTasks(shardDir string) (map[string]rebuildTask, error) {
	return readTasks(func(fn func(key, value []byte) error) error {
		return shardmeta.ForEachOffline(shardDir, tasksNamespace, fn)
	})
}
