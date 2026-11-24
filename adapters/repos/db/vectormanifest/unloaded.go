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
	"fmt"
	"os"
	"path/filepath"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/shardmeta"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/dynamic"
)

// DropUnloaded is BeginDrop and FinishDrop for the shard at shardDir, which is
// not loaded. A shard that a load holds is left to that load. One from before
// the records existed is dropped from by the naming rule; siblings are the
// collection's other vectors, whose files stay there.
func DropUnloaded(shardDir, name string, siblings []string) error {
	records, initialized, err := readUnloaded(shardDir)
	if shardmeta.IsLocked(err) {
		return nil
	}
	if err != nil {
		return err
	}

	var artifacts helpers.VectorIndexArtifacts
	var id string
	rec, recorded := records[name]
	switch {
	case !initialized:
		id = helpers.VectorIndexIDForTarget(name)
		artifacts = helpers.VectorIndexArtifactsFor(name, siblings)
	case !recorded:
		return nil // nothing recorded, nothing owned
	default:
		id = rec.storageID()
		artifacts, err = dropArtifacts(records, name)
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
	err = dynamic.RemoveStateKeyForID(shardDir, id)
	if err != nil {
		return fmt.Errorf("remove dynamic state for %q: %w", name, err)
	}
	// the record goes last, so a failed drop keeps it for the retry
	err = shardmeta.DeleteOffline(shardDir, namespace, []byte(recordKey(name)))
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
	records, _, err := readUnloaded(shardDir)
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
// a load refuses. initialized is false for a shard that has none, such as one
// restored from a backup older than them.
func readUnloaded(shardDir string) (records map[string]record, initialized bool, err error) {
	records, initialized, _, err = readRecords(func(fn func(key, value []byte) error) error {
		return shardmeta.ForEachOffline(shardDir, namespace, fn)
	})
	return records, initialized, err
}
