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
	"encoding/json"
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
// not loaded. siblings are the collection's other vectors, whose files stay.
// A shard from before the records existed is dropped from by the naming rule,
// and one that a load holds is left to that load.
func DropUnloaded(shardDir, name string, siblings []string) error {
	records, initialized, err := readUnloaded(shardDir, append([]string{name}, siblings...))
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
	records, _, err := readUnloaded(shardDir, names)
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

// readUnloaded reads the records of names from a shard that is not loaded,
// refusing what a load refuses. initialized is false for a shard that has no
// records at all, such as one restored from a backup older than them.
func readUnloaded(shardDir string, names []string) (records map[string]record, initialized bool, err error) {
	keys := make([][]byte, 0, len(names)+1)
	keys = append(keys, []byte(formatVersionKey))
	for _, name := range names {
		keys = append(keys, []byte(recordKey(name)))
	}
	vals, _, err := shardmeta.GetOfflineMulti(shardDir, namespace, keys...)
	if err != nil {
		return nil, false, fmt.Errorf("read vector index mapping: %w", err)
	}
	if vals == nil {
		return nil, false, nil
	}
	version, values := vals[0], vals[1:]
	if version == nil {
		if slices.ContainsFunc(values, func(v []byte) bool { return v != nil }) {
			return nil, false, errors.New("read vector index mapping: records without a format version")
		}
		return nil, false, nil
	}
	if string(version) != formatVersion {
		return nil, false, fmt.Errorf("read vector index mapping: unsupported format version %q, this binary reads %q",
			version, formatVersion)
	}
	records = make(map[string]record, len(names))
	for i, name := range names {
		if values[i] == nil {
			continue
		}
		var rec record
		err := json.Unmarshal(values[i], &rec)
		if err == nil {
			err = rec.validate()
		}
		if err != nil {
			return nil, false, fmt.Errorf("read vector index mapping: record %q: %w", name, err)
		}
		records[name] = rec
	}
	return records, true, nil
}

// readUnloadedTasks reads the tasks of names from a shard that is
// not loaded, with Load's refusals. A shard without a metadata DB has none.
func readUnloadedTasks(shardDir string, names []string) (map[string]rebuildTask, error) {
	keys := make([][]byte, 0, len(names)+1)
	keys = append(keys, []byte(formatVersionKey))
	for _, name := range names {
		keys = append(keys, []byte(recordKey(name)))
	}
	vals, _, err := shardmeta.GetOfflineMulti(shardDir, tasksNamespace, keys...)
	if err != nil {
		return nil, fmt.Errorf("read vector index tasks: %w", err)
	}
	tasks := map[string]rebuildTask{}
	if vals == nil {
		return tasks, nil
	}
	if vals[0] == nil {
		if slices.ContainsFunc(vals[1:], func(v []byte) bool { return v != nil }) {
			return nil, errors.New("read vector index tasks: tasks without a format version")
		}
		return tasks, nil
	}
	err = checkTasksFormatVersion(vals[0])
	if err != nil {
		return nil, fmt.Errorf("read vector index tasks: %w", err)
	}
	for i, name := range names {
		v := vals[i+1]
		if v == nil {
			continue
		}
		task, err := decodeTask(name, v)
		if err != nil {
			return nil, fmt.Errorf("read vector index tasks: %w", err)
		}
		tasks[name] = task
	}
	return tasks, nil
}
