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

	"github.com/weaviate/weaviate/adapters/repos/db/shardmeta"
)

// The task section of index.db: one entry per vector whose index is being
// rebuilt on this shard, keyed like the mapping. Every key and field name is
// read by later versions; changing one is a format change.
const (
	tasksNamespace     = "vector_index_tasks"
	tasksFormatVersion = "1"
)

// taskIndex is one of the two indexes a task names.
type taskIndex struct {
	PhysicalID string `json:"physical_id"`
	IndexType  string `json:"index_type"`
}

// storageID is the ID the index's files are named by, the same rule as a
// record's.
func (i taskIndex) storageID() string {
	return storageIDFor(i.PhysicalID)
}

// recordedID is the physical ID as the task holds it, for messages only.
func (i taskIndex) recordedID() string {
	return i.PhysicalID
}

func (i taskIndex) validate() error {
	if i.PhysicalID == "" {
		return errors.New("empty physical id")
	}
	err := validateID(i.PhysicalID)
	if err != nil {
		return fmt.Errorf("physical id: %w", err)
	}
	if i.IndexType == "" {
		return errors.New("empty index type")
	}
	return nil
}

// rebuildTask is a rebuild in progress: Candidate is built to replace
// Source. It is written before any candidate storage exists and removed once
// the index the mapping does not name is gone.
type rebuildTask struct {
	TaskID      string    `json:"task_id"`
	TaskVersion uint64    `json:"task_version"`
	Source      taskIndex `json:"source"`
	Candidate   taskIndex `json:"candidate"`
}

func (t rebuildTask) validate() error {
	if t.TaskID == "" {
		return errors.New("empty task id")
	}
	err := t.Source.validate()
	if err != nil {
		return fmt.Errorf("source: %w", err)
	}
	err = t.Candidate.validate()
	if err != nil {
		return fmt.Errorf("candidate: %w", err)
	}
	if t.Source.storageID() == t.Candidate.storageID() {
		return fmt.Errorf("source and candidate are both %q", t.Source.storageID())
	}
	return nil
}

// retired lists the indexes that go once the rebuild ends: the one rec does not
// name, or both for a vector without a record.
func (t rebuildTask) retired(rec record, recorded bool) ([]taskIndex, error) {
	switch {
	case !recorded:
		return []taskIndex{t.Source, t.Candidate}, nil
	case rec.storageID() == t.Source.storageID():
		return []taskIndex{t.Candidate}, nil
	case rec.storageID() == t.Candidate.storageID():
		return []taskIndex{t.Source}, nil
	default:
		return nil, fmt.Errorf("the mapping names %q, the rebuild task names source %q and candidate %q",
			rec.recordedID(), t.Source.recordedID(), t.Candidate.recordedID())
	}
}

// taskStore is the shard's task section in index.db.
type taskStore struct {
	ns *shardmeta.Namespace
}

func newTaskStore(db *shardmeta.DB) *taskStore {
	return &taskStore{ns: db.Namespace(tasksNamespace)}
}

// Load reads every task, keyed by vector name. Anything it cannot account
// for fails the load, as for the mapping.
func (t *taskStore) Load() (map[string]rebuildTask, error) {
	return readTasks(t.ns.ForEach)
}

// readTasks is taskStore.Load over the keys forEach walks.
func readTasks(forEach func(fn func(key, value []byte) error) error) (map[string]rebuildTask, error) {
	tasks := map[string]rebuildTask{}
	versioned := false
	err := forEach(func(key, value []byte) error {
		k := string(key)
		if k == formatVersionKey {
			err := checkTasksFormatVersion(value)
			if err != nil {
				return err
			}
			versioned = true
			return nil
		}
		name, ok := vectorName(k)
		if !ok {
			return fmt.Errorf("unknown key %q", k)
		}
		task, err := decodeTask(name, value)
		if err != nil {
			return err
		}
		tasks[name] = task
		return nil
	})
	if err == nil && !versioned && len(tasks) > 0 {
		err = errors.New("tasks without a format version")
	}
	if err != nil {
		return nil, fmt.Errorf("load vector index tasks: %w", err)
	}
	return tasks, nil
}

// Get returns name's task, and whether there is one.
func (t *taskStore) Get(name string) (rebuildTask, bool, error) {
	v, err := t.ns.Get([]byte(recordKey(name)))
	if err != nil || v == nil {
		return rebuildTask{}, false, err
	}
	task, err := decodeTask(name, v)
	if err != nil {
		return rebuildTask{}, false, err
	}
	return task, true, nil
}

// Put writes or overwrites name's task, and the format version with the
// first one.
func (t *taskStore) Put(name string, task rebuildTask) error {
	err := t.ns.Update(func(b *shardmeta.Batch) error {
		return putTask(b, name, task)
	})
	if err != nil {
		return fmt.Errorf("put vector index task %q: %w", name, err)
	}
	return nil
}

// Delete removes name's task. A missing task is already deleted.
func (t *taskStore) Delete(name string) error {
	err := t.ns.Update(func(b *shardmeta.Batch) error {
		return b.Delete([]byte(recordKey(name)))
	})
	if err != nil {
		return fmt.Errorf("delete vector index task %q: %w", name, err)
	}
	return nil
}

// putTask validates and writes one task inside a batch of the
// task section.
func putTask(b *shardmeta.Batch, name string, task rebuildTask) error {
	err := task.validate()
	if err != nil {
		return fmt.Errorf("task %q: %w", name, err)
	}
	value, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("task %q: %w", name, err)
	}
	version, err := b.Get([]byte(formatVersionKey))
	if err != nil {
		return err
	}
	if version == nil {
		err = b.Put([]byte(formatVersionKey), []byte(tasksFormatVersion))
		if err != nil {
			return err
		}
	}
	return b.Put([]byte(recordKey(name)), value)
}

func checkTasksFormatVersion(v []byte) error {
	if string(v) != tasksFormatVersion {
		return fmt.Errorf("unsupported format version %q, this binary reads %q", v, tasksFormatVersion)
	}
	return nil
}

func decodeTask(name string, value []byte) (rebuildTask, error) {
	var task rebuildTask
	err := json.Unmarshal(value, &task)
	if err == nil {
		err = task.validate()
	}
	if err != nil {
		return rebuildTask{}, fmt.Errorf("task %q: %w", name, err)
	}
	return task, nil
}
