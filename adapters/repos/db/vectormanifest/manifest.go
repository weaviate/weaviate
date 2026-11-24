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

// Package vectormanifest owns the state a shard persists about its vector
// indexes: which index on disk each vector uses, in <shard>/index.db. Only
// this package reads or writes that state, and only it decides which files
// belong to a vector.
package vectormanifest

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/shardmeta"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/dynamic"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
)

// Config is what a Manifest needs from the shard that owns it.
type Config struct {
	DB       *shardmeta.DB
	ShardDir string
	// RemoveBucket deletes one of the shard's LSM buckets, open or not.
	RemoveBucket func(ctx context.Context, bucket string) error
	Logger       logrus.FieldLogger
}

// Manifest is the vector index state of one loaded shard. The legacy vector
// is the empty name.
type Manifest struct {
	records      *recordStore
	tasks        *taskStore
	state        dynamic.StateOps
	shardDir     string
	removeBucket func(ctx context.Context, bucket string) error
	logger       logrus.FieldLogger

	// mu serializes the checks and writes that record a change.
	mu sync.Mutex
	// opened holds the records Open returned that Ready has not seen yet.
	opened map[string]record
	// firstLoad is set until Ready writes the first records.
	firstLoad bool
}

// Entry is where a vector's index lives and what it is opened with.
type Entry struct {
	Name string
	// StorageID is the ID the index's files are named by.
	StorageID string
	// QueueID is the ID the queue's directory is named by.
	QueueID   string
	IndexType string
	Config    schemaConfig.VectorIndexConfig
}

// contents is everything the manifest has on disk, by vector name.
type contents struct {
	records map[string]record
	tasks   map[string]rebuildTask
}

// names lists the vectors that have a record or a task, in order.
func (c contents) names() []string {
	names := slices.AppendSeq(slices.Collect(maps.Keys(c.records)), maps.Keys(c.tasks))
	slices.Sort(names)
	return slices.Compact(names)
}

// storageIDs lists the IDs name's files are under: its record's, and the two
// of its task.
func (c contents) storageIDs(name string) []string {
	var ids []string
	if rec, ok := c.records[name]; ok {
		ids = append(ids, rec.storageID())
	}
	if task, ok := c.tasks[name]; ok {
		ids = append(ids, task.Source.storageID(), task.Candidate.storageID())
	}
	slices.Sort(ids)
	return slices.Compact(ids)
}

// otherStorageIDs lists the storage IDs of every vector but name.
func (c contents) otherStorageIDs(name string) []string {
	var ids []string
	for _, other := range c.names() {
		if other != name {
			ids = append(ids, c.storageIDs(other)...)
		}
	}
	return ids
}

// indexArtifacts is what the index at id owns alone.
func (c contents) indexArtifacts(id string) helpers.VectorIndexArtifacts {
	var others []string
	for _, name := range c.names() {
		others = append(others, c.storageIDs(name)...)
	}
	return helpers.VectorIndexArtifactsForID(id, others)
}

func New(cfg Config) *Manifest {
	return &Manifest{
		records:      newRecordStore(cfg.DB),
		tasks:        newTaskStore(cfg.DB),
		state:        cfg.DB.Namespace(dynamic.StateNamespace),
		shardDir:     cfg.ShardDir,
		removeBucket: cfg.RemoveBucket,
		logger:       cfg.Logger,
	}
}

func entryOf(name string, rec record, cfg schemaConfig.VectorIndexConfig) Entry {
	return Entry{
		Name:      name,
		StorageID: rec.storageID(),
		QueueID:   rec.queueIDOrDefault(),
		IndexType: rec.IndexType,
		Config:    cfg,
	}
}

// Open puts the persisted state in order against schema, the vectors that own
// storage, and returns the indexes to open, in name order. Nothing may use the
// shard's vector indexes before it. firstLoad is true for a shard that has no
// records yet; Ready writes them.
func (m *Manifest) Open(ctx context.Context, schema map[string]schemaConfig.VectorIndexConfig,
) (entries []Entry, firstLoad bool, err error) {
	records, initialized, stripped, err := m.records.LoadAndStrip()
	if err != nil {
		return nil, false, err
	}
	tasks, err := m.tasks.Load()
	if err != nil {
		return nil, false, err
	}
	if !initialized && len(tasks) > 0 {
		return nil, false, errors.New("rebuild tasks without a vector index mapping")
	}
	if len(stripped) > 0 {
		m.logger.Infof("rewrote vector index mapping records written by a newer version, dropping fields this version does not know: %v", stripped)
	}
	if initialized {
		records, err = m.reconcile(ctx, schema, contents{records: records, tasks: tasks})
		if err != nil {
			return nil, false, err
		}
	} else {
		records = make(map[string]record, len(schema))
		for name, cfg := range schema {
			records[name] = recordFor(name, cfg, stateCreating)
		}
		for _, c := range collisions(ownersOf(records)) {
			m.logger.Warnf("vector index physical names collide, a drop of either may take the other's files: %s", c)
		}
	}

	m.mu.Lock()
	m.opened = records
	m.firstLoad = !initialized
	m.mu.Unlock()

	for _, name := range slices.Sorted(maps.Keys(records)) {
		entries = append(entries, entryOf(name, records[name], schema[name]))
	}
	return entries, !initialized, nil
}

// reconcile matches the records with the schema after a crash, a schema change
// while the shard was cold, or lost files. It returns the record each schema
// vector is built from:
//   - a ready record whose storage is on disk builds at the recorded ID;
//   - a ready record whose storage is gone is rebuilt: a backup or a
//     transfer carries no directory for an index that wrote nothing;
//   - a creating record (a crash between the two writes of a creation) and a
//     missing record (a vector added while the shard was cold) are built,
//     then marked ready.
//
// Interrupted rebuilds, then drops, are finished first. A record the schema no
// longer has is deleted; its storage is left alone.
func (m *Manifest) reconcile(ctx context.Context, schema map[string]schemaConfig.VectorIndexConfig,
	c contents,
) (map[string]record, error) {
	// before any deletion: finishing a drop would take the other's queue
	err := refuseSharedFiles(c)
	if err != nil {
		return nil, err
	}
	err = m.finishRebuilds(ctx, schema, c)
	if err != nil {
		return nil, err
	}
	records := c.records

	for _, name := range slices.Sorted(maps.Keys(records)) {
		if records[name].State != stateDropping {
			continue
		}
		// a deferred deletion the process did not live to finish
		err := m.finishDrop(ctx, name, c)
		if err != nil {
			return nil, err
		}
		delete(records, name)
		delete(c.tasks, name)
	}

	toBuild := make(map[string]record, len(schema))
	owners := ownersOf(records)
	for _, name := range slices.Sorted(maps.Keys(schema)) {
		cfg := schema[name]
		rec, ok := records[name]
		if !ok {
			// a vector without a record is a newcomer: refused like a live
			// creation when its files belong to another vector, and counted as
			// an owner for the newcomers after it
			rec = recordFor(name, cfg, stateCreating)
			if shared := collisionsWith(owners, name, ownerOf(rec)); len(shared) > 0 {
				return nil, collisionError(name, shared)
			}
			err := m.records.Put(name, rec)
			if err != nil {
				return nil, err
			}
			owners[name] = ownerOf(rec)
		}
		if rec.IndexType != cfg.IndexType() {
			return nil, fmt.Errorf("vector %q: the mapping records a %s index at %q but the schema says %s",
				name, rec.IndexType, rec.recordedID(), cfg.IndexType())
		}
		if rec.State == stateReady {
			exists, err := m.hasStorage(rec.IndexType, rec.storageID())
			if err != nil {
				return nil, fmt.Errorf("vector %q: %w", name, err)
			}
			if !exists {
				// under async indexing this includes an index whose vectors
				// are all still queued
				rec.State = stateCreating
			}
		}
		toBuild[name] = rec
	}

	for name := range records {
		if _, ok := schema[name]; ok {
			continue
		}
		// dropped or gone from the schema: the record goes, the storage stays
		err := m.records.Delete(name)
		if err != nil {
			return nil, err
		}
	}
	return toBuild, nil
}

// finishRebuilds finishes the rebuilds a restart interrupted: the index the
// record names stays, the others its task names go, then the task. It runs
// before any queue opens, which deletes the chunks a rebuild kept. Every task
// is checked before anything is deleted.
func (m *Manifest) finishRebuilds(ctx context.Context, schema map[string]schemaConfig.VectorIndexConfig,
	c contents,
) error {
	names := slices.Sorted(maps.Keys(c.tasks))
	retired := make(map[string][]taskIndex, len(names))
	for _, name := range names {
		rec, recorded := c.records[name]
		if _, ok := schema[name]; ok && !recorded {
			return fmt.Errorf("vector %q: a rebuild task but no record", name)
		}
		if recorded && rec.State == stateDropping {
			continue // the drop takes both indexes
		}
		var err error
		retired[name], err = c.tasks[name].retired(rec, recorded)
		if err != nil {
			return fmt.Errorf("vector %q: %w", name, err)
		}
	}
	for _, name := range names {
		rec, recorded := c.records[name]
		if recorded && rec.State == stateDropping {
			continue
		}
		task := c.tasks[name]
		m.logger.WithFields(logrus.Fields{
			"target_vector": name,
			"task_id":       task.TaskID,
			"task_version":  task.TaskVersion,
			"source":        task.Source.recordedID(),
			"candidate":     task.Candidate.recordedID(),
		}).Infof("finishing a vector index rebuild interrupted by a restart, removing %v", retired[name])
		if recorded && rec.storageID() == task.Candidate.storageID() {
			err := queue.RewindDir(filepath.Join(m.shardDir, helpers.QueueDirName(rec.queueIDOrDefault())))
			if err != nil {
				return fmt.Errorf("vector %q: serve the writes kept during the rebuild again: %w", name, err)
			}
		}
		err := m.finishRebuild(ctx, name, retired[name], c)
		if err != nil {
			return err
		}
		delete(c.tasks, name)
	}
	return nil
}

// finishRebuild deletes the retired indexes of name's rebuild, then its task,
// so a failed cleanup is retried at the next load.
func (m *Manifest) finishRebuild(ctx context.Context, name string, retired []taskIndex, c contents) error {
	others := c.otherStorageIDs(name)
	if rec, ok := c.records[name]; ok {
		others = append(others, rec.storageID())
	}
	for _, index := range retired {
		id := index.storageID()
		err := m.removeArtifacts(ctx, helpers.VectorIndexArtifactsForID(id, others))
		if err == nil {
			err = dynamic.RemoveStateKeyIn(m.state, id)
		}
		if err != nil {
			return fmt.Errorf("vector %q: remove the %s index at %q: %w", name, index.IndexType, index.recordedID(), err)
		}
	}
	return m.tasks.Delete(name)
}

// Create records that name's index is about to be built under the naming rule
// and returns where. It refuses a vector whose files another one owns; a retry
// of an interrupted creation is not refused.
func (m *Manifest) Create(name string, cfg schemaConfig.VectorIndexConfig) (Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	records, _, err := m.records.Load()
	if err != nil {
		return Entry{}, err
	}
	rec := recordFor(name, cfg, stateCreating)
	if shared := collisionsWith(ownersOf(records), name, ownerOf(rec)); len(shared) > 0 {
		return Entry{}, collisionError(name, shared)
	}
	err = m.records.Put(name, rec)
	if err != nil {
		return Entry{}, err
	}
	return entryOf(name, rec, cfg), nil
}

// Ready records that the indexes of built are complete, each with the config
// it was built with, after making their directories durable. The call after a
// first Open takes every vector and writes all the records at once.
//
// The schema decides the config at load, so a ready record whose config
// differs is rewritten: a shard that was cold during a config change picks it
// up here, and a record from before the config and queue ID fields existed
// gets them.
func (m *Manifest) Ready(built map[string]schemaConfig.VectorIndexConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.firstLoad {
		return m.initialize(built)
	}
	for _, name := range slices.Sorted(maps.Keys(built)) {
		rec, ok := m.opened[name]
		if !ok {
			var err error
			rec, ok, err = m.records.Get(name)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("vector %q has no mapping record", name)
			}
		}
		adopted, changed := rec.adopted(built[name])
		if rec.State != stateReady {
			err := m.syncStorage(name, adopted)
			if err != nil {
				return err
			}
			adopted.State = stateReady
			changed = true
		}
		if changed {
			err := m.records.Put(name, adopted)
			if err != nil {
				return err
			}
		}
		delete(m.opened, name)
	}
	return nil
}

// initialize writes the records of a first load.
func (m *Manifest) initialize(built map[string]schemaConfig.VectorIndexConfig) error {
	records := make(map[string]record, len(m.opened))
	for name, rec := range m.opened {
		cfg, ok := built[name]
		if !ok {
			return fmt.Errorf("vector %q was opened but not built", name)
		}
		rec, _ = rec.adopted(cfg)
		err := m.syncStorage(name, rec)
		if err != nil {
			return err
		}
		rec.State = stateReady
		records[name] = rec
	}
	err := m.records.Initialize(records)
	if err != nil {
		return err
	}
	m.firstLoad = false
	m.opened = nil
	return nil
}

// Reset records that name's index is about to be emptied and built again in
// place: a crash from here on leaves a record the next load resumes.
func (m *Manifest) Reset(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	rec, ok, err := m.records.Get(name)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("vector %q has no mapping record", name)
	}
	rec.State = stateCreating
	return m.records.Put(name, rec)
}

// RemoveIndexFiles deletes the files of name's index and keeps its queue and
// its record.
func (m *Manifest) RemoveIndexFiles(ctx context.Context, name string) error {
	c, err := m.load()
	if err != nil {
		return err
	}
	rec, ok := c.records[name]
	if !ok {
		return nil
	}
	return m.removeArtifacts(ctx, c.indexArtifacts(rec.storageID()))
}

// BeginDrop records that name is being dropped, so a crash before its files go
// is finished at the next load. found is false for a vector without a record,
// which owns nothing: a skipped vector, or a drop that already completed.
func (m *Manifest) BeginDrop(name string) (found bool, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	rec, ok, err := m.records.Get(name)
	if err != nil || !ok {
		return false, err
	}
	rec.State = stateDropping
	return true, m.records.Put(name, rec)
}

// FinishDrop deletes everything name owns, its record and its rebuild task
// last. A retry after a partial run finds less to do.
func (m *Manifest) FinishDrop(ctx context.Context, name string) error {
	m.mu.Lock()
	c, err := m.load()
	m.mu.Unlock()
	if err != nil {
		return err
	}
	if _, ok := c.records[name]; !ok {
		return nil
	}
	return m.finishDrop(ctx, name, c)
}

func (m *Manifest) finishDrop(ctx context.Context, name string, c contents) error {
	artifacts, err := dropArtifacts(c, name)
	if err != nil {
		return fmt.Errorf("drop vector %q: %w", name, err)
	}
	err = m.removeArtifacts(ctx, artifacts)
	if err != nil {
		return fmt.Errorf("drop vector %q: %w", name, err)
	}
	for _, id := range c.storageIDs(name) {
		err = dynamic.RemoveStateKeyIn(m.state, id)
		if err != nil {
			return fmt.Errorf("drop vector %q: %w", name, err)
		}
	}
	// the record goes last, so a failed drop keeps it for the retry
	if _, ok := c.tasks[name]; !ok {
		return m.records.Delete(name)
	}
	key := []byte(recordKey(name))
	err = shardmeta.Update([]*shardmeta.Namespace{m.records.ns, m.tasks.ns}, func(b []*shardmeta.Batch) error {
		err := b[0].Delete(key)
		if err != nil {
			return err
		}
		return b[1].Delete(key)
	})
	if err != nil {
		return fmt.Errorf("delete the record and rebuild task of vector %q: %w", name, err)
	}
	return nil
}

// load reads the records and the tasks.
func (m *Manifest) load() (contents, error) {
	records, _, err := m.records.Load()
	if err != nil {
		return contents{}, err
	}
	tasks, err := m.tasks.Load()
	if err != nil {
		return contents{}, err
	}
	return contents{records: records, tasks: tasks}, nil
}

// Get returns name's entry with the config its record holds, and whether it
// has one.
func (m *Manifest) Get(name string) (Entry, bool, error) {
	rec, ok, err := m.records.Get(name)
	if err != nil || !ok {
		return Entry{}, false, err
	}
	return entryOf(name, rec, rec.Config), true, nil
}

// HasStorage reports whether e's index has its directories on disk.
func (m *Manifest) HasStorage(e Entry) (bool, error) {
	exists, err := m.hasStorage(e.IndexType, e.StorageID)
	if err != nil {
		return false, fmt.Errorf("vector %q: %w", e.Name, err)
	}
	return exists, nil
}

func (m *Manifest) hasStorage(indexType, storageID string) (bool, error) {
	dirs, err := storageDirs(m.shardDir, m.state, indexType, storageID)
	if err != nil {
		return false, err
	}
	return storageExists(dirs)
}

// syncStorage makes rec's directories durable before the record says ready.
func (m *Manifest) syncStorage(name string, rec record) error {
	dirs, err := storageDirs(m.shardDir, m.state, rec.IndexType, rec.storageID())
	if err != nil {
		return fmt.Errorf("vector %q: %w", name, err)
	}
	err = syncStorage(dirs)
	if err != nil {
		return fmt.Errorf("vector %q: %w", name, err)
	}
	return nil
}

func (m *Manifest) removeArtifacts(ctx context.Context, artifacts helpers.VectorIndexArtifacts) error {
	for _, bucket := range artifacts.LSMBuckets {
		err := m.removeBucket(ctx, bucket)
		if err != nil {
			return fmt.Errorf("remove bucket %q: %w", bucket, err)
		}
	}
	for _, dir := range artifacts.ShardDirs {
		err := os.RemoveAll(filepath.Join(m.shardDir, dir))
		if err != nil {
			return fmt.Errorf("remove directory %q: %w", dir, err)
		}
	}
	return nil
}

// dropArtifacts is what dropping name deletes: the files of every index it has,
// minus the buckets another vector owns, and its queue.
func dropArtifacts(c contents, name string) (helpers.VectorIndexArtifacts, error) {
	// a vector sharing a queue or files with name would lose them
	err := refuseSharedFiles(c)
	if err != nil {
		return helpers.VectorIndexArtifacts{}, err
	}
	var artifacts helpers.VectorIndexArtifacts
	others := c.otherStorageIDs(name)
	for _, id := range c.storageIDs(name) {
		a := helpers.VectorIndexArtifactsForID(id, others)
		artifacts.LSMBuckets = append(artifacts.LSMBuckets, a.LSMBuckets...)
		artifacts.ShardDirs = append(artifacts.ShardDirs, a.ShardDirs...)
	}
	if rec, ok := c.records[name]; ok {
		artifacts.ShardDirs = append(artifacts.ShardDirs, helpers.QueueDirName(rec.queueIDOrDefault()))
	}
	return artifacts, nil
}
