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
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
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

func New(cfg Config) *Manifest {
	return &Manifest{
		records:      newRecordStore(cfg.DB),
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
	records, _, err := m.records.Load()
	if err != nil {
		return err
	}
	rec, ok := records[name]
	if !ok {
		return nil
	}
	return m.removeArtifacts(ctx, helpers.VectorIndexArtifactsForID(rec.storageID(), otherStorageIDs(records, name)))
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

// FinishDrop deletes everything name owns, its record last. A retry after a
// partial run finds less to do.
func (m *Manifest) FinishDrop(ctx context.Context, name string) error {
	records, _, err := m.records.Load()
	if err != nil {
		return err
	}
	if _, ok := records[name]; !ok {
		return nil
	}
	return m.finishDrop(ctx, name, records)
}

func (m *Manifest) finishDrop(ctx context.Context, name string, records map[string]record) error {
	artifacts, err := dropArtifacts(records, name)
	if err != nil {
		return fmt.Errorf("drop vector %q: %w", name, err)
	}
	err = m.removeArtifacts(ctx, artifacts)
	if err != nil {
		return fmt.Errorf("drop vector %q: %w", name, err)
	}
	err = dynamic.RemoveStateKeyIn(m.state, records[name].storageID())
	if err != nil {
		return fmt.Errorf("drop vector %q: %w", name, err)
	}
	// the record goes last, so a failed drop keeps it for the retry
	return m.records.Delete(name)
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

// dropArtifacts is what dropping name deletes: its index's files, minus the
// buckets another vector owns, and its queue.
func dropArtifacts(records map[string]record, name string) (helpers.VectorIndexArtifacts, error) {
	// a vector sharing a queue or files with name would lose them
	err := refuseSharedFiles(records)
	if err != nil {
		return helpers.VectorIndexArtifacts{}, err
	}
	rec := records[name]
	artifacts := helpers.VectorIndexArtifactsForID(rec.storageID(), otherStorageIDs(records, name))
	artifacts.ShardDirs = append(artifacts.ShardDirs, helpers.QueueDirName(rec.queueIDOrDefault()))
	return artifacts, nil
}

// otherStorageIDs lists the storage IDs of every vector but name.
func otherStorageIDs(records map[string]record, name string) []string {
	ids := make([]string, 0, len(records))
	for other, rec := range records {
		if other != name {
			ids = append(ids, rec.storageID())
		}
	}
	return ids
}
