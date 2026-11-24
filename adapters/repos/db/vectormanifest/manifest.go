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
	"os"
	"path/filepath"
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
