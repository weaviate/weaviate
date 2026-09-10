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
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/dynamic"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// vectorIndexRecordFor is the record a vector gets when the shard creates its
// index under the naming rule: the derived physical ID and the schema's type.
func vectorIndexRecordFor(name string, cfg schemaConfig.VectorIndexConfig, state string) vectorIndexRecord {
	return vectorIndexRecord{PhysicalID: vectorIndexID(name), IndexType: cfg.IndexType(), State: state}
}

// vectorIndexHasStorage reports whether cfg describes a physical index. An
// hnsw config with skip set builds a no-op index that owns no files, so the
// mapping does not record it.
func vectorIndexHasStorage(cfg schemaConfig.VectorIndexConfig) bool {
	hnswCfg, ok := cfg.(hnswent.UserConfig)
	return !ok || !hnswCfg.Skip
}

// activeVectorIndexConfigs is the schema's vectors that own storage, keyed by
// logical name with the legacy vector under the empty name. Dropped vectors
// are already absent from targets: the schema parser skips them.
func activeVectorIndexConfigs(legacy schemaConfig.VectorIndexConfig,
	targets map[string]schemaConfig.VectorIndexConfig,
) map[string]schemaConfig.VectorIndexConfig {
	configs := make(map[string]schemaConfig.VectorIndexConfig, len(targets)+1)
	if legacy != nil && vectorIndexHasStorage(legacy) {
		configs[""] = legacy
	}
	for name, cfg := range targets {
		if vectorIndexHasStorage(cfg) {
			configs[name] = cfg
		}
	}
	return configs
}

// vectorIndexStorageDirsFor lists the directories rec occupies under this
// shard. A dynamic record's verdict is read through the shard's own handle.
func (s *Shard) vectorIndexStorageDirsFor(rec vectorIndexRecord) ([]string, error) {
	return vectorIndexStorageDirs(s.path(), s.metadataDB.Namespace(dynamic.StateNamespace), rec.IndexType, rec.PhysicalID)
}

// syncVectorIndexRecordStorage makes rec's directories durable, so a record
// that says ready never outlives the storage it points at.
func (s *Shard) syncVectorIndexRecordStorage(name string, rec vectorIndexRecord) error {
	dirs, err := s.vectorIndexStorageDirsFor(rec)
	if err != nil {
		return fmt.Errorf("vector %q: %w", name, err)
	}
	err = syncVectorIndexStorage(dirs)
	if err != nil {
		return fmt.Errorf("vector %q: %w", name, err)
	}
	return nil
}

// initVectorIndexMapping runs after a first-load build: every index the
// shard just created under the naming rule gets a ready record, once its
// directories are durable, all in one transaction. A shard from before the
// mapping existed and a brand-new shard both come through here once.
func (s *Shard) initVectorIndexMapping(configs map[string]schemaConfig.VectorIndexConfig) error {
	records := make(map[string]vectorIndexRecord, len(configs))
	for name, cfg := range configs {
		rec := vectorIndexRecordFor(name, cfg, vectorIndexStateReady)
		err := s.syncVectorIndexRecordStorage(name, rec)
		if err != nil {
			return err
		}
		records[name] = rec
	}
	err := s.mapping.Initialize(records)
	if err != nil {
		return err
	}
	return nil
}

// errVectorIndexStorageMissing is the fail-closed refusal: a record says an
// index is ready, and its directories are not on disk. An empty index in
// its place would serve nothing where there was data.
var errVectorIndexStorageMissing = errors.New("vector index storage is missing")

// reconcileVectorIndexMapping is a later load: every vector the schema has
// is opened at the ID its record holds, after the record and the storage
// are checked against each other, and every record the schema no longer
// has is deleted. Names are visited in order so a failure is deterministic.
func (s *Shard) reconcileVectorIndexMapping(ctx context.Context, legacy schemaConfig.VectorIndexConfig,
	targets map[string]schemaConfig.VectorIndexConfig, records map[string]vectorIndexRecord,
) error {
	configs := activeVectorIndexConfigs(legacy, targets)
	s.migrateCompressedVectors(legacy, targets)

	names := make([]string, 0, len(configs))
	for name := range configs {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		cfg := configs[name]
		rec, ok := records[name]
		if !ok {
			// added while the shard was cold, or by a version without the
			// mapping: recorded as creating first, like a crash mid-creation
			rec = vectorIndexRecordFor(name, cfg, vectorIndexStateCreating)
			err := s.mapping.Put(name, rec)
			if err != nil {
				return err
			}
		}
		if rec.IndexType != cfg.IndexType() {
			return fmt.Errorf("vector %q: the mapping records a %s index at %q but the schema says %s",
				name, rec.IndexType, rec.PhysicalID, cfg.IndexType())
		}
		err := s.openRecordedVectorIndex(ctx, name, cfg, rec)
		if err != nil {
			return err
		}
	}

	for name := range records {
		if _, active := configs[name]; active {
			continue
		}
		// dropped (the load-time sweep removed its files) or gone from the
		// schema without the marker: the record goes, the storage stays
		err := s.mapping.Delete(name)
		if err != nil {
			return err
		}
	}
	return nil
}

// openRecordedVectorIndex builds name's index at the recorded ID. A ready
// record is probed first and refused when its storage is gone; a creating
// record is built, made durable, and flipped to ready.
func (s *Shard) openRecordedVectorIndex(ctx context.Context, name string, cfg schemaConfig.VectorIndexConfig, rec vectorIndexRecord) error {
	if rec.State == vectorIndexStateReady {
		dirs, err := s.vectorIndexStorageDirsFor(rec)
		if err != nil {
			return fmt.Errorf("vector %q: %w", name, err)
		}
		exists, err := vectorIndexStorageExists(dirs)
		if err != nil {
			return fmt.Errorf("vector %q: %w", name, err)
		}
		if !exists {
			return fmt.Errorf("%w: vector %q is recorded ready at %q, expected %v on disk",
				errVectorIndexStorageMissing, name, rec.PhysicalID, dirs)
		}
		return s.createVectorIndex(ctx, name, rec.PhysicalID, cfg, s.lazySegmentLoadingEnabled)
	}

	err := s.createVectorIndex(ctx, name, rec.PhysicalID, cfg, s.lazySegmentLoadingEnabled)
	if err != nil {
		return err
	}
	err = s.syncVectorIndexRecordStorage(name, rec)
	if err != nil {
		return err
	}
	rec.State = vectorIndexStateReady
	err = s.mapping.Put(name, rec)
	if err != nil {
		return err
	}
	return nil
}
