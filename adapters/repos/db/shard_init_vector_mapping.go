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

// initVectorIndexMapping records every index of a first-load build as
// ready, in one transaction, once its directories are durable.
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
	return s.mapping.Initialize(records)
}

// reconcileVectorIndexMapping runs on every load after the first. The mapping
// says which indexes the shard has, the schema says which it should have, and
// the two can disagree after a crash, a schema change while the shard was
// cold, or lost files. For each vector the schema has:
//   - a ready record whose storage is on disk opens at the recorded ID;
//   - a ready record whose storage is gone refuses the load: an empty index in
//     its place would silently serve nothing where there was data;
//   - a creating record (a crash between the two writes of a creation) is
//     built, then marked ready;
//   - no record (a vector added while the shard was cold) is treated like
//     creating.
//
// A record the schema no longer has is deleted; its storage is left alone.
func (s *Shard) reconcileVectorIndexMapping(ctx context.Context, legacy schemaConfig.VectorIndexConfig,
	targets map[string]schemaConfig.VectorIndexConfig, records map[string]vectorIndexRecord,
) error {
	configs := activeVectorIndexConfigs(legacy, targets)
	s.migrateCompressedVectors(legacy, targets)

	// in name order, so a failure is deterministic
	names := make([]string, 0, len(configs))
	for name := range configs {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		cfg := configs[name]
		rec, ok := records[name]
		if !ok {
			// added while the shard was cold: treated like a crash mid-creation
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
		// dropped or gone from the schema: the record goes, the storage stays
		err := s.mapping.Delete(name)
		if err != nil {
			return err
		}
	}
	return nil
}

// errVectorIndexStorageMissing: a ready record whose directories are gone.
// An empty index in their place would serve nothing where there was data.
var errVectorIndexStorageMissing = errors.New("vector index storage is missing")

// openRecordedVectorIndex builds name's index at the recorded ID: a ready
// record is probed first, a creating one is built, synced and flipped.
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

// activeVectorIndexConfigs is the schema's vectors that own storage, the
// legacy one under the empty name.
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

// vectorIndexHasStorage is false for a skipped hnsw config: a no-op index
// owns no files, so the mapping does not record it.
func vectorIndexHasStorage(cfg schemaConfig.VectorIndexConfig) bool {
	hnswCfg, ok := cfg.(hnswent.UserConfig)
	return !ok || !hnswCfg.Skip
}

// vectorIndexRecordFor is the record of a vector created under the naming rule.
func vectorIndexRecordFor(name string, cfg schemaConfig.VectorIndexConfig, state string) vectorIndexRecord {
	return vectorIndexRecord{PhysicalID: vectorIndexID(name), IndexType: cfg.IndexType(), State: state}
}

// vectorIndexStorageDirsFor lists the directories rec occupies under this shard.
func (s *Shard) vectorIndexStorageDirsFor(rec vectorIndexRecord) ([]string, error) {
	return vectorIndexStorageDirs(s.path(), s.metadataDB.Namespace(dynamic.StateNamespace), rec.IndexType, rec.PhysicalID)
}

// syncVectorIndexRecordStorage makes rec's directories durable before the
// record says ready.
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
