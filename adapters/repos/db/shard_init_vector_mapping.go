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
	"fmt"

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
	return !(ok && hnswCfg.Skip)
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
