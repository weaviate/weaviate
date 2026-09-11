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
	"maps"
	"slices"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/dynamic"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// reconcileVectorIndexMapping runs on every load after the first. The mapping
// says which indexes the shard has, the schema says which it should have, and
// the two can disagree after a crash, a schema change while the shard was
// cold, or lost files. It returns the record each schema vector is built
// from, in name order so a failure is deterministic:
//   - a ready record whose storage is on disk builds at the recorded ID;
//   - a ready record whose storage is gone refuses the load if the object
//     store holds a vector for it: an empty index in its place would silently
//     serve nothing where there was data. With nothing to index it is
//     rebuilt: a backup or a transfer carries no directory for an empty index;
//   - a creating record (a crash between the two writes of a creation) and a
//     missing record (a vector added while the shard was cold) are built,
//     then marked ready.
//
// A record the schema no longer has is deleted; its storage is left alone.
func (s *Shard) reconcileVectorIndexMapping(ctx context.Context, active map[string]schemaConfig.VectorIndexConfig,
	records map[string]vectorIndexRecord,
) (map[string]vectorIndexRecord, error) {
	toBuild := make(map[string]vectorIndexRecord, len(active))
	for _, name := range slices.Sorted(maps.Keys(active)) {
		cfg := active[name]
		rec, ok := records[name]
		if !ok {
			rec = vectorIndexRecordFor(name, cfg, vectorIndexStateCreating)
			err := s.mapping.Put(name, rec)
			if err != nil {
				return nil, err
			}
		}
		if rec.IndexType != cfg.IndexType() {
			return nil, fmt.Errorf("vector %q: the mapping records a %s index at %q but the schema says %s",
				name, rec.IndexType, rec.PhysicalID, cfg.IndexType())
		}
		if rec.State == vectorIndexStateReady {
			rebuild, err := s.readyVectorIndexNeedsRebuild(ctx, name, cfg, rec)
			if err != nil {
				return nil, err
			}
			if rebuild {
				rec.State = vectorIndexStateCreating
			}
		}
		toBuild[name] = rec
	}

	for name := range records {
		if _, ok := active[name]; ok {
			continue
		}
		// dropped or gone from the schema: the record goes, the storage stays
		err := s.mapping.Delete(name)
		if err != nil {
			return nil, err
		}
	}
	return toBuild, nil
}

// errVectorIndexStorageMissing: a ready record whose directories are gone.
// An empty index in their place would serve nothing where there was data.
var errVectorIndexStorageMissing = errors.New("vector index storage is missing")

// readyVectorIndexNeedsRebuild probes a ready record's storage. Present:
// nothing to do. Missing with vectors to index: the load is refused. Missing
// with nothing to index: the index is rebuilt.
func (s *Shard) readyVectorIndexNeedsRebuild(ctx context.Context, name string, cfg schemaConfig.VectorIndexConfig, rec vectorIndexRecord) (bool, error) {
	dirs, err := s.vectorIndexStorageDirsFor(rec)
	if err != nil {
		return false, fmt.Errorf("vector %q: %w", name, err)
	}
	exists, err := vectorIndexStorageExists(dirs)
	if err != nil {
		return false, fmt.Errorf("vector %q: %w", name, err)
	}
	if exists {
		return false, nil
	}
	hasVectors, err := s.hasVectorsFor(ctx, name, cfg.IsMultiVector())
	if err != nil {
		return false, fmt.Errorf("vector %q: %w", name, err)
	}
	if hasVectors {
		return false, fmt.Errorf("%w: vector %q is recorded ready at %q, expected %v on disk",
			errVectorIndexStorageMissing, name, rec.PhysicalID, dirs)
	}
	return true, nil
}

// commitVectorIndexRecords makes the indexes just built durable in the
// mapping. On the first load every record is written ready in one
// transaction; later, only the records that were creating are flipped.
func (s *Shard) commitVectorIndexRecords(records map[string]vectorIndexRecord, initialized bool) error {
	for name, rec := range records {
		if initialized && rec.State == vectorIndexStateReady {
			continue
		}
		if !initialized {
			err := s.syncVectorIndexRecordStorage(name, rec)
			if err != nil {
				return err
			}
			rec.State = vectorIndexStateReady
			records[name] = rec
			continue
		}
		err := s.markVectorIndexReady(name, rec)
		if err != nil {
			return err
		}
	}
	if initialized {
		return nil
	}
	return s.mapping.Initialize(records)
}

// markVectorIndexCreating records that name's index is about to be built at
// rec.PhysicalID. A crash from here on leaves a record the next load resumes.
func (s *Shard) markVectorIndexCreating(name string, rec vectorIndexRecord) error {
	rec.State = vectorIndexStateCreating
	return s.mapping.Put(name, rec)
}

// markVectorIndexReady makes rec's directories durable, then records the
// index as ready.
func (s *Shard) markVectorIndexReady(name string, rec vectorIndexRecord) error {
	err := s.syncVectorIndexRecordStorage(name, rec)
	if err != nil {
		return err
	}
	rec.State = vectorIndexStateReady
	return s.mapping.Put(name, rec)
}

// vectorIndexConfigsByStorage splits the schema's vectors, the legacy one
// under the empty name, into those that own storage and the skipped ones.
func vectorIndexConfigsByStorage(legacy schemaConfig.VectorIndexConfig,
	targets map[string]schemaConfig.VectorIndexConfig,
) (active, skipped map[string]schemaConfig.VectorIndexConfig) {
	active = make(map[string]schemaConfig.VectorIndexConfig, len(targets)+1)
	skipped = make(map[string]schemaConfig.VectorIndexConfig)
	if legacy != nil {
		targets = maps.Clone(targets)
		targets[""] = legacy
	}
	for name, cfg := range targets {
		if vectorIndexHasStorage(cfg) {
			active[name] = cfg
		} else {
			skipped[name] = cfg
		}
	}
	return active, skipped
}

// errVectorFound stops the object scan at the first vector.
var errVectorFound = errors.New("vector found")

// hasVectorsFor reports whether any object holds a vector for name.
func (s *Shard) hasVectorsFor(ctx context.Context, name string, multi bool) (bool, error) {
	var err error
	if multi {
		err = s.iterateOnLSMMultiVectors(ctx, 0, name, func(_ uint64, v [][]float32) error {
			if len(v) > 0 {
				return errVectorFound
			}
			return nil
		})
	} else {
		err = s.iterateOnLSMVectors(ctx, 0, name, func(_ uint64, v []float32) error {
			if len(v) > 0 {
				return errVectorFound
			}
			return nil
		})
	}
	if errors.Is(err, errVectorFound) {
		return true, nil
	}
	return false, err
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
