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
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/weaviate/weaviate/adapters/repos/db/shardmeta"
)

// The mapping's on-disk layout inside index.db. Every key and field name
// below is read by later versions; changing one is a format change.
const (
	vectorIndexMappingNamespace        = "vector_index_mapping"
	vectorIndexMappingFormatVersionKey = "format_version"
	vectorIndexMappingFormatVersion    = "1"
	vectorIndexMappingLegacyKey        = "legacy"
	vectorIndexMappingNamedPrefix      = "named/"
)

// Record states. There is no dropped state: the schema carries deletion
// intent, and a record outlives it until cleanup completes.
const (
	vectorIndexStateCreating = "creating"
	vectorIndexStateReady    = "ready"
)

var (
	errVectorIndexMappingUninitialized = errors.New("vector index mapping is not initialized")
	errVectorIndexMappingInitialized   = errors.New("vector index mapping is already initialized")
)

// vectorIndexRecord is what the shard persists about one logical vector:
// where its index lives on disk, which implementation opens it, and whether
// creation completed.
type vectorIndexRecord struct {
	PhysicalID string `json:"physical_id"`
	IndexType  string `json:"index_type"`
	State      string `json:"state"`
}

// vectorIndexMapping is a shard's persisted view of which physical vector
// indexes it has: one record per logical vector, in index.db. The legacy
// vector is the empty name.
type vectorIndexMapping struct {
	ns *shardmeta.Namespace
}

func newVectorIndexMapping(db *shardmeta.DB) *vectorIndexMapping {
	return &vectorIndexMapping{ns: db.Namespace(vectorIndexMappingNamespace)}
}

// vectorIndexMappingKey is the on-disk key of a logical vector name.
func vectorIndexMappingKey(name string) string {
	if name == "" {
		return vectorIndexMappingLegacyKey
	}
	return vectorIndexMappingNamedPrefix + name
}

// vectorIndexMappingName is the inverse of vectorIndexMappingKey. ok is
// false for a key that does not name a vector, including a bare "named/":
// the empty name is the legacy vector, and only the legacy key may say so.
func vectorIndexMappingName(key string) (name string, ok bool) {
	if key == vectorIndexMappingLegacyKey {
		return "", true
	}
	name, found := strings.CutPrefix(key, vectorIndexMappingNamedPrefix)
	if found && name != "" {
		return name, true
	}
	return "", false
}

// validate rejects a record that no version of this code writes.
func (r vectorIndexRecord) validate() error {
	if r.PhysicalID == "" {
		return errors.New("empty physical id")
	}
	if r.IndexType == "" {
		return errors.New("empty index type")
	}
	if r.State != vectorIndexStateCreating && r.State != vectorIndexStateReady {
		return fmt.Errorf("unknown state %q", r.State)
	}
	return nil
}

// Load reads every record, keyed by logical name. initialized is false when
// the namespace has no format version, which is a shard from before the
// mapping existed and the signal for first-load initialization. Anything
// the mapping cannot account for fails the load: an unknown format version,
// a key that names no vector, a record that does not parse or validate.
func (m *vectorIndexMapping) Load() (records map[string]vectorIndexRecord, initialized bool, err error) {
	records = map[string]vectorIndexRecord{}
	err = m.ns.ForEach(func(key, value []byte) error {
		k := string(key)
		if k == vectorIndexMappingFormatVersionKey {
			if string(value) != vectorIndexMappingFormatVersion {
				return fmt.Errorf("unsupported format version %q, this binary reads %q",
					value, vectorIndexMappingFormatVersion)
			}
			initialized = true
			return nil
		}
		name, ok := vectorIndexMappingName(k)
		if !ok {
			return fmt.Errorf("unknown key %q", k)
		}
		var rec vectorIndexRecord
		err := json.Unmarshal(value, &rec)
		if err != nil {
			return fmt.Errorf("record %q: %w", name, err)
		}
		err = rec.validate()
		if err != nil {
			return fmt.Errorf("record %q: %w", name, err)
		}
		records[name] = rec
		return nil
	})
	if err != nil {
		return nil, false, fmt.Errorf("load vector index mapping: %w", err)
	}
	if !initialized && len(records) > 0 {
		return nil, false, errors.New("load vector index mapping: records without a format version")
	}
	return records, initialized, nil
}

// Get returns name's record, and whether the mapping has one.
func (m *vectorIndexMapping) Get(name string) (vectorIndexRecord, bool, error) {
	v, err := m.ns.Get([]byte(vectorIndexMappingKey(name)))
	if err != nil {
		return vectorIndexRecord{}, false, fmt.Errorf("get vector index mapping record %q: %w", name, err)
	}
	if v == nil {
		return vectorIndexRecord{}, false, nil
	}
	var rec vectorIndexRecord
	err = json.Unmarshal(v, &rec)
	if err != nil {
		return vectorIndexRecord{}, false, fmt.Errorf("record %q: %w", name, err)
	}
	return rec, true, nil
}

// Initialize writes the format version and every record in one
// transaction, or nothing. It is the first-load step for a shard whose
// Load reported initialized=false, and it refuses to run on a mapping
// that already has a format version.
func (m *vectorIndexMapping) Initialize(records map[string]vectorIndexRecord) error {
	err := m.ns.Update(func(b *shardmeta.Batch) error {
		existing, err := b.Get([]byte(vectorIndexMappingFormatVersionKey))
		if err != nil {
			return fmt.Errorf("read format version: %w", err)
		}
		if existing != nil {
			return errVectorIndexMappingInitialized
		}
		err = b.Put([]byte(vectorIndexMappingFormatVersionKey), []byte(vectorIndexMappingFormatVersion))
		if err != nil {
			return fmt.Errorf("write format version: %w", err)
		}
		for name, rec := range records {
			err = putVectorIndexRecord(b, name, rec)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("initialize vector index mapping: %w", err)
	}
	return nil
}

// putVectorIndexRecord validates and writes one record inside a batch.
func putVectorIndexRecord(b *shardmeta.Batch, name string, rec vectorIndexRecord) error {
	err := rec.validate()
	if err != nil {
		return fmt.Errorf("record %q: %w", name, err)
	}
	value, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("record %q: %w", name, err)
	}
	return b.Put([]byte(vectorIndexMappingKey(name)), value)
}

// Put writes or overwrites name's record. The mapping must be initialized:
// a record with no format version is what Load refuses.
func (m *vectorIndexMapping) Put(name string, rec vectorIndexRecord) error {
	err := m.ns.Update(func(b *shardmeta.Batch) error {
		err := requireVectorIndexMappingInitialized(b)
		if err != nil {
			return err
		}
		return putVectorIndexRecord(b, name, rec)
	})
	if err != nil {
		return fmt.Errorf("put vector index mapping record %q: %w", name, err)
	}
	return nil
}

func requireVectorIndexMappingInitialized(b *shardmeta.Batch) error {
	v, err := b.Get([]byte(vectorIndexMappingFormatVersionKey))
	if err != nil {
		return fmt.Errorf("read format version: %w", err)
	}
	if v == nil {
		return errVectorIndexMappingUninitialized
	}
	return nil
}

// Delete removes name's record. A missing record, or a mapping that was
// never initialized, is already deleted.
func (m *vectorIndexMapping) Delete(name string) error {
	err := m.ns.Update(func(b *shardmeta.Batch) error {
		return b.Delete([]byte(vectorIndexMappingKey(name)))
	})
	if err != nil {
		return fmt.Errorf("delete vector index mapping record %q: %w", name, err)
	}
	return nil
}
