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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/shardmeta"
)

var (
	errUninitialized = errors.New("vector index mapping is not initialized")
	errInitialized   = errors.New("vector index mapping is already initialized")
)

// recordStore is a shard's persisted view of which physical vector
// indexes it has: one record per logical vector, in index.db. The legacy
// vector is the empty name.
type recordStore struct {
	ns *shardmeta.Namespace
}

func newRecordStore(db *shardmeta.DB) *recordStore {
	return &recordStore{ns: db.Namespace(namespace)}
}

// Load reads every record, keyed by logical name. initialized is false when
// the namespace has no format version, which is a shard from before the
// mapping existed and the signal for first-load initialization. Anything
// the mapping cannot account for fails the load: an unknown format version,
// a key that names no vector, a record that does not parse or validate.
func (m *recordStore) Load() (records map[string]record, initialized bool, err error) {
	records, initialized, _, err = m.load()
	return records, initialized, err
}

// LoadAndStrip is Load, then rewrites in one transaction every record whose
// stored value is not what this binary writes, and returns their names. A
// later version adds fields an older reader ignores but keeps on disk; they
// would go stale here and be trusted again after an upgrade. A mapping this
// binary wrote is not written to.
func (m *recordStore) LoadAndStrip() (records map[string]record, initialized bool, stripped []string, err error) {
	records, initialized, stripped, err = m.load()
	if err != nil || len(stripped) == 0 {
		return records, initialized, nil, err
	}
	err = m.ns.Update(func(b *shardmeta.Batch) error {
		for _, name := range stripped {
			err := putRecord(b, name, records[name])
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, false, nil, fmt.Errorf("strip vector index mapping records: %w", err)
	}
	return records, initialized, stripped, nil
}

// load is Load, and also names the records whose stored value differs from
// what this binary would write for them.
func (m *recordStore) load() (records map[string]record, initialized bool, foreign []string, err error) {
	return readRecords(m.ns.ForEach)
}

// readRecords is recordStore.load over the keys forEach walks.
func readRecords(forEach func(fn func(key, value []byte) error) error,
) (records map[string]record, initialized bool, foreign []string, err error) {
	records = map[string]record{}
	err = forEach(func(key, value []byte) error {
		k := string(key)
		if k == formatVersionKey {
			if string(value) != formatVersion {
				return fmt.Errorf("unsupported format version %q, this binary reads %q",
					value, formatVersion)
			}
			initialized = true
			return nil
		}
		name, ok := vectorName(k)
		if !ok {
			return fmt.Errorf("unknown key %q", k)
		}
		var rec record
		err := json.Unmarshal(value, &rec)
		if err != nil {
			return fmt.Errorf("record %q: %w", name, err)
		}
		err = rec.validate()
		if err != nil {
			return fmt.Errorf("record %q: %w", name, err)
		}
		own, err := json.Marshal(rec)
		if err != nil {
			return fmt.Errorf("record %q: %w", name, err)
		}
		if !bytes.Equal(own, value) {
			foreign = append(foreign, name)
		}
		records[name] = rec
		return nil
	})
	if err != nil {
		return nil, false, nil, fmt.Errorf("load vector index mapping: %w", err)
	}
	if !initialized && len(records) > 0 {
		return nil, false, nil, errors.New("load vector index mapping: records without a format version")
	}
	return records, initialized, foreign, nil
}

// Get returns name's record, and whether the mapping has one.
func (m *recordStore) Get(name string) (record, bool, error) {
	v, err := m.ns.Get([]byte(recordKey(name)))
	if err != nil {
		return record{}, false, fmt.Errorf("get vector index mapping record %q: %w", name, err)
	}
	if v == nil {
		return record{}, false, nil
	}
	var rec record
	err = json.Unmarshal(v, &rec)
	if err != nil {
		return record{}, false, fmt.Errorf("record %q: %w", name, err)
	}
	return rec, true, nil
}

// Initialize writes the format version and every record in one
// transaction, or nothing. It is the first-load step for a shard whose
// Load reported initialized=false, and it refuses to run on a mapping
// that already has a format version.
func (m *recordStore) Initialize(records map[string]record) error {
	err := m.ns.Update(func(b *shardmeta.Batch) error {
		existing, err := b.Get([]byte(formatVersionKey))
		if err != nil {
			return fmt.Errorf("read format version: %w", err)
		}
		if existing != nil {
			return errInitialized
		}
		err = b.Put([]byte(formatVersionKey), []byte(formatVersion))
		if err != nil {
			return fmt.Errorf("write format version: %w", err)
		}
		for name, rec := range records {
			err = putRecord(b, name, rec)
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

// putRecord validates and writes one record inside a batch.
func putRecord(b *shardmeta.Batch, name string, rec record) error {
	err := rec.validate()
	if err != nil {
		return fmt.Errorf("record %q: %w", name, err)
	}
	value, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("record %q: %w", name, err)
	}
	return b.Put([]byte(recordKey(name)), value)
}

// Put writes or overwrites name's record. The mapping must be initialized:
// a record with no format version is what Load refuses.
func (m *recordStore) Put(name string, rec record) error {
	err := m.ns.Update(func(b *shardmeta.Batch) error {
		err := requireInitialized(b)
		if err != nil {
			return err
		}
		return putRecord(b, name, rec)
	})
	if err != nil {
		return fmt.Errorf("put vector index mapping record %q: %w", name, err)
	}
	return nil
}

func requireInitialized(b *shardmeta.Batch) error {
	v, err := b.Get([]byte(formatVersionKey))
	if err != nil {
		return fmt.Errorf("read format version: %w", err)
	}
	if v == nil {
		return errUninitialized
	}
	return nil
}

// Delete removes name's record. A missing record, or a mapping that was
// never initialized, is already deleted.
func (m *recordStore) Delete(name string) error {
	err := m.ns.Update(func(b *shardmeta.Batch) error {
		return b.Delete([]byte(recordKey(name)))
	})
	if err != nil {
		return fmt.Errorf("delete vector index mapping record %q: %w", name, err)
	}
	return nil
}
