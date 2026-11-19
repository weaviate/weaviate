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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strings"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/shardmeta"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/vectorindex"
)

// The mapping's on-disk layout inside index.db. Every key and field name
// below is read by later versions; changing one is a format change. New
// information goes into fields of the existing records, never into a new
// key or a new state: a reader from before the field ignores it.
// A named vector is keyed by its name. The two other keys start with a dot,
// which a vector name cannot, so they can never collide with one.
const (
	vectorIndexMappingNamespace        = "vector_index_mapping"
	vectorIndexMappingFormatVersionKey = ".format_version"
	vectorIndexMappingFormatVersion    = "1"
	vectorIndexMappingLegacyKey        = ".legacy"
)

// Record states. There is no dropped state: the schema carries deletion
// intent, and a record outlives it until cleanup completes.
const (
	vectorIndexStateCreating = "creating"
	vectorIndexStateReady    = "ready"
	// dropping: the slot is gone, the files are not yet; the next load finishes it
	vectorIndexStateDropping = "dropping"
)

var (
	errVectorIndexMappingUninitialized = errors.New("vector index mapping is not initialized")
	errVectorIndexMappingInitialized   = errors.New("vector index mapping is already initialized")
)

// vectorIndexRecord is what the shard persists about one logical vector:
// where its index lives on disk, which implementation opens it, whether
// creation completed, which queue feeds it, and the config the index
// adopted.
//
// QueueID and Config were added after the first records were written, as
// fields inside the record so an older reader ignores them. A record from
// before then has neither: the shard fills both in when it loads, the queue
// ID from the physical ID (the queue has always lived under the index's ID)
// and the config from the schema. The IDs differ once the queue outlives
// the index it was created with.
type vectorIndexRecord struct {
	PhysicalID string `json:"physical_id"`
	IndexType  string `json:"index_type"`
	State      string `json:"state"`
	// QueueID names the queue directory, "<QueueID>.queue.d". Empty in a
	// record written before the field existed.
	QueueID string `json:"queue_id,omitempty"`
	// Config is the parsed config the index was built with at its creation
	// or last load, a hnsw, flat, dynamic or hfresh UserConfig written with
	// its API field names, every default filled in. A config update to a
	// loaded index reaches the record at the next load. Nil in a record
	// written before the field existed.
	Config schemaConfig.VectorIndexConfig `json:"config,omitempty"`
}

// UnmarshalJSON decodes Config by IndexType; the field is an interface the
// standard decoder cannot fill on its own.
func (r *vectorIndexRecord) UnmarshalJSON(data []byte) error {
	type plain vectorIndexRecord
	var aux struct {
		plain
		Config json.RawMessage `json:"config"`
	}
	err := json.Unmarshal(data, &aux)
	if err != nil {
		return err
	}
	*r = vectorIndexRecord(aux.plain)
	r.Config = nil
	if len(aux.Config) == 0 || string(aux.Config) == "null" {
		return nil
	}
	cfg, err := vectorindex.UnmarshalConfig(r.IndexType, aux.Config)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	r.Config = cfg
	return nil
}

// storageID is the ID r's index files are named by. Every reader of the
// physical ID goes through it; a test refuses any other read of the field.
func (r vectorIndexRecord) storageID() string {
	return vectorIndexStorageID(r.PhysicalID)
}

// recordedID is the physical ID as the record holds it, for messages only:
// files are named by storageID.
func (r vectorIndexRecord) recordedID() string {
	return r.PhysicalID
}

// vectorIndexStorageID is the naming rule from a physical ID to the ID its
// files are named by. "main" and "vectors_<x>" keep their names; any other ID
// is named as if it were "vectors_<id>". The index implementations treat an
// ID without the "vectors_" prefix as the legacy vector's, so an ID that
// skipped this rule would name the legacy vector's files.
func vectorIndexStorageID(physicalID string) string {
	if physicalID == vectorIndexID("") || strings.HasPrefix(physicalID, helpers.VectorsBucketLSM+"_") {
		return physicalID
	}
	return helpers.VectorsBucketLSM + "_" + physicalID
}

var (
	vectorIndexDefaultIDPattern   = regexp.MustCompile(`^` + helpers.VectorsBucketLSM + `_` + schema.TargetVectorNameRegex + `$`)
	vectorIndexGeneratedIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]*-[A-Za-z0-9_-]*$`)
)

// validateVectorIndexID accepts the IDs the naming rule gives a vector name,
// "main" and "vectors_<name>", and generated IDs, which contain a "-" so they
// can never equal one of those: a vector name has no "-". The schema's
// collision check reads names only and relies on it.
func validateVectorIndexID(id string) error {
	if id == vectorIndexID("") || vectorIndexDefaultIDPattern.MatchString(id) || vectorIndexGeneratedIDPattern.MatchString(id) {
		return nil
	}
	return fmt.Errorf("%q is neither %q, \"vectors_<vector name>\", nor an ID with a \"-\" made of letters, digits, \"_\" and \"-\"",
		id, vectorIndexID(""))
}

// queueIDOrDefault is the queue ID a record names, or the index's storage ID
// for a record from before the field existed: the queue has always lived
// under the index's ID.
func (r vectorIndexRecord) queueIDOrDefault() string {
	if r.QueueID == "" {
		return r.storageID()
	}
	return r.QueueID
}

// adopted is r with cfg recorded as its config, and a missing queue ID
// filled in. changed reports whether the record differs from r.
func (r vectorIndexRecord) adopted(cfg schemaConfig.VectorIndexConfig) (rec vectorIndexRecord, changed bool) {
	rec = r
	rec.QueueID = r.queueIDOrDefault()
	rec.Config = cfg
	return rec, !reflect.DeepEqual(rec, r)
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
	return name
}

// vectorIndexMappingName is the inverse of vectorIndexMappingKey. ok is
// false for a key that names no vector: an empty one, or a dotted one that
// is not the legacy key.
func vectorIndexMappingName(key string) (name string, ok bool) {
	if key == vectorIndexMappingLegacyKey {
		return "", true
	}
	if key == "" || strings.HasPrefix(key, ".") {
		return "", false
	}
	return key, true
}

// validate rejects a record that no version of this code writes.
func (r vectorIndexRecord) validate() error {
	if r.PhysicalID == "" {
		return errors.New("empty physical id")
	}
	err := validateVectorIndexID(r.PhysicalID)
	if err != nil {
		return fmt.Errorf("physical id: %w", err)
	}
	// empty in a record from before the field existed
	if r.QueueID != "" {
		err := validateVectorIndexID(r.QueueID)
		if err != nil {
			return fmt.Errorf("queue id: %w", err)
		}
	}
	if r.IndexType == "" {
		return errors.New("empty index type")
	}
	switch r.State {
	case vectorIndexStateCreating, vectorIndexStateReady, vectorIndexStateDropping:
	default:
		return fmt.Errorf("unknown state %q", r.State)
	}
	// Catches a mismatched write only: a read decodes the config by
	// IndexType, so a mismatch already on disk is not seen here.
	if r.Config != nil && r.Config.IndexType() != r.IndexType {
		return fmt.Errorf("config is for a %s index, the record says %s", r.Config.IndexType(), r.IndexType)
	}
	return nil
}

// Load reads every record, keyed by logical name. initialized is false when
// the namespace has no format version, which is a shard from before the
// mapping existed and the signal for first-load initialization. Anything
// the mapping cannot account for fails the load: an unknown format version,
// a key that names no vector, a record that does not parse or validate.
func (m *vectorIndexMapping) Load() (records map[string]vectorIndexRecord, initialized bool, err error) {
	records, initialized, _, err = m.load()
	return records, initialized, err
}

// LoadAndStrip is Load, then rewrites in one transaction every record whose
// stored value is not what this binary writes, and returns their names. A
// later version adds fields an older reader ignores but keeps on disk; they
// would go stale here and be trusted again after an upgrade. A mapping this
// binary wrote is not written to.
func (m *vectorIndexMapping) LoadAndStrip() (records map[string]vectorIndexRecord, initialized bool, stripped []string, err error) {
	records, initialized, stripped, err = m.load()
	if err != nil || len(stripped) == 0 {
		return records, initialized, nil, err
	}
	err = m.ns.Update(func(b *shardmeta.Batch) error {
		for _, name := range stripped {
			err := putVectorIndexRecord(b, name, records[name])
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
func (m *vectorIndexMapping) load() (records map[string]vectorIndexRecord, initialized bool, foreign []string, err error) {
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

// readVectorIndexRecordOffline reads name's record from a shard that is not
// loaded, for the cold sweep. initialized is false for a shard restored from
// a pre-mapping backup that has not loaded since; such a shard has no
// records at all.
func readVectorIndexRecordOffline(shardDir, name string) (rec vectorIndexRecord, ok, initialized bool, err error) {
	// one open for both keys; an absent key is a nil value
	vals, _, err := shardmeta.GetOfflineMulti(shardDir, vectorIndexMappingNamespace,
		[]byte(vectorIndexMappingFormatVersionKey), []byte(vectorIndexMappingKey(name)))
	if err != nil || vals == nil {
		return vectorIndexRecord{}, false, false, err
	}
	version, v := vals[0], vals[1]
	// the same refusals as Load: a deletion must not act on a record this
	// binary cannot read
	if version == nil {
		if v != nil {
			return vectorIndexRecord{}, false, false, errors.New("records without a format version")
		}
		return vectorIndexRecord{}, false, false, nil
	}
	if string(version) != vectorIndexMappingFormatVersion {
		return vectorIndexRecord{}, false, true, fmt.Errorf("unsupported format version %q, this binary reads %q",
			version, vectorIndexMappingFormatVersion)
	}
	if v == nil {
		return vectorIndexRecord{}, false, true, nil
	}
	err = json.Unmarshal(v, &rec)
	if err == nil {
		err = rec.validate()
	}
	if err != nil {
		return vectorIndexRecord{}, false, true, fmt.Errorf("record %q: %w", name, err)
	}
	return rec, true, true, nil
}

// unloadedVectorIndexStorageIDs maps each of names, vectors of a shard that is
// not loaded, to the ID its files are named by: its record's, or the naming
// rule's for a vector without one. When a load holds the mapping (locked) or
// it cannot be read (err), the IDs are the naming rule's and may be wrong.
func unloadedVectorIndexStorageIDs(shardDir string, names []string) (ids map[string]string, locked bool, err error) {
	namingRule := func() map[string]string {
		ids := make(map[string]string, len(names))
		for _, name := range names {
			ids[name] = vectorIndexID(name)
		}
		return ids
	}
	ids = namingRule()
	keys := make([][]byte, 0, len(names)+1)
	keys = append(keys, []byte(vectorIndexMappingFormatVersionKey))
	for _, name := range names {
		keys = append(keys, []byte(vectorIndexMappingKey(name)))
	}
	vals, _, err := shardmeta.GetOfflineMulti(shardDir, vectorIndexMappingNamespace, keys...)
	if shardmeta.IsLocked(err) {
		return ids, true, nil
	}
	if err != nil {
		return namingRule(), false, fmt.Errorf("read vector index mapping: %w", err)
	}
	if vals == nil {
		return ids, false, nil
	}
	if vals[0] == nil {
		// a load refuses records without a version; with none, the naming
		// rule is what the first load records
		if slices.ContainsFunc(vals[1:], func(v []byte) bool { return v != nil }) {
			return namingRule(), false, errors.New("read vector index mapping: records without a format version")
		}
		return ids, false, nil
	}
	if string(vals[0]) != vectorIndexMappingFormatVersion {
		return namingRule(), false, fmt.Errorf("read vector index mapping: unsupported format version %q, this binary reads %q",
			vals[0], vectorIndexMappingFormatVersion)
	}
	for i, name := range names {
		v := vals[i+1]
		if v == nil {
			continue
		}
		var rec vectorIndexRecord
		err := json.Unmarshal(v, &rec)
		if err == nil {
			err = rec.validate()
		}
		if err != nil {
			return namingRule(), false, fmt.Errorf("read vector index mapping: record %q: %w", name, err)
		}
		ids[name] = rec.storageID()
	}
	return ids, false, nil
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
