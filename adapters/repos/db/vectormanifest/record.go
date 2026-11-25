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
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
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
	namespace        = "vector_index_mapping"
	formatVersionKey = ".format_version"
	formatVersion    = "1"
	legacyKey        = ".legacy"
)

// Record states. There is no dropped state: the schema carries deletion
// intent, and a record outlives it until cleanup completes.
const (
	stateCreating = "creating"
	stateReady    = "ready"
	// dropping: the slot is gone, the files are not yet; the next load finishes it
	stateDropping = "dropping"
)

var (
	defaultIDPattern   = regexp.MustCompile(`^` + helpers.VectorsBucketLSM + `_` + schema.TargetVectorNameRegex + `$`)
	generatedIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]*-[A-Za-z0-9_-]*$`)
)

// maxFileName is what file systems allow.
const maxFileName = 255

// record is what the shard persists about one logical vector:
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
type record struct {
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
func (r *record) UnmarshalJSON(data []byte) error {
	type plain record
	var aux struct {
		plain
		Config json.RawMessage `json:"config"`
	}
	err := json.Unmarshal(data, &aux)
	if err != nil {
		return err
	}
	*r = record(aux.plain)
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
func (r record) storageID() string {
	return storageIDFor(r.PhysicalID)
}

// recordedID is the physical ID as the record holds it, for messages only:
// files are named by storageID.
func (r record) recordedID() string {
	return r.PhysicalID
}

// storageIDFor is the naming rule from a physical ID to the ID its
// files are named by. "main" and "vectors_<x>" keep their names; any other ID
// is named as if it were "vectors_<id>". The index implementations treat an
// ID without the "vectors_" prefix as the legacy vector's, so an ID that
// skipped this rule would name the legacy vector's files.
func storageIDFor(physicalID string) string {
	if physicalID == helpers.VectorIndexIDForTarget("") || strings.HasPrefix(physicalID, helpers.VectorsBucketLSM+"_") {
		return physicalID
	}
	return helpers.VectorsBucketLSM + "_" + physicalID
}

// validateID accepts the IDs the naming rule gives a vector name,
// "main" and "vectors_<name>", and generated IDs, which contain a "-" so they
// can never equal one of those: a vector name has no "-". The schema's
// collision check reads names only and relies on it.
func validateID(id string) error {
	if id == helpers.VectorIndexIDForTarget("") || defaultIDPattern.MatchString(id) {
		return nil
	}
	if generatedIDPattern.MatchString(id) {
		// the longest name an index creates
		name := helpers.HNSWCommitLogDirNameForID(helpers.CentroidsID(storageIDFor(id)))
		if len(name) > maxFileName {
			return fmt.Errorf("%q is too long: it would name a file of %d characters, the limit is %d", id, len(name), maxFileName)
		}
		return nil
	}
	return fmt.Errorf("%q is neither %q, \"vectors_<vector name>\", nor an ID with a \"-\" made of letters, digits, \"_\" and \"-\"",
		id, helpers.VectorIndexIDForTarget(""))
}

// queueIDOrDefault is the queue ID a record names, or the index's storage ID
// for a record from before the field existed: the queue has always lived
// under the index's ID.
func (r record) queueIDOrDefault() string {
	if r.QueueID == "" {
		return r.storageID()
	}
	return r.QueueID
}

// adopted is r with cfg recorded as its config, and a missing queue ID
// filled in. changed reports whether the record differs from r.
func (r record) adopted(cfg schemaConfig.VectorIndexConfig) (rec record, changed bool) {
	rec = r
	rec.QueueID = r.queueIDOrDefault()
	rec.Config = cfg
	return rec, !reflect.DeepEqual(rec, r)
}

// validate rejects a record that no version of this code writes.
func (r record) validate() error {
	if r.PhysicalID == "" {
		return errors.New("empty physical id")
	}
	err := validateID(r.PhysicalID)
	if err != nil {
		return fmt.Errorf("physical id: %w", err)
	}
	// empty in a record from before the field existed
	if r.QueueID != "" {
		err := validateID(r.QueueID)
		if err != nil {
			return fmt.Errorf("queue id: %w", err)
		}
	}
	if r.IndexType == "" {
		return errors.New("empty index type")
	}
	switch r.State {
	case stateCreating, stateReady, stateDropping:
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

// recordKey is the on-disk key of a logical vector name.
func recordKey(name string) string {
	if name == "" {
		return legacyKey
	}
	return name
}

// vectorName is the inverse of recordKey. ok is
// false for a key that names no vector: an empty one, or a dotted one that
// is not the legacy key.
func vectorName(key string) (name string, ok bool) {
	if key == legacyKey {
		return "", true
	}
	if key == "" || strings.HasPrefix(key, ".") {
		return "", false
	}
	return key, true
}

// recordFor is the record of a vector created under the naming rule: its queue
// shares the index's ID, and cfg is what it is built with.
func recordFor(name string, cfg schemaConfig.VectorIndexConfig, state string) record {
	id := helpers.VectorIndexIDForTarget(name)
	return record{PhysicalID: id, IndexType: cfg.IndexType(), State: state, QueueID: id, Config: cfg}
}
