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
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/shardmeta"
	entlsmkv "github.com/weaviate/weaviate/entities/lsmkv"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/vectorindex"
)

// manifestRecord and manifestRecords read and write what the manifest keeps in
// index.db without going through it, to plant a state and to check the result.
type manifestRecord struct {
	PhysicalID string                         `json:"physical_id"`
	IndexType  string                         `json:"index_type"`
	State      string                         `json:"state"`
	QueueID    string                         `json:"queue_id,omitempty"`
	Config     schemaConfig.VectorIndexConfig `json:"config,omitempty"`
}

const (
	manifestNamespace        = "vector_index_mapping"
	manifestFormatVersionKey = ".format_version"
)

func (r *manifestRecord) UnmarshalJSON(data []byte) error {
	type plain manifestRecord
	var aux struct {
		plain
		Config json.RawMessage `json:"config"`
	}
	err := json.Unmarshal(data, &aux)
	if err != nil {
		return err
	}
	*r = manifestRecord(aux.plain)
	if aux.Config != nil {
		r.Config, err = vectorindex.UnmarshalConfig(r.IndexType, aux.Config)
	}
	return err
}

// storageID is the ID the index's files are named by.
func (r manifestRecord) storageID() string {
	if r.PhysicalID == "main" || strings.HasPrefix(r.PhysicalID, "vectors_") {
		return r.PhysicalID
	}
	return "vectors_" + r.PhysicalID
}

func (r manifestRecord) queueID() string {
	if r.QueueID == "" {
		return r.storageID()
	}
	return r.QueueID
}

type manifestRecords struct {
	ns *shardmeta.Namespace
}

func newManifestRecords(db *shardmeta.DB) *manifestRecords {
	return &manifestRecords{ns: db.Namespace(manifestNamespace)}
}

func recordsOf(s *Shard) *manifestRecords {
	return newManifestRecords(s.metadataDB)
}

func manifestKey(name string) string {
	if name == "" {
		return ".legacy"
	}
	return name
}

func (m *manifestRecords) Load() (records map[string]manifestRecord, initialized bool, err error) {
	records = map[string]manifestRecord{}
	err = m.ns.ForEach(func(k, v []byte) error {
		name := string(k)
		switch {
		case name == manifestFormatVersionKey:
			initialized = true
			return nil
		case name == ".legacy":
			name = ""
		case strings.HasPrefix(name, "."):
			return errors.New("unknown key " + name)
		}
		var rec manifestRecord
		err := json.Unmarshal(v, &rec)
		records[name] = rec
		return err
	})
	return records, initialized, err
}

func (m *manifestRecords) Get(name string) (rec manifestRecord, ok bool, err error) {
	v, err := m.ns.Get([]byte(manifestKey(name)))
	if err != nil || v == nil {
		return rec, false, err
	}
	err = json.Unmarshal(v, &rec)
	return rec, true, err
}

func (m *manifestRecords) Put(name string, rec manifestRecord) error {
	v, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return m.ns.Put([]byte(manifestKey(name)), v)
}

func (m *manifestRecords) Delete(name string) error {
	return m.ns.Delete([]byte(manifestKey(name)))
}

func (m *manifestRecords) Initialize(records map[string]manifestRecord) error {
	for name, rec := range records {
		err := m.Put(name, rec)
		if err != nil {
			return err
		}
	}
	return m.ns.Put([]byte(manifestFormatVersionKey), []byte("1"))
}

// manifestTask and manifestTasks are manifestRecord and manifestRecords for the
// rebuild tasks.
type manifestTask struct {
	TaskID      string            `json:"task_id"`
	TaskVersion uint64            `json:"task_version"`
	Source      manifestTaskIndex `json:"source"`
	Candidate   manifestTaskIndex `json:"candidate"`
}

type manifestTaskIndex struct {
	PhysicalID string `json:"physical_id"`
	IndexType  string `json:"index_type"`
}

type manifestTasks struct {
	ns *shardmeta.Namespace
}

func newManifestTasks(db *shardmeta.DB) *manifestTasks {
	return &manifestTasks{ns: db.Namespace("vector_index_tasks")}
}

func tasksOf(s *Shard) *manifestTasks {
	return newManifestTasks(s.metadataDB)
}

func (m *manifestTasks) Put(name string, task manifestTask) error {
	v, err := json.Marshal(task)
	if err != nil {
		return err
	}
	err = m.ns.Put([]byte(manifestFormatVersionKey), []byte("1"))
	if err != nil {
		return err
	}
	return m.ns.Put([]byte(manifestKey(name)), v)
}

// Load lists the vectors that have a task.
func (m *manifestTasks) Load() (names []string, err error) {
	err = m.ns.ForEach(func(k, _ []byte) error {
		if string(k) != manifestFormatVersionKey {
			names = append(names, string(k))
		}
		return nil
	})
	return names, err
}

func (m *manifestTasks) Get(name string) (value []byte, ok bool, err error) {
	value, err = m.ns.Get([]byte(manifestKey(name)))
	return value, value != nil, err
}

// recorded is name's record as the shard writes it today: built at
// physicalID with its queue beside it, holding the schema's config.
func recorded(shard *Shard, name, physicalID, state string) manifestRecord {
	cfg := shard.index.GetVectorIndexConfig(name)
	return manifestRecord{PhysicalID: physicalID, IndexType: cfg.IndexType(), State: state, QueueID: physicalID, Config: cfg}
}

// seedGeneratedVectorIndexRecords initializes the mapping of the shard at
// shardDir before its first load, every vector in configs (the legacy one
// under the empty name) recorded creating at a generated physical and queue
// ID, as a rebuild names them. The first load builds each index there.
func seedGeneratedVectorIndexRecords(t *testing.T, shardDir string,
	configs map[string]schemaConfig.VectorIndexConfig,
) map[string]manifestRecord {
	t.Helper()
	require.NoError(t, os.MkdirAll(shardDir, 0o755))
	db, err := shardmeta.Open(shardDir, entlsmkv.BoltFlockTimeout)
	require.NoError(t, err)
	defer db.Close()
	records := make(map[string]manifestRecord, len(configs))
	for name, cfg := range configs {
		if !vectorIndexHasStorage(cfg) {
			continue
		}
		records[name] = manifestRecord{
			PhysicalID: uuid.NewString(),
			IndexType:  cfg.IndexType(),
			State:      "creating",
			QueueID:    uuid.NewString(),
			Config:     cfg,
		}
	}
	require.NoError(t, newManifestRecords(db).Initialize(records))
	return records
}
