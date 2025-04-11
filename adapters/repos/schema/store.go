//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package schema

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/types"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	ucs "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
	bolt "go.etcd.io/bbolt"
)

var (
	// old keys are still needed for migration
	schemaBucket = []byte("schema")
	schemaKey    = []byte("schema")
	// static keys
	keyMetaClass         = []byte{eTypeMeta, 0}
	keyShardingState     = []byte{eTypeSharingState, 0}
	keyConfig            = []byte{eTypeConfig, 0}
	_Version         int = 2
)

// constant to encode the type of entry in the DB
const (
	eTypeConfig       byte = 1
	eTypeClass        byte = 2
	eTypeShard        byte = 4
	eTypeMeta         byte = 5
	eTypeSharingState byte = 15
)

const (
	// BoltDBTimeout is the timeout for acquiring file lock when opening BoltDB
	BoltDBTimeout = 5 * time.Second
)

// config configuration specific the stored schema
type config struct {
	Version int
	// add more fields
}

/*
Store is responsible for storing and persisting the schema in a structured manner.
It ensures that each class has a dedicated bucket, which includes metadata, and sharding state.

Schema Structure:
  - Config: contains metadata related to parsing the schema
  - Nested buckets for each class

Schema Structure for a class Bucket:
  - Metadata contains models.Class
  - Sharding state without shards
  - Class shards: individual shard associated with the sharding state

By organizing the schema in this manner, it facilitates efficient management of class specific data during runtime.
In addition, old schema are backed up and migrated to the new structure for a seamless transitions
*/
type store struct {
	version int    // schema version
	homeDir string // home directory of schema files
	log     logrus.FieldLogger
	db      *bolt.DB
}

// NewStore returns a new schema repository. Call the Open() method to open the underlying DB.
// To free the resources, call the Close() method.
func NewStore(homeDir string, logger logrus.FieldLogger) *store {
	return &store{
		version: _Version,
		homeDir: homeDir,
		log:     logger,
	}
}

func initBoltDB(filePath string, version int, cfg *config) (*bolt.DB, error) {
	db, err := bolt.Open(filePath, 0o600, &bolt.Options{Timeout: BoltDBTimeout})
	if err != nil {
		return nil, fmt.Errorf("open %q: %w", filePath, err)
	}
	root := func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket(schemaBucket)
		// A new bucket has been created
		if err == nil {
			*cfg = config{Version: version}
			return saveConfig(b, *cfg)
		}
		// load existing bucket
		b = tx.Bucket(schemaBucket)
		if b == nil {
			return fmt.Errorf("retrieve existing bucket %q", schemaBucket)
		}
		// read config:  config exists since version 2
		data := b.Get(keyConfig)
		if len(data) > 0 {
			if err := json.Unmarshal(data, &cfg); err != nil {
				return fmt.Errorf("cannot read config: %w", err)
			}
		}
		return nil
	}

	return db, db.Update(root)
}

// Open the underlying DB
// Deprecated: instead schema now is persistent via RAFT
// see : cluster package
// Load and save are left to support backward compatibility
func (r *store) Open() (err error) {
	if err := os.MkdirAll(r.homeDir, 0o777); err != nil {
		return fmt.Errorf("create root directory %q: %w", r.homeDir, err)
	}
	cfg := config{}
	path := path.Join(r.homeDir, "schema.db")
	boltDB, err := initBoltDB(path, r.version, &cfg)
	if err != nil {
		return fmt.Errorf("init bolt_db: %w", err)
	}
	defer func() {
		if err != nil {
			boltDB.Close()
		}
	}()
	r.db = boltDB
	if cfg.Version < r.version {
		if err := r.migrate(path, cfg.Version, r.version); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	if cfg.Version > r.version {
		return fmt.Errorf("schema version %d higher than %d", cfg.Version, r.version)
	}
	return err
}

// Close the underlying DB
func (r *store) Close() {
	r.db.Close()
}

// migrate from old to new schema
// It will back up the old schema file if it exists
func (r *store) migrate(filePath string, from, to int) (err error) {
	r.log.Infof("schema migration from v%d to v%d process has started", from, to)
	defer func() {
		if err == nil {
			r.log.Infof("successfully completed schema migration from v%d to v%d", from, to)
		}
	}()
	state, err := r.loadSchemaV1()
	if err != nil {
		return fmt.Errorf("load old schema: %w", err)
	}
	if state != nil {
		// create backupPath by copying file
		backupPath := fmt.Sprintf("%s_v%d.bak", filePath, from)
		if err := copyFile(backupPath, filePath); err != nil {
			return fmt.Errorf("schema backup: %w", err)
		}

		// write new schema
		f := func(tx *bolt.Tx) error {
			b := tx.Bucket(schemaBucket)
			if err := saveConfig(b, config{Version: to}); err != nil {
				return err
			}
			b.Delete(schemaKey) // remove old schema
			return r.saveAllTx(context.Background(), b, *state)(tx)
		}
		if err := r.db.Update(f); err != nil {
			os.Remove(backupPath)
			return fmt.Errorf("convert to new schema: %w", err)
		}
	}
	return nil
}

// saveSchemaV1 might be needed to migrate from v2 to v0
func (r *store) saveSchemaV1(schema ucs.State) error {
	schemaJSON, err := json.Marshal(schema)
	if err != nil {
		return fmt.Errorf("marshal schema state to json: %w", err)
	}

	return r.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(schemaBucket)
		return b.Put(schemaKey, schemaJSON)
	})
}

// loadSchemaV1 is needed to migrate from v0 to v2
func (r *store) loadSchemaV1() (*ucs.State, error) {
	var schemaJSON []byte
	r.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(schemaBucket)
		schemaJSON = b.Get(schemaKey)
		return nil
	})

	if len(schemaJSON) == 0 {
		return nil, nil
	}

	var state ucs.State
	err := json.Unmarshal(schemaJSON, &state)
	if err != nil {
		return nil, fmt.Errorf("parse schema state from JSON: %w", err)
	}

	return &state, nil
}

func (r *store) updateClass(b *bolt.Bucket, data ucs.ClassPayload) error {
	// remove old shards
	if data.ReplaceShards {
		cursor := b.Cursor() // b.Put before
		for key, _ := cursor.First(); key != nil; {
			if key[0] == eTypeShard {
				b.Delete(key)
			}
			key, _ = cursor.Next()
		}
	}
	if data.Metadata != nil {
		if err := b.Put(keyMetaClass, data.Metadata); err != nil {
			return err
		}
	}

	if data.ShardingState != nil {
		if err := b.Put(keyShardingState, data.ShardingState); err != nil {
			return err
		}
	}

	return appendShards(b, data.Shards, make([]byte, 1, 68))
}

// Load loads the complete schema from the persistent storage
func (r *store) Load(ctx context.Context) (ucs.State, error) {
	state := ucs.NewState(32)
	for data := range r.load(ctx) {
		if data.Error != nil {
			return state, data.Error
		}
		cls := models.Class{Class: string(data.Name)}
		ss := sharding.State{}

		if err := json.Unmarshal(data.Metadata, &cls); err != nil {
			return state, fmt.Errorf("unmarshal class %q", cls.Class)
		}
		if err := json.Unmarshal(data.ShardingState, &ss); err != nil {
			return state, fmt.Errorf("unmarshal sharding state for class %q size %d",
				cls.Class, len(data.ShardingState))
		}
		if n := len(data.Shards); n > 0 {
			ss.Physical = make(map[string]sharding.Physical, n)
		}
		for _, shard := range data.Shards {
			phy := sharding.Physical{}
			name := string(shard.Key)
			if err := json.Unmarshal(shard.Value, &phy); err != nil {
				return state, fmt.Errorf("unmarshal shard %q for class %q", name, cls.Class)
			}
			ss.Physical[name] = phy
		}
		state.ObjectSchema.Classes = append(state.ObjectSchema.Classes, &cls)
		state.ShardingState[cls.Class] = &ss
	}
	return state, nil
}

func (r *store) load(ctx context.Context) <-chan ucs.ClassPayload {
	ch := make(chan ucs.ClassPayload, 1)
	f := func(tx *bolt.Tx) (err error) {
		root := tx.Bucket(schemaBucket)
		rootCursor := root.Cursor()
		for cls, _ := rootCursor.First(); cls != nil; {
			if cls[0] != eTypeClass {
				cls, _ = rootCursor.Next()
				continue
			}
			if err := ctx.Err(); err != nil {
				ch <- ucs.ClassPayload{Error: err}
				return err
			}
			b := root.Bucket(cls)
			if b == nil {
				err := fmt.Errorf("class not found")
				ch <- ucs.ClassPayload{Error: err}
				return err
			}
			x := ucs.ClassPayload{
				Name:   string(cls[1:]),
				Shards: make([]ucs.KeyValuePair, 0, 32),
			}
			cursor := b.Cursor()
			for key, value := cursor.First(); key != nil; {
				if bytes.Equal(key, keyMetaClass) {
					x.Metadata = value
				} else if bytes.Equal(key, keyShardingState) {
					x.ShardingState = value
				} else {
					x.Shards = append(x.Shards, ucs.KeyValuePair{Key: string(key[1:]), Value: value})
				}
				key, value = cursor.Next()
			}
			ch <- x
			cls, _ = rootCursor.Next()
		}
		return nil
	}
	enterrors.GoWrapper(func() {
		defer close(ch)
		r.db.View(f)
	}, r.log)
	return ch
}

// Save saves the complete schema to the persistent storage
func (r *store) Save(ctx context.Context, ss ucs.State) error {
	if (ss.ObjectSchema == nil || len(ss.ObjectSchema.Classes) == 0) &&
		len(ss.ShardingState) == 0 {
		return nil // empty schema nothing to store
	}

	if ss.ObjectSchema == nil ||
		len(ss.ObjectSchema.Classes) == 0 ||
		len(ss.ShardingState) == 0 {
		return fmt.Errorf("inconsistent schema: missing required fields")
	}

	currState, err := r.Load(ctx)
	if err != nil {
		return fmt.Errorf("load existing schema state: %w", err)
	}
	// If the store already contains the same contents as the incoming
	// schema state, we don't need to delete and re-put all schema contents.
	// Doing so can cause a very high MTTR when the number of tenants is on
	// the order of 100k+.
	//
	// Here we have to check equality with rough equivalency checks, because
	// there is currently no way to make a comparison at the byte-level
	//
	// See: https://github.com/weaviate/weaviate/issues/4634
	if currState.EqualEnough(&ss) {
		return nil
	}

	r.log.WithField("action", "save_schema").
		Infof("Current schema state outdated, updating schema store")

	f := func(tx *bolt.Tx) error {
		root := tx.Bucket(schemaBucket)
		return r.saveAllTx(ctx, root, ss)(tx)
	}

	err = r.db.Update(f)
	if err == nil {
		r.log.WithField("action", "save_schema").
			Infof("Schema store successfully updated")
	}

	return err
}

func (r *store) saveAllTx(ctx context.Context, root *bolt.Bucket, ss ucs.State) func(tx *bolt.Tx) error {
	return func(tx *bolt.Tx) error {
		rootCursor := root.Cursor()
		for cls, _ := rootCursor.First(); cls != nil; {
			if cls[0] == eTypeClass {
				err := root.DeleteBucket(cls)
				if err != nil && !errors.Is(err, bolt.ErrBucketNotFound) {
					return err
				}
			}
			cls, _ = rootCursor.Next()
		}
		for _, cls := range ss.ObjectSchema.Classes {
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("context for class %q: %w", cls.Class, err)
			}
			sharding := ss.ShardingState[cls.Class]
			payload, err := createClassPayload(cls, sharding)
			if err != nil {
				return fmt.Errorf("create payload for class %q: %w", cls.Class, err)
			}
			b, err := root.CreateBucket(encodeClassName(cls.Class))
			if err != nil {
				return fmt.Errorf("create bucket for class %q: %w", cls.Class, err)
			}
			if err := r.updateClass(b, payload); err != nil {
				return fmt.Errorf("update bucket %q: %w", cls.Class, err)
			}
			r.log.WithField("action", "update_schema_store").
				Debugf("Class updated: %s", cls.Class)
		}

		r.log.WithField("action", "update_schema_store").
			Debug("All classes updated")

		return nil
	}
}

func saveConfig(root *bolt.Bucket, cfg config) error {
	data, err := json.Marshal(&cfg)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	if err := root.Put(keyConfig, data); err != nil {
		return fmt.Errorf("write config: %w", err)
	}
	return nil
}

func appendShards(b *bolt.Bucket, shards []ucs.KeyValuePair, key []byte) error {
	key[0] = eTypeShard
	for _, pair := range shards {
		kLen := len(pair.Key) + 1
		key = append(key, pair.Key...)
		if err := b.Put(key[:kLen], pair.Value); err != nil {
			return err
		}
		key = key[:1]
	}
	return nil
}

func encodeClassName(name string) []byte {
	len := len(name) + 1
	buf := make([]byte, 1, len)
	buf[0] = eTypeClass
	buf = append(buf, name...)
	return buf[:len]
}

func copyFile(dst, src string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0o644)
}

func createClassPayload(class *models.Class,
	shardingState *sharding.State,
) (pl ucs.ClassPayload, err error) {
	pl.Name = class.Class
	if pl.Metadata, err = json.Marshal(class); err != nil {
		return pl, fmt.Errorf("marshal class %q metadata: %w", pl.Name, err)
	}
	if shardingState != nil {
		ss := *shardingState
		pl.Shards = make([]ucs.KeyValuePair, len(ss.Physical))
		i := 0
		for name, shard := range ss.Physical {
			data, err := json.Marshal(shard)
			if err != nil {
				return pl, fmt.Errorf("marshal shard %q metadata: %w", name, err)
			}
			pl.Shards[i] = ucs.KeyValuePair{Key: name, Value: data}
			i++
		}
		ss.Physical = nil
		if pl.ShardingState, err = json.Marshal(&ss); err != nil {
			return pl, fmt.Errorf("marshal class %q sharding state: %w", pl.Name, err)
		}
	}
	return pl, nil
}

func (r *store) LoadLegacySchema() (map[string]types.ClassState, error) {
	res := make(map[string]types.ClassState)
	legacySchema, err := r.Load(context.Background())
	if err != nil {
		return res, fmt.Errorf("could not load legacy schema: %w", err)
	}
	for _, c := range legacySchema.ObjectSchema.Classes {
		res[c.Class] = types.ClassState{Class: *c, Shards: *legacySchema.ShardingState[c.Class]}
	}
	return res, nil
}

func (r *store) SaveLegacySchema(cluster map[string]types.ClassState) error {
	states := ucs.NewState(len(cluster))

	for _, s := range cluster {
		currState := s // new var to avoid passing pointer to s
		states.ObjectSchema.Classes = append(states.ObjectSchema.Classes, &currState.Class)
		states.ShardingState[s.Class.Class] = &currState.Shards
	}

	return r.Save(context.Background(), states)
}
