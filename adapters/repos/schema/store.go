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
	"fmt"
	"os"
	"path"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
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
	db, err := bolt.Open(filePath, 0o600, nil)
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
		return errors.Wrapf(err, "marshal schema state to json")
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
		return nil, errors.Wrapf(err, "parse schema state from JSON")
	}

	return &state, nil
}

// UpdateClass if it exists, otherwise return an error.
func (r *store) UpdateClass(_ context.Context, data ucs.ClassPayload) error {
	classKey := encodeClassName(data.Name)
	f := func(tx *bolt.Tx) error {
		b := tx.Bucket(schemaBucket).Bucket(classKey)
		if b == nil {
			return fmt.Errorf("class not found")
		}
		return r.updateClass(b, data)
	}
	return r.db.Update(f)
}

// NewClass creates a new class if it doesn't exists, otherwise return an error
func (r *store) NewClass(_ context.Context, data ucs.ClassPayload) error {
	classKey := encodeClassName(data.Name)
	f := func(tx *bolt.Tx) error {
		b, err := tx.Bucket(schemaBucket).CreateBucket(classKey)
		if err != nil {
			return err
		}
		return r.updateClass(b, data)
	}
	return r.db.Update(f)
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

// DeleteClass class
func (r *store) DeleteClass(_ context.Context, class string) error {
	classKey := encodeClassName(class)
	f := func(tx *bolt.Tx) error {
		err := tx.Bucket(schemaBucket).DeleteBucket(classKey)
		if err != nil && !errors.Is(err, bolt.ErrBucketNotFound) {
			return err
		}
		return nil
	}
	return r.db.Update(f)
}

// NewShards add new shards to an existing class
func (r *store) NewShards(_ context.Context, class string, shards []ucs.KeyValuePair) error {
	classKey := encodeClassName(class)
	f := func(tx *bolt.Tx) error {
		b := tx.Bucket(schemaBucket).Bucket(classKey)
		if b == nil {
			return fmt.Errorf("class not found")
		}
		return appendShards(b, shards, make([]byte, 1, 68))
	}
	return r.db.Update(f)
}

// Update shards updates (replaces) shards of existing class
// Error is returned if class or shard does not exist
func (r *store) UpdateShards(_ context.Context, class string, shards []ucs.KeyValuePair) error {
	classKey := encodeClassName(class)
	f := func(tx *bolt.Tx) error {
		b := tx.Bucket(schemaBucket).Bucket(classKey)
		if b == nil {
			return fmt.Errorf("class not found")
		}
		keyBuf := make([]byte, 1, 68)
		if !existShards(b, shards, keyBuf) {
			return fmt.Errorf("shard not found")
		}
		return appendShards(b, shards, keyBuf)
	}
	return r.db.Update(f)
}

// DeleteShards of a specific class
//
//	If the class or a shard does not exist then nothing is done and a nil error is returned
func (r *store) DeleteShards(_ context.Context, class string, shards []string) error {
	classKey := encodeClassName(class)
	f := func(tx *bolt.Tx) error {
		b := tx.Bucket(schemaBucket).Bucket(classKey)
		if b == nil {
			return nil
		}
		return deleteShards(b, shards, make([]byte, 1, 68))
	}
	return r.db.Update(f)
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
	go func() {
		defer close(ch)
		r.db.View(f)
	}()
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

	f := func(tx *bolt.Tx) error {
		root := tx.Bucket(schemaBucket)
		return r.saveAllTx(ctx, root, ss)(tx)
	}
	return r.db.Update(f)
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
			payload, err := ucs.CreateClassPayload(cls, sharding)
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
		}

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

func existShards(b *bolt.Bucket, shards []ucs.KeyValuePair, keyBuf []byte) bool {
	keyBuf[0] = eTypeShard
	for _, pair := range shards {
		kLen := len(pair.Key) + 1
		keyBuf = append(keyBuf, pair.Key...)
		if val := b.Get(keyBuf[:kLen]); val == nil {
			return false
		}
		keyBuf = keyBuf[:1]
	}
	return true
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

func deleteShards(b *bolt.Bucket, shards []string, keyBuf []byte) error {
	keyBuf[0] = eTypeShard
	for _, name := range shards {
		kLen := len(name) + 1
		keyBuf = append(keyBuf, name...)
		if err := b.Delete(keyBuf[:kLen]); err != nil {
			return err
		}
		keyBuf = keyBuf[:1]
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

// var _ = schemauc.Repo(&Repo{})
