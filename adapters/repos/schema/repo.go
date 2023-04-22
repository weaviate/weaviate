//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	schemauc "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
	bolt "go.etcd.io/bbolt"
)

var (
	// monolithic schema
	schemaBucket        = []byte("schema")
	monolithicSchemaKey = []byte("schema")

	schemaBucketV2 = []byte("schema_v2")
)

type Repo struct {
	logger  logrus.FieldLogger
	baseDir string
	db      *bolt.DB
}

func NewRepo(baseDir string, logger logrus.FieldLogger) (*Repo, error) {
	r := &Repo{
		baseDir: baseDir,
		logger:  logger,
	}

	err := r.init()
	return r, err
}

func (r *Repo) DBPath() string {
	return fmt.Sprintf("%s/schema.db", r.baseDir)
}

func (r *Repo) init() error {
	if err := os.MkdirAll(r.baseDir, 0o777); err != nil {
		return errors.Wrapf(err, "create root path directory at %s", r.baseDir)
	}

	boltdb, err := bolt.Open(r.DBPath(), 0o600, nil)
	if err != nil {
		return errors.Wrapf(err, "open bolt at %s", r.DBPath())
	}

	err = boltdb.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(schemaBucket); err != nil {
			return errors.Wrapf(err, "create schema bucket '%s'",
				string(schemaBucket))
		}

		if _, err := tx.CreateBucketIfNotExists(schemaBucketV2); err != nil {
			return errors.Wrapf(err, "create schema bucket '%s'",
				string(schemaBucket))
		}
		return nil
	})
	if err != nil {
		return errors.Wrapf(err, "create bolt buckets")
	}

	r.db = boltdb

	return nil
}

func (r *Repo) SaveSchema(ctx context.Context, schema schemauc.State) error {
	panic("oh oh, looks like someone's still calling an old method")
	// schemaJSON, err := json.Marshal(schema)
	// if err != nil {
	// 	return errors.Wrapf(err, "marshal schema state to json")
	// }

	// return r.db.Update(func(tx *bolt.Tx) error {
	// 	b := tx.Bucket(schemaBucket)
	// 	return b.Put(monolithicSchemaKey, schemaJSON)
	// })
}

type classShardingTuple struct {
	Class    *models.Class   `json:"class"`
	Sharding *sharding.State `json:"sharding"`
}

func keyFromClassName(in string) []byte {
	return []byte(strings.ToLower(in))
}

func (r *Repo) SaveClass(ctx context.Context, c *models.Class,
	shardSt *sharding.State,
) error {
	jsonBytes, err := json.Marshal(classShardingTuple{c, shardSt})
	if err != nil {
		return errors.Wrapf(err, "marshal schema state to json")
	}

	return r.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(schemaBucketV2)
		return b.Put(keyFromClassName(c.Class), jsonBytes)
	})
}

func (r *Repo) LoadAllClasses(ctx context.Context) (*schemauc.State, error) {
	out := &schemauc.State{
		ObjectSchema:  &models.Schema{},
		ShardingState: map[string]*sharding.State{},
	}

	err := r.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(schemaBucketV2)
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			var t classShardingTuple
			if err := json.Unmarshal(v, &t); err != nil {
				return err
			}

			out.ShardingState[t.Class.Class] = t.Sharding
			out.ObjectSchema.Classes = append(out.ObjectSchema.Classes, t.Class)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return out, nil
}

func (r *Repo) LoadSchema(ctx context.Context) (*schemauc.State, error) {
	panic("oh oh, looks like someone's still calling an old method")
	// var schemaJSON []byte
	// r.db.View(func(tx *bolt.Tx) error {
	// 	b := tx.Bucket(schemaBucket)
	// 	schemaJSON = b.Get(monolithicSchemaKey)
	// 	return nil
	// })

	// if len(schemaJSON) == 0 {
	// 	return nil, nil
	// }

	// var state schemauc.State
	// err := json.Unmarshal(schemaJSON, &state)
	// if err != nil {
	// 	return nil, errors.Wrapf(err, "parse schema state from JSON")
	// }

	// return &state, nil
}

var _ = schemauc.Repo(&Repo{})
