//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	schemauc "github.com/semi-technologies/weaviate/usecases/schema"
	"github.com/sirupsen/logrus"
)

var schemaBucket = []byte("schema")
var schemaKey = []byte("schema")

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
	if err := os.MkdirAll(r.baseDir, 0777); err != nil {
		return errors.Wrapf(err, "create root path directory at %s", r.baseDir)
	}

	boltdb, err := bolt.Open(r.DBPath(), 0600, nil)
	if err != nil {
		return errors.Wrapf(err, "open bolt at %s", r.DBPath())
	}

	err = boltdb.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(schemaBucket); err != nil {
			return errors.Wrapf(err, "create schema bucket '%s'",
				string(helpers.ObjectsBucket))
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
	schemaJSON, err := json.Marshal(schema)
	if err != nil {
		return errors.Wrapf(err, "marshal schema state to json")
	}

	return r.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(schemaBucket)
		return b.Put(schemaKey, schemaJSON)
	})
}

func (r *Repo) LoadSchema(ctx context.Context) (*schemauc.State, error) {
	var schemaJSON []byte
	r.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(schemaBucket)
		schemaJSON = b.Get(schemaKey)
		return nil
	})

	if len(schemaJSON) == 0 {
		return nil, nil
	}

	var state schemauc.State
	err := json.Unmarshal(schemaJSON, &state)
	if err != nil {
		return nil, errors.Wrapf(err, "parse schema state from JSON")
	}

	return &state, nil
}

var _ = schemauc.Repo(&Repo{})
