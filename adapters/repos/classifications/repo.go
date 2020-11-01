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

package classifications

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/boltdb/bolt"
	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/classification"
	"github.com/sirupsen/logrus"
)

var classificationsBucket = []byte("classifications")

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
	return fmt.Sprintf("%s/classifications.db", r.baseDir)
}

func (r *Repo) keyFromID(id strfmt.UUID) []byte {
	return []byte(id)
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
		if _, err := tx.CreateBucketIfNotExists(classificationsBucket); err != nil {
			return errors.Wrapf(err, "create classifications bucket '%s'",
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

func (r *Repo) Put(ctx context.Context, classification models.Classification) error {
	classificationJSON, err := json.Marshal(classification)
	if err != nil {
		return errors.Wrap(err, "marshal classification to JSON")
	}

	return r.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(classificationsBucket)
		return b.Put(r.keyFromID(classification.ID), classificationJSON)
	})
}

func (r *Repo) Get(ctx context.Context, id strfmt.UUID) (*models.Classification, error) {
	var classificationJSON []byte
	r.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(classificationsBucket)
		classificationJSON = b.Get(r.keyFromID(id))
		return nil
	})

	if len(classificationJSON) == 0 {
		return nil, nil
	}

	var c models.Classification
	err := json.Unmarshal(classificationJSON, &c)
	if err != nil {
		return nil, errors.Wrapf(err, "parse classification from JSON")
	}

	return &c, nil
}

var _ = classification.Repo(&Repo{})
