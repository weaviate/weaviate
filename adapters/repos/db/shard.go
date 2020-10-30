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

package db

import (
	"context"
	"fmt"
	"time"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/indexcounter"
	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
)

// Shard is the smallest completely-contained index unit. A shard mananages
// database files for all the objects it owns. How a shard is determined for a
// target object (e.g. Murmur hash, etc.) is still open at this point
type Shard struct {
	index            *Index // a reference to the underlying index, which in turn contains schema information
	name             string
	db               *bolt.DB // one db file per shard, uses buckets for separation between data storage, index storage, etc.
	counter          *indexcounter.Counter
	vectorIndex      VectorIndex
	invertedRowCache *inverted.RowCacher
	metrics          *Metrics
	propertyIndices  map[string]PropertyIndex
}

func NewShard(shardName string, index *Index) (*Shard, error) {
	s := &Shard{
		index:            index,
		name:             shardName,
		invertedRowCache: inverted.NewRowCacher(50 * 1024 * 1024),
		metrics:          NewMetrics(index.logger),
	}

	vi, err := hnsw.New(hnsw.Config{
		Logger:   index.logger,
		RootPath: s.index.Config.RootPath,
		ID:       s.ID(),
		MakeCommitLoggerThunk: func() (hnsw.CommitLogger, error) {
			return hnsw.NewCommitLogger(s.index.Config.RootPath, s.ID(), 10*time.Second,
				index.logger)
		},
		MaximumConnections:       60,
		EFConstruction:           128,
		VectorForIDThunk:         s.vectorByIndexID,
		TombstoneCleanupInterval: 5 * time.Minute,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "init shard %q: hnsw index", s.ID())
	}
	s.vectorIndex = vi

	err = s.initDBFile()
	if err != nil {
		return nil, errors.Wrapf(err, "init shard %q: shard db", s.ID())
	}

	counter, err := indexcounter.New(s.ID(), index.Config.RootPath)
	if err != nil {
		return nil, errors.Wrapf(err, "init shard %q: index counter", s.ID())
	}

	s.counter = counter

	if err := s.initPerPropertyIndices(); err != nil {
		return nil, errors.Wrapf(err, "init shard %q: init per property indices", s.ID())
	}

	return s, nil
}

func (s *Shard) ID() string {
	return fmt.Sprintf("%s_%s", s.index.ID(), s.name)
}

func (s *Shard) DBPath() string {
	return fmt.Sprintf("%s/%s.db", s.index.Config.RootPath, s.ID())
}

func (s *Shard) initDBFile() error {
	boltdb, err := bolt.Open(s.DBPath(), 0o600, nil)
	if err != nil {
		return errors.Wrapf(err, "open bolt at %s", s.DBPath())
	}

	err = boltdb.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(helpers.ObjectsBucket); err != nil {
			return errors.Wrapf(err, "create objects bucket '%s'", string(helpers.ObjectsBucket))
		}

		if _, err := tx.CreateBucketIfNotExists(helpers.IndexIDBucket); err != nil {
			return errors.Wrapf(err, "create indexID bucket '%s'", string(helpers.IndexIDBucket))
		}

		return nil
	})
	if err != nil {
		return errors.Wrapf(err, "create bolt buckets")
	}

	s.db = boltdb
	return nil
}

func (s *Shard) addProperty(ctx context.Context, prop *models.Property) error {
	if err := s.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(helpers.BucketFromPropName(prop.Name))
		if err != nil {
			return err
		}

		if schema.IsRefDataType(prop.DataType) {
			_, err := tx.CreateBucketIfNotExists(helpers.BucketFromPropName(helpers.MetaCountProp(prop.Name)))
			if err != nil {
				return err
			}
		}

		if schema.DataType(prop.DataType[0]) == schema.DataTypeGeoCoordinates {
			if err := s.initGeoProp(prop); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "bolt update tx")
	}

	return nil
}
