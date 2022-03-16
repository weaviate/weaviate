//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package db

import (
	"context"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/docid"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/indexcounter"
	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted"
	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv"
	"github.com/semi-technologies/weaviate/adapters/repos/db/propertyspecific"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/noop"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/storagestate"
	"github.com/sirupsen/logrus"
)

// Shard is the smallest completely-contained index unit. A shard manages
// database files for all the objects it owns. How a shard is determined for a
// target object (e.g. Murmur hash, etc.) is still open at this point
type Shard struct {
	index            *Index // a reference to the underlying index, which in turn contains schema information
	name             string
	store            *lsmkv.Store
	counter          *indexcounter.Counter
	vectorIndex      VectorIndex
	invertedRowCache *inverted.RowCacher
	metrics          *Metrics
	propertyIndices  propertyspecific.Indices
	deletedDocIDs    *docid.InMemDeletedTracker
	cleanupInterval  time.Duration
	cleanupCancel    chan struct{}
	propLengths      *inverted.PropertyLengthTracker
	randomSource     *bufferedRandomGen
	versioner        *shardVersioner

	status     storagestate.Status
	statusLock sync.Mutex
}

func NewShard(ctx context.Context, shardName string, index *Index) (*Shard, error) {
	rand, err := newBufferedRandomGen(64 * 1024)
	if err != nil {
		return nil, errors.Wrap(err, "init bufferend random generator")
	}

	s := &Shard{
		index:            index,
		name:             shardName,
		invertedRowCache: inverted.NewRowCacher(500 * 1024 * 1024),
		metrics:          NewMetrics(index.logger),
		deletedDocIDs:    docid.NewInMemDeletedTracker(),
		cleanupInterval: time.Duration(index.invertedIndexConfig.
			CleanupIntervalSeconds) * time.Second,
		cleanupCancel: make(chan struct{}),
		randomSource:  rand,
	}

	hnswUserConfig, ok := index.vectorIndexUserConfig.(hnsw.UserConfig)
	if !ok {
		return nil, errors.Errorf("hnsw vector index: config is not hnsw.UserConfig: %T",
			index.vectorIndexUserConfig)
	}

	if hnswUserConfig.Skip {
		s.vectorIndex = noop.NewIndex()
	} else {
		vi, err := hnsw.New(hnsw.Config{
			Logger:   index.logger,
			RootPath: s.index.Config.RootPath,
			ID:       s.ID(),
			MakeCommitLoggerThunk: func() (hnsw.CommitLogger, error) {
				return hnsw.NewCommitLogger(s.index.Config.RootPath, s.ID(), 10*time.Second,
					index.logger)
			},
			VectorForIDThunk: s.vectorByIndexID,
			DistanceProvider: distancer.NewDotProductProvider(),
		}, hnswUserConfig)
		if err != nil {
			return nil, errors.Wrapf(err, "init shard %q: hnsw index", s.ID())
		}
		s.vectorIndex = vi

		defer vi.PostStartup()
	}

	err = s.initDBFile(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "init shard %q: shard db", s.ID())
	}

	counter, err := indexcounter.New(s.ID(), index.Config.RootPath)
	if err != nil {
		return nil, errors.Wrapf(err, "init shard %q: index counter", s.ID())
	}
	s.counter = counter

	dataPresent := s.counter.PreviewNext() != 0
	versionPath := path.Join(index.Config.RootPath, s.ID()+".version")
	versioner, err := newShardVersioner(versionPath, dataPresent)
	if err != nil {
		return nil, errors.Wrapf(err, "init shard %q: check versions", s.ID())
	}
	s.versioner = versioner

	plPath := path.Join(index.Config.RootPath, s.ID()+".proplengths")
	propLengths, err := inverted.NewPropertyLengthTracker(plPath)
	if err != nil {
		return nil, errors.Wrapf(err, "init shard %q: prop length tracker", s.ID())
	}
	s.propLengths = propLengths

	if err := s.initProperties(); err != nil {
		return nil, errors.Wrapf(err, "init shard %q: init per property indices", s.ID())
	}

	return s, nil
}

func (s *Shard) ID() string {
	return fmt.Sprintf("%s_%s", s.index.ID(), s.name)
}

func (s *Shard) DBPathLSM() string {
	return fmt.Sprintf("%s/%s_lsm", s.index.Config.RootPath, s.ID())
}

func (s *Shard) initDBFile(ctx context.Context) error {
	annotatedLogger := s.index.logger.WithFields(logrus.Fields{
		"shard": s.name,
		"index": s.index.ID(),
		"class": s.index.Config.ClassName,
	})
	store, err := lsmkv.New(s.DBPathLSM(), annotatedLogger)
	if err != nil {
		return errors.Wrapf(err, "init lsmkv store at %s", s.DBPathLSM())
	}

	err = store.CreateOrLoadBucket(ctx, helpers.ObjectsBucketLSM,
		lsmkv.WithStrategy(lsmkv.StrategyReplace),
		lsmkv.WithSecondaryIndicies(1))
	if err != nil {
		return errors.Wrap(err, "create objects bucket")
	}

	s.store = store

	return nil
}

func (s *Shard) drop() error {
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}

	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()

	if err := s.store.Shutdown(ctx); err != nil {
		return errors.Wrap(err, "stop lsmkv store")
	}

	if _, err := os.Stat(s.DBPathLSM()); err == nil {
		err := os.RemoveAll(s.DBPathLSM())
		if err != nil {
			return errors.Wrapf(err, "remove lsm store at %s", s.DBPathLSM())
		}
	}
	// delete indexcount
	err := s.counter.Drop()
	if err != nil {
		return errors.Wrapf(err, "remove indexcount at %s", s.DBPathLSM())
	}

	// delete indexcount
	err = s.versioner.Drop()
	if err != nil {
		return errors.Wrapf(err, "remove indexcount at %s", s.DBPathLSM())
	}
	// remove vector index
	err = s.vectorIndex.Drop()
	if err != nil {
		return errors.Wrapf(err, "remove vector index at %s", s.DBPathLSM())
	}

	// delete indexcount
	err = s.propLengths.Drop()
	if err != nil {
		return errors.Wrapf(err, "remove prop length tracker at %s", s.DBPathLSM())
	}

	// TODO: can we remove this?
	s.deletedDocIDs.BulkRemove(s.deletedDocIDs.GetAll())

	err = s.propertyIndices.DropAll()
	if err != nil {
		return errors.Wrapf(err, "remove property specific indices at %s", s.DBPathLSM())
	}

	return nil
}

func (s *Shard) addIDProperty(ctx context.Context) error {
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}

	err := s.store.CreateOrLoadBucket(ctx,
		helpers.BucketFromPropNameLSM(helpers.PropertyNameID),
		lsmkv.WithStrategy(lsmkv.StrategySetCollection))
	if err != nil {
		return err
	}

	err = s.store.CreateOrLoadBucket(ctx,
		helpers.HashBucketFromPropNameLSM(helpers.PropertyNameID),
		lsmkv.WithStrategy(lsmkv.StrategyReplace))
	if err != nil {
		return err
	}

	return nil
}

func (s *Shard) addProperty(ctx context.Context, prop *models.Property) error {
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}

	if schema.IsRefDataType(prop.DataType) {
		err := s.store.CreateOrLoadBucket(ctx,
			helpers.BucketFromPropNameLSM(helpers.MetaCountProp(prop.Name)),
			lsmkv.WithStrategy(lsmkv.StrategySetCollection)) // ref props do not have frequencies -> Set
		if err != nil {
			return err
		}

		err = s.store.CreateOrLoadBucket(ctx,
			helpers.HashBucketFromPropNameLSM(helpers.MetaCountProp(prop.Name)),
			lsmkv.WithStrategy(lsmkv.StrategyReplace))
		if err != nil {
			return err
		}
	}

	if schema.DataType(prop.DataType[0]) == schema.DataTypeGeoCoordinates {
		return s.initGeoProp(prop)
	}

	var mapOpts []lsmkv.BucketOption
	if inverted.HasFrequency(schema.DataType(prop.DataType[0])) {
		mapOpts = append(mapOpts, lsmkv.WithStrategy(lsmkv.StrategyMapCollection))
		if s.versioner.Version() < 2 {
			mapOpts = append(mapOpts, lsmkv.WithLegacyMapSorting())
		}
	} else {
		mapOpts = append(mapOpts, lsmkv.WithStrategy(lsmkv.StrategySetCollection))
	}

	err := s.store.CreateOrLoadBucket(ctx, helpers.BucketFromPropNameLSM(prop.Name),
		mapOpts...)
	if err != nil {
		return err
	}

	err = s.store.CreateOrLoadBucket(ctx, helpers.HashBucketFromPropNameLSM(prop.Name),
		lsmkv.WithStrategy(lsmkv.StrategyReplace))
	if err != nil {
		return err
	}

	return nil
}

func (s *Shard) updateVectorIndexConfig(ctx context.Context,
	updated schema.VectorIndexConfig) error {
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}

	return s.vectorIndex.UpdateUserConfig(updated)
}

func (s *Shard) shutdown(ctx context.Context) error {
	if err := s.propLengths.Close(); err != nil {
		return errors.Wrap(err, "close prop length tracker")
	}

	return s.store.Shutdown(ctx)
}

func (s *Shard) notifyReady() {
	s.initStatus()
	s.index.logger.
		WithField("action", "startup").
		Debugf("shard=%s is ready", s.name)

	go s.scanDiskUse()
}
