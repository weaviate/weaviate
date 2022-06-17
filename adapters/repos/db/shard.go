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
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/storagestate"
	"github.com/semi-technologies/weaviate/usecases/monitoring"
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
	promMetrics      *monitoring.PrometheusMetrics
	propertyIndices  propertyspecific.Indices
	deletedDocIDs    *docid.InMemDeletedTracker
	cleanupInterval  time.Duration
	cancel           chan struct{}
	propLengths      *inverted.PropertyLengthTracker
	randomSource     *bufferedRandomGen
	versioner        *shardVersioner
	diskScanState    *diskScanState

	status     storagestate.Status
	statusLock sync.Mutex
}

func NewShard(ctx context.Context, promMetrics *monitoring.PrometheusMetrics,
	shardName string, index *Index) (*Shard, error) {
	before := time.Now()

	rand, err := newBufferedRandomGen(64 * 1024)
	if err != nil {
		return nil, errors.Wrap(err, "init bufferend random generator")
	}

	invertedIndexConfig := index.getInvertedIndexConfig()

	s := &Shard{
		index:            index,
		name:             shardName,
		invertedRowCache: inverted.NewRowCacher(500 * 1024 * 1024),
		promMetrics:      promMetrics,
		metrics: NewMetrics(index.logger, promMetrics,
			string(index.Config.ClassName), shardName),
		deletedDocIDs: docid.NewInMemDeletedTracker(),
		cleanupInterval: time.Duration(invertedIndexConfig.
			CleanupIntervalSeconds) * time.Second,
		cancel:        make(chan struct{}, 1),
		randomSource:  rand,
		diskScanState: newDiskScanState(),
	}
	defer s.metrics.ShardStartup(before)

	hnswUserConfig, ok := index.vectorIndexUserConfig.(hnsw.UserConfig)
	if !ok {
		return nil, errors.Errorf("hnsw vector index: config is not hnsw.UserConfig: %T",
			index.vectorIndexUserConfig)
	}

	if hnswUserConfig.Skip {
		s.vectorIndex = noop.NewIndex()
	} else {

		var distProv distancer.Provider

		switch hnswUserConfig.Distance {
		case "", "cosine":
			distProv = distancer.NewDotProductProvider()
		case "l2-squared":
			distProv = distancer.NewL2SquaredProvider()
		case "manhattan":
			distProv = distancer.NewManhattanProvider()
		default:
			return nil, errors.Errorf("unrecognized distance metric %q,"+
				"choose one of [\"cosine\", \"l2-squared\", \"manhattan\"]", hnswUserConfig.Distance)
		}

		vi, err := hnsw.New(hnsw.Config{
			Logger:            index.logger,
			RootPath:          s.index.Config.RootPath,
			ID:                s.ID(),
			ShardName:         s.name,
			ClassName:         s.index.Config.ClassName.String(),
			PrometheusMetrics: s.promMetrics,
			MakeCommitLoggerThunk: func() (hnsw.CommitLogger, error) {
				// Previously we had an interval of 10s in here, which was changed to
				// 0.5s as part of gh-1867. There's really no way to wait so long in
				// between checks: If you are running on a low-powered machine, the
				// interval will simply find that there is no work and do nothing in
				// each iteration. However, if you are running on a very powerful
				// machine within 10s you could have potentially created two units of
				// work, but we'll only be handling one every 10s. This means
				// uncombined/uncodensed hnsw commit logs will keep piling up can only
				// be processes long after the initial insert is complete. This also
				// means that if there is a crash during importing a lot of work needs
				// to be done at startup, since the commit logs still contain too many
				// redundancies. So as of now it seems there are only advantages to
				// running the cleanup checks and work much more often.
				return hnsw.NewCommitLogger(s.index.Config.RootPath, s.ID(), 500*time.Millisecond,
					index.logger)
			},
			VectorForIDThunk: s.vectorByIndexID,
			DistanceProvider: distProv,
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
	var metrics *lsmkv.Metrics
	if s.promMetrics != nil {
		metrics = lsmkv.NewMetrics(s.promMetrics, string(s.index.Config.ClassName), s.name)
	}

	store, err := lsmkv.New(s.DBPathLSM(), annotatedLogger, metrics)
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

func (s *Shard) drop(force bool) error {
	if s.isReadOnly() && !force {
		return storagestate.ErrStatusReadOnly
	}

	s.cancel <- struct{}{}

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
		helpers.BucketFromPropNameLSM(filters.InternalPropID),
		lsmkv.WithStrategy(lsmkv.StrategySetCollection))
	if err != nil {
		return err
	}

	err = s.store.CreateOrLoadBucket(ctx,
		helpers.HashBucketFromPropNameLSM(filters.InternalPropID),
		lsmkv.WithStrategy(lsmkv.StrategyReplace))
	if err != nil {
		return err
	}

	return nil
}

func (s *Shard) addTimestampProperties(ctx context.Context) error {
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}

	if err := s.addCreationTimeUnixProperty(ctx); err != nil {
		return err
	}
	if err := s.addLastUpdateTimeUnixProperty(ctx); err != nil {
		return err
	}

	return nil
}

func (s *Shard) addCreationTimeUnixProperty(ctx context.Context) error {
	err := s.store.CreateOrLoadBucket(ctx,
		helpers.BucketFromPropNameLSM(filters.InternalPropCreationTimeUnix),
		lsmkv.WithStrategy(lsmkv.StrategySetCollection))
	if err != nil {
		return err
	}
	err = s.store.CreateOrLoadBucket(ctx,
		helpers.HashBucketFromPropNameLSM(filters.InternalPropCreationTimeUnix),
		lsmkv.WithStrategy(lsmkv.StrategyReplace))
	if err != nil {
		return err
	}

	return nil
}

func (s *Shard) addLastUpdateTimeUnixProperty(ctx context.Context) error {
	err := s.store.CreateOrLoadBucket(ctx,
		helpers.BucketFromPropNameLSM(filters.InternalPropLastUpdateTimeUnix),
		lsmkv.WithStrategy(lsmkv.StrategySetCollection))
	if err != nil {
		return err
	}
	err = s.store.CreateOrLoadBucket(ctx,
		helpers.HashBucketFromPropNameLSM(filters.InternalPropLastUpdateTimeUnix),
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
	s.cancel <- struct{}{}

	if err := s.propLengths.Close(); err != nil {
		return errors.Wrap(err, "close prop length tracker")
	}

	// to ensure that all commitlog entries are written to disk.
	// otherwise in some cases the tombstone cleanup process'
	// 'RemoveTombstone' entry is not picked up on restarts
	// resulting in perpetually attempting to remove a tombstone
	// which doesn't actually exist anymore
	if err := s.vectorIndex.Flush(); err != nil {
		return errors.Wrap(err, "flush vector index commitlog")
	}

	s.vectorIndex.Shutdown()

	return s.store.Shutdown(ctx)
}

func (s *Shard) notifyReady() {
	s.initStatus()
	s.index.logger.
		WithField("action", "startup").
		Debugf("shard=%s is ready", s.name)
}
