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
	"context"
	"fmt"
	"path"
	"time"

	"github.com/weaviate/weaviate/entities/schema"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/indexcounter"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

func (s *Shard) initNonVector(ctx context.Context, class *models.Class) error {
	before := time.Now()
	defer func() {
		took := time.Since(before)
		s.index.logger.WithFields(logrus.Fields{
			"action":   "init_shard_non_vector",
			"duration": took,
		}).Debugf("loaded non-vector (lsm, object, inverted) in %s for shard %q", took, s.ID())
	}()

	// the shard versioner is also dependency of some of the bucket
	// initializations, so it also needs to happen synchronously
	if err := s.initIndexCounterVersionerAndBitmapFactory(); err != nil {
		return fmt.Errorf("init shard %q: %w", s.ID(), err)
	}

	// Run all other inits in parallel and use a single error group to wait for
	// all init tasks, the wait statement is at the end of this method. No other
	// methods should attempt to wait on this error group.
	eg := enterrors.NewErrorGroupWrapper(s.index.logger)

	eg.Go(func() error {
		return s.initObjectBucket(ctx)
	})

	eg.Go(func() error {
		return s.initProplenTracker()
	})

	// geo props depend on the object bucket and we need to wait for its creation in this case
	hasGeoProp := false
	for _, prop := range class.Properties {
		if len(prop.DataType) != 1 {
			continue
		}
		if prop.DataType[0] == schema.DataTypeGeoCoordinates.String() {
			hasGeoProp = true
			break
		}
	}

	if hasGeoProp {
		err := eg.Wait()
		if err != nil {
			// annotate error with shard id only once, all inner functions should only
			// annotate what they do, but not repeat the shard id.
			return fmt.Errorf("init shard %q: %w", s.ID(), err)
		}
	}

	// error group is passed, so properties can be initialized in parallel with
	// the other initializations going on here.
	s.initProperties(eg, class)

	err := eg.Wait()
	if err != nil {
		// annotate error with shard id only once, all inner functions should only
		// annotate what they do, but not repeat the shard id.
		return fmt.Errorf("init shard %q: %w", s.ID(), err)
	}

	if s.index.AsyncReplicationEnabledForShard(s.name) {
		config := s.index.AsyncReplicationConfig()

		// Compute the effective config (needed for hashtreeHeight) before taking
		// the write lock so we can load the cached hashtree from disk outside it.
		// tryLoadHashtreeFromDisk does synchronous I/O (ReadDir, OpenFile, Remove,
		// Fsync); holding the write lock for its duration would block all concurrent
		// RLock callers (hashbeat readers, object writes, commit handlers).
		effectiveConfig := config
		if s.index.globalreplicationConfig != nil {
			effectiveConfig = config.Effective(*s.index.globalreplicationConfig)
		}
		var cached hashtree.AggregatedHashTree
		cached, err = s.tryLoadHashtreeFromDisk(effectiveConfig.hashtreeHeight)
		if err != nil {
			return fmt.Errorf("load hashtree from disk on shard %q: %w", s.ID(), err)
		}

		func() {
			s.asyncReplicationRWMux.Lock()
			defer s.asyncReplicationRWMux.Unlock()
			err = s.initAsyncReplication(config, cached)
		}()
		if err != nil {
			return fmt.Errorf("init async replication on shard %q: %w", s.ID(), err)
		}
	} else {
		// Discard any .ht left by a previous async-enabled shutdown: the shard
		// will serve writes with async off, which would invalidate the
		// snapshot, and a later runtime enable would otherwise load it stale.
		// See disableAsyncReplication for the symmetric rationale.
		if err := s.removePersistedHashtree(); err != nil {
			return fmt.Errorf("discard stale hashtree on shard %q: %w", s.ID(), err)
		}
		if s.index.replicationEnabled() {
			s.index.logger.Debugf("async replication disabled on shard %q", s.ID())
		}
	}

	// check if we need to set Inverted Index config to use BlockMax inverted format for new properties
	// TODO(amourao): this is a temporary solution, we need to update the inverted index config in the schema as well
	// right now, this is done as part of the migration process, but we need to find a way of dealing with MT indices
	// where some shards are using the old format and some shards are using the new format
	if !s.usingBlockMaxWAND && config.DefaultUsingBlockMaxWAND {
		s.usingBlockMaxWAND = s.areAllSearchableBucketsBlockMax()
	}

	return nil
}

func (s *Shard) initLSMStore() error {
	annotatedLogger := s.index.logger.WithFields(logrus.Fields{
		"shard": s.name,
		"index": s.index.ID(),
		"class": s.index.Config.ClassName,
	})

	var metrics *lsmkv.Metrics
	var err error

	if s.promMetrics != nil {
		metrics, err = lsmkv.NewMetrics(s.promMetrics, string(s.index.Config.ClassName), s.name)
		if err != nil {
			return fmt.Errorf("init lsmkv metrics: %w", err)
		}
	}

	store, err := lsmkv.New(s.pathLSM(), s.path(), annotatedLogger, metrics, s.index.bucketLoadLimiter,
		s.cycleCallbacks.compactionCallbacks,
		s.cycleCallbacks.compactionAuxCallbacks,
		s.cycleCallbacks.flushCallbacks)
	if err != nil {
		return fmt.Errorf("init lsmkv store at %s: %w", s.pathLSM(), err)
	}

	s.store = store

	return nil
}

func (s *Shard) initObjectBucket(ctx context.Context) error {
	opts := s.makeDefaultBucketOptions(lsmkv.StrategyReplace,
		lsmkv.WithSecondaryIndices(1),
		lsmkv.WithKeepTombstones(true),
		lsmkv.WithCalcCountNetAdditions(true),
		lsmkv.WithLazySegmentLoading(false), // always load
		lsmkv.WithClassName(s.index.Config.ClassName.String()),
		// Strip dropped vector indexes from stored objects during compaction/cleanup.
		// Keyed by op type so the persisted edit ops drive which transformer runs.
		lsmkv.WithEditOpTransformers(map[lsmkv.OpType]lsmkv.OpTransformerFactory{
			lsmkv.OpTypeRemoveTargetVectors: dropVectorTransformer(
				s.index.Config.ClassName.String(),
				s.index.Config.SkipWriteClassNameOnDisk,
			),
		}),
	)

	if s.metrics != nil && !s.metrics.grouped {
		// If metrics are grouped we cannot observe the count of an individual
		// shard's object store because there is just a single metric. We would
		// override it. See https://github.com/weaviate/weaviate/issues/4396 for
		// details.
		opts = append(opts, lsmkv.WithMonitorCount())
	}

	err := s.store.CreateOrLoadBucket(ctx, helpers.ObjectsBucketLSM, opts...)
	if err != nil {
		return fmt.Errorf("create objects bucket: %w", err)
	}

	return nil
}

func (s *Shard) initProplenTracker() error {
	plPath := path.Join(s.path(), "proplengths")
	tracker, err := inverted.NewJsonShardMetaData(plPath, s.index.logger)
	if err != nil {
		return fmt.Errorf("init prop length tracker: %w", err)
	}

	s.propLenTracker = tracker
	return nil
}

func (s *Shard) initIndexCounterVersionerAndBitmapFactory() error {
	counter, err := indexcounter.New(s.path())
	if err != nil {
		return fmt.Errorf("init index counter: %w", err)
	}
	s.counter = counter
	// counter is incremented whenever new docID is fetched, therefore last docID is lower by 1
	s.bitmapFactory = roaringset.NewBitmapFactory(s.bitmapBufPool, func() uint64 { return s.counter.Get() - 1 })

	dataPresent := s.counter.PreviewNext() != 0
	versionPath := path.Join(s.path(), "version")
	versioner, err := newShardVersioner(versionPath, dataPresent)
	if err != nil {
		return fmt.Errorf("init shard versioner: %w", err)
	}
	s.versioner = versioner

	return nil
}
