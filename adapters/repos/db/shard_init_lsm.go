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

	// init the store itself synchronously
	err := s.initLSMStore()
	if err != nil {
		return fmt.Errorf("init shard %q: lsm store: %w", s.ID(), err)
	}

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

	err = eg.Wait()
	if err != nil {
		// annotate error with shard id only once, all inner functions should only
		// annotate what they do, but not repeat the shard id.
		return fmt.Errorf("init shard %q: %w", s.ID(), err)
	}

	// Object bucket must be available, initHashTree depends on it
	if s.index.asyncReplicationEnabled() {
		err = s.initHashTree(ctx)
		if err != nil {
			return fmt.Errorf("init shard %q: shard hashtree: %w", s.ID(), err)
		}
	} else if s.index.replicationEnabled() {
		s.index.logger.Infof("async replication disabled on shard %q", s.ID())
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
	if s.promMetrics != nil {
		metrics = lsmkv.NewMetrics(s.promMetrics, string(s.index.Config.ClassName), s.name)
	}

	store, err := lsmkv.New(s.pathLSM(), s.path(), annotatedLogger, metrics,
		s.cycleCallbacks.compactionCallbacks, s.cycleCallbacks.flushCallbacks)
	if err != nil {
		return fmt.Errorf("init lsmkv store at %s: %w", s.pathLSM(), err)
	}

	s.store = store

	return nil
}

func (s *Shard) initObjectBucket(ctx context.Context) error {
	opts := []lsmkv.BucketOption{
		lsmkv.WithStrategy(lsmkv.StrategyReplace),
		lsmkv.WithSecondaryIndices(2),
		lsmkv.WithPread(s.index.Config.AvoidMMap),
		lsmkv.WithKeepTombstones(true),
		s.dynamicMemtableSizing(),
		s.memtableDirtyConfig(),
		lsmkv.WithAllocChecker(s.index.allocChecker),
		lsmkv.WithMaxSegmentSize(s.index.Config.MaxSegmentSize),
		s.segmentCleanupConfig(),
	}

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
	s.bitmapFactory = roaringset.NewBitmapFactory(s.counter.Get, s.index.logger)

	dataPresent := s.counter.PreviewNext() != 0
	versionPath := path.Join(s.path(), "version")
	versioner, err := newShardVersioner(versionPath, dataPresent)
	if err != nil {
		return fmt.Errorf("init shard versioner: %w", err)
	}
	s.versioner = versioner

	return nil
}
