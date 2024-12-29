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
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/indexcheckpoint"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	entsentry "github.com/weaviate/weaviate/entities/sentry"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func NewShard(ctx context.Context, promMetrics *monitoring.PrometheusMetrics,
	shardName string, index *Index, class *models.Class, jobQueueCh chan job,
	scheduler *queue.Scheduler,
	indexCheckpoints *indexcheckpoint.Checkpoints,
) (_ *Shard, err error) {
	before := time.Now()

	index.logger.WithFields(logrus.Fields{
		"action": "init_shard",
		"shard":  shardName,
		"index":  index.ID(),
	}).Debugf("initializing shard %q", shardName)

	s := &Shard{
		index:       index,
		class:       class,
		name:        shardName,
		promMetrics: promMetrics,
		metrics: NewMetrics(index.logger, promMetrics,
			string(index.Config.ClassName), shardName),
		slowQueryReporter:     helpers.NewSlowQueryReporterFromEnv(index.logger),
		stopDimensionTracking: make(chan struct{}),
		replicationMap:        pendingReplicaTasks{Tasks: make(map[string]replicaTask, 32)},
		centralJobQueue:       jobQueueCh,
		scheduler:             scheduler,
		indexCheckpoints:      indexCheckpoints,

		shut:         false,
		shutdownLock: new(sync.RWMutex),

		status: NewShardStatus(),
	}

	defer func() {
		p := recover()
		if p != nil {
			err = fmt.Errorf("unexpected error initializing shard %q of index %q: %v", shardName, index.ID(), p)
			index.logger.WithError(err).WithFields(logrus.Fields{
				"index": index.ID(),
				"shard": shardName,
			}).Error("panic during shard initialization")
			debug.PrintStack()
		}

		if err != nil {
			// Initializing a shard should normally not fail. If it does, this could
			// mean that this setup requires further attention, e.g. to manually fix
			// a data corruption. This makes it a prime use case for sentry:
			entsentry.CaptureException(err)
			// spawn a new context as we cannot guarantee that the init context is
			// still valid, but we want to make sure that we have enough time to clean
			// up the partial init
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()
			s.index.logger.WithFields(logrus.Fields{
				"action":   "new_shard",
				"duration": 120 * time.Second,
			}).Debug("context.WithTimeout")

			s.cleanupPartialInit(ctx)
		}
	}()

	s.activityTracker.Store(1) // initial state
	s.initCycleCallbacks()

	s.docIdLock = make([]sync.Mutex, IdLockPoolSize)

	defer s.metrics.ShardStartup(before)

	_, err = os.Stat(s.path())
	exists := false
	if err == nil {
		exists = true
	}

	if err := os.MkdirAll(s.path(), os.ModePerm); err != nil {
		return nil, err
	}

	mux := sync.Mutex{}
	s.objectPropagationNeededCond = sync.NewCond(&mux)

	if err := s.initNonVector(ctx, class); err != nil {
		return nil, errors.Wrapf(err, "init shard %q", s.ID())
	}

	if s.hasTargetVectors() {
		if err := s.initTargetVectors(ctx); err != nil {
			return nil, err
		}
		if err := s.initTargetQueues(); err != nil {
			return nil, err
		}
	} else {
		if err := s.initLegacyVector(ctx); err != nil {
			return nil, err
		}
		if err := s.initLegacyQueue(); err != nil {
			return nil, err
		}
	}

	s.initDimensionTracking()

	if asyncEnabled() {
		f := func() {
			// convert in-memory queues to on-disk queues in the background.
			// no-op if the queues are already on disk.
			if s.hasTargetVectors() {
				for targetVector, queue := range s.queues {
					err := s.ConvertQueue(targetVector)
					if err != nil {
						queue.Logger.WithError(err).Errorf("preload shard for target vector: %s", targetVector)
					}
				}
			} else {
				err := s.ConvertQueue("")
				if err != nil {
					s.queue.Logger.WithError(err).Error("preload shard")
				}
			}
		}
		enterrors.GoWrapper(f, s.index.logger)
	}
	s.NotifyReady()

	if exists {
		s.index.logger.Printf("Completed loading shard %s in %s", s.ID(), time.Since(before))
	} else {
		s.index.logger.Printf("Created shard %s in %s", s.ID(), time.Since(before))
	}
	return s, nil
}

// cleanupPartialInit is called when the shard was only partially initialized.
// Internally it just uses [Shutdown], but also adds some logging.
func (s *Shard) cleanupPartialInit(ctx context.Context) {
	log := s.index.logger.WithField("action", "cleanup_partial_initialization")
	if err := s.Shutdown(ctx); err != nil {
		log.WithError(err).Error("failed to shutdown store")
	}

	log.Debug("successfully cleaned up partially initialized shard")
}

func (s *Shard) NotifyReady() {
	s.initStatus()
	s.index.logger.
		WithField("action", "startup").
		Debugf("shard=%s is ready", s.name)
}
