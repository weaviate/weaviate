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

package cron

import (
	"context"
	"fmt"
	"sync"
	"time"

	gocron "github.com/netresearch/go-cron"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster"
	"github.com/weaviate/weaviate/entities/cron"
	"github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/config"
	namespacecleanup "github.com/weaviate/weaviate/usecases/namespace_cleanup"
	objectttl "github.com/weaviate/weaviate/usecases/object_ttl"
)

type configGetter func() config.Config

type Crons struct {
	objectsttl       *cronsObjectsTTL
	namespaceCleanup *cronsNamespaceCleanup

	logger            logrus.FieldLogger
	gocronLogger      gocron.Logger
	serverShutdownCtx context.Context
}

func NewCrons(serverShutdownCtx context.Context, logger logrus.FieldLogger, configGetter configGetter) *Crons {
	logger = logger.WithField("action", "cron")
	gocronLogger := cron.NewGoCronLogger(logger, logrus.DebugLevel)

	return &Crons{
		objectsttl:        newCronsObjectsTTL(serverShutdownCtx, logger, gocronLogger, configGetter),
		namespaceCleanup:  newCronsNamespaceCleanup(serverShutdownCtx, logger, gocronLogger, configGetter),
		logger:            logger,
		gocronLogger:      gocronLogger,
		serverShutdownCtx: serverShutdownCtx,
	}
}

// blocking
func (c *Crons) Init(clusterService *cluster.Service, ttlCoordinator *objectttl.Coordinator,
	nsCleanupCoordinator *namespacecleanup.Coordinator,
) error {
	cr := initGoCron(c.serverShutdownCtx, c.gocronLogger)

	if err := c.objectsttl.Init(cr, clusterService, ttlCoordinator); err != nil {
		return fmt.Errorf("init objects ttl cron: %w", err)
	}

	if err := c.namespaceCleanup.Init(cr, clusterService, nsCleanupCoordinator); err != nil {
		return fmt.Errorf("init namespace cleanup cron: %w", err)
	}

	cr.Start()
	<-c.serverShutdownCtx.Done()
	cr.Stop()
	// Await the namespace-cleanup registration goroutine before returning.
	c.namespaceCleanup.wait()

	return nil
}

func (c *Crons) RuntimeConfigHooks() map[string]func() error {
	return map[string]func() error{
		"ObjectsTTL":       c.objectsttl.RuntimeConfigHook,
		"NamespaceCleanup": c.namespaceCleanup.RuntimeConfigHook,
	}
}

// ----------------------------------------------------------------------------

type cronsObjectsTTL struct {
	lock            *sync.Mutex
	currentSchedule string
	scheduleCh      chan string

	logger            logrus.FieldLogger
	gocronLogger      gocron.Logger
	configGetter      configGetter
	serverShutdownCtx context.Context
}

func newCronsObjectsTTL(serverShutdownCtx context.Context,
	logger logrus.FieldLogger, gocronLogger gocron.Logger, configGetter configGetter,
) *cronsObjectsTTL {
	currentSchedule := configGetter().ObjectsTTLDeleteSchedule.Get()
	scheduleCh := make(chan string, 1)
	scheduleCh <- currentSchedule

	return &cronsObjectsTTL{
		lock:            new(sync.Mutex),
		currentSchedule: currentSchedule,
		scheduleCh:      scheduleCh,

		logger:            logger,
		gocronLogger:      gocronLogger,
		configGetter:      configGetter,
		serverShutdownCtx: serverShutdownCtx,
	}
}

func (c *cronsObjectsTTL) Init(cr *gocron.Cron, clusterService *cluster.Service,
	coordinator *objectttl.Coordinator,
) error {
	if coordinator == nil {
		return fmt.Errorf("objects ttl coordinator is nil")
	}
	errors.GoWrapper(func() {
		jobName := "trigger_objects_ttl_deletion"
		jobLogger := c.logger.WithField("job", jobName)
		job := c.createJob(jobLogger, c.gocronLogger, clusterService, coordinator)

		for {
			select {
			case schedule := <-c.scheduleCh:
				if schedule == "" {
					if cr.RemoveByName(jobName) {
						jobLogger.Info("cron job removed")
					}
					jobLogger.Info("cron job skipped, no schedule")
					continue
				}

				// Skip rescheduling if shutdown was requested.
				select {
				case <-c.serverShutdownCtx.Done():
					jobLogger.Debug("server shutdown context cancelled")
					return
				default:
				}

				// Drain the running invocation, then atomically replace the
				// entry, leaving no window where the old schedule can fire again.
				entryId, err := cr.DrainAndUpsertJob(schedule, job, gocron.WithName(jobName))
				if err != nil {
					jobLogger.WithError(err).Error("cron job upsert failed")
					continue
				}
				jobLogger.WithFields(logrus.Fields{
					"entry_id": entryId,
					"schedule": schedule,
				}).Info("cron job upserted")

			case <-c.serverShutdownCtx.Done():
				jobLogger.Debug("server shutdown context cancelled")
				return
			}
		}
	}, c.logger)

	return nil
}

func (c *cronsObjectsTTL) createJob(jobLogger logrus.FieldLogger, gocronLogger gocron.Logger,
	clusterService *cluster.Service, coordinator *objectttl.Coordinator,
) gocron.Job {
	// FuncJobWithContext receives a per-entry context, canceled when the entry
	// is replaced or the scheduler stops, so a long-running deletion can abort.
	return gocron.NewChain(
		gocron.SkipIfStillRunning(gocronLogger),
	).Then(gocron.FuncJobWithContext(func(ctx context.Context) {
		if !clusterService.IsLeader() {
			jobLogger.Debug("not a ttl scheduler - skipping")
			return
		}

		var err error
		started := time.Now()

		jobLogger.Debug("trigger ttl deletion started")
		defer func() {
			jobLogger := jobLogger.WithField("took", time.Since(started))
			if err != nil {
				jobLogger.WithError(err).Error("trigger ttl deletion failed")
				return
			}
			jobLogger.Debug("trigger ttl deletion finished")
		}()

		err = coordinator.Start(ctx, false, started, started)
	}))
}

func (c *cronsObjectsTTL) RuntimeConfigHook() error {
	newSchedule := c.configGetter().ObjectsTTLDeleteSchedule.Get()
	c.lock.Lock()
	if c.currentSchedule == newSchedule {
		c.lock.Unlock()
		// nothing to do, schedule have not changed
		return nil
	}
	c.currentSchedule = newSchedule
	c.lock.Unlock()

	select {
	case <-c.scheduleCh:
		// read previous, not yet handled value. discard in favour of new one
		//
		// It could happen that schedule A was changed to B and then back to A.
		// If B as not applied and read here, effectively it will be A changed to A
		// which could be skipped. For now this unlikely case will be ignored.
	default:
		// nothing in the channel, safe to push new one
	}

	c.scheduleCh <- newSchedule
	return nil
}

func initGoCron(ctx context.Context, logger gocron.Logger) *gocron.Cron {
	return gocron.New(
		gocron.WithContext(ctx),
		gocron.WithLogger(logger),
		gocron.WithChain(gocron.Recover(logger)),
		gocron.WithParser(gocron.FullParser()),
	)
}
