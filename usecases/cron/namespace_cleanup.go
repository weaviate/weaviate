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
	"sync/atomic"
	"time"

	gocron "github.com/netresearch/go-cron"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster"
	"github.com/weaviate/weaviate/entities/errors"
	namespacecleanup "github.com/weaviate/weaviate/usecases/namespace_cleanup"
)

const namespaceCleanupJobName = "namespace_cleanup"

// cronsNamespaceCleanup runs Coordinator.Tick on a configurable interval.
// The interval is read from runtime config and hot-reloads via
// RuntimeConfigHook.
type cronsNamespaceCleanup struct {
	currentInterval atomic.Int64 // time.Duration nanoseconds
	intervalCh      chan time.Duration

	logger            logrus.FieldLogger
	gocronLogger      gocron.Logger
	configGetter      configGetter
	serverShutdownCtx context.Context
}

func newCronsNamespaceCleanup(serverShutdownCtx context.Context,
	logger logrus.FieldLogger, gocronLogger gocron.Logger, configGetter configGetter,
) *cronsNamespaceCleanup {
	current := configGetter().Namespaces.CleanupInterval.Get()
	intervalCh := make(chan time.Duration, 1)
	intervalCh <- current

	c := &cronsNamespaceCleanup{
		intervalCh:        intervalCh,
		logger:            logger,
		gocronLogger:      gocronLogger,
		configGetter:      configGetter,
		serverShutdownCtx: serverShutdownCtx,
	}
	c.currentInterval.Store(int64(current))
	return c
}

// Init launches a goroutine that registers (and re-registers, on
// interval changes) the cleanup job. Returns immediately when namespaces
// are disabled. Rejects a nil coordinator.
func (c *cronsNamespaceCleanup) Init(cr *gocron.Cron, clusterService *cluster.Service,
	coordinator *namespacecleanup.Coordinator,
) error {
	if !c.configGetter().Namespaces.Enabled {
		c.logger.WithField("job", namespaceCleanupJobName).Info("cron job skipped, namespaces disabled")
		return nil
	}
	if coordinator == nil {
		return fmt.Errorf("namespace cleanup coordinator is nil")
	}
	errors.GoWrapper(func() {
		jobLogger := c.logger.WithField("job", namespaceCleanupJobName)
		// runMu serialises the cron callback with the re-registration
		// loop: the callback holds it for one tick, the loop Lock/Unlocks
		// as a barrier before swapping the job.
		runMu := new(sync.Mutex)

		for {
			select {
			case interval := <-c.intervalCh:
				if cr.RemoveByName(namespaceCleanupJobName) {
					jobLogger.Info("cron job removed")
				}
				if interval <= 0 {
					jobLogger.Info("cron job skipped, interval <= 0")
					continue
				}

				runMu.Lock()
				runMu.Unlock()
				select {
				case <-c.serverShutdownCtx.Done():
					jobLogger.Debug("server shutdown context cancelled")
					return
				default:
				}

				schedule := fmt.Sprintf("@every %s", interval)
				job := c.createJob(jobLogger, clusterService, coordinator, runMu)
				entryId, err := cr.AddJob(schedule, job, gocron.WithName(namespaceCleanupJobName))
				if err != nil {
					jobLogger.WithError(err).Error("cron job not added")
					continue
				}
				jobLogger.WithFields(logrus.Fields{
					"entry_id": entryId,
					"schedule": schedule,
				}).Info("cron job added")

			case <-c.serverShutdownCtx.Done():
				jobLogger.Debug("server shutdown context cancelled")
				return
			}
		}
	}, c.logger)

	return nil
}

// createJob returns the per-tick callback. SkipIfStillRunning prevents
// overlap at the cron layer; the coordinator additionally guards against
// concurrent ticks.
func (c *cronsNamespaceCleanup) createJob(jobLogger logrus.FieldLogger,
	clusterService *cluster.Service, coordinator *namespacecleanup.Coordinator,
	runMu *sync.Mutex,
) gocron.Job {
	return gocron.NewChain(
		gocron.SkipIfStillRunning(c.gocronLogger),
	).Then(gocron.FuncJob(func() {
		runMu.Lock()
		defer runMu.Unlock()

		if !clusterService.IsLeader() {
			return
		}
		if err := coordinator.Tick(c.serverShutdownCtx); err != nil {
			jobLogger.WithError(err).Error("namespace cleanup tick failed")
		}
	}))
}

// RuntimeConfigHook re-reads the interval from config; on change, pushes
// the new value to the registration loop.
func (c *cronsNamespaceCleanup) RuntimeConfigHook() error {
	newInterval := int64(c.configGetter().Namespaces.CleanupInterval.Get())
	old := c.currentInterval.Load()
	if old == newInterval || !c.currentInterval.CompareAndSwap(old, newInterval) {
		return nil
	}

	select {
	case <-c.intervalCh:
	default:
	}
	c.intervalCh <- time.Duration(newInterval)
	return nil
}
