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
	"github.com/weaviate/weaviate/entities/errors"
	namespacecleanup "github.com/weaviate/weaviate/usecases/namespace_cleanup"
)

const namespaceCleanupJobName = "namespace_cleanup"

// cronsNamespaceCleanup runs Coordinator.Tick on a configurable interval.
// The interval is read from runtime config and hot-reloads via
// RuntimeConfigHook.
type cronsNamespaceCleanup struct {
	// mu serialises RuntimeConfigHook so the read-compare-store and the
	// channel drain+push run as one unit. Without it, concurrent callers
	// can interleave and leave the channel holding a different interval
	// than currentInterval.
	mu              sync.Mutex
	currentInterval time.Duration // guarded by mu
	intervalCh      chan time.Duration

	// registerWG lets shutdown await Init's registration goroutine instead
	// of returning while it still runs.
	registerWG sync.WaitGroup

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
		currentInterval:   current,
		intervalCh:        intervalCh,
		logger:            logger,
		gocronLogger:      gocronLogger,
		configGetter:      configGetter,
		serverShutdownCtx: serverShutdownCtx,
	}
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
	c.registerWG.Add(1)
	errors.GoWrapper(func() {
		defer c.registerWG.Done()
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
					jobLogger.Errorf("cron job not added: %v", err)
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

// wait blocks until Init's registration goroutine has exited. Returns at
// once when Init never launched one (namespaces disabled, nil coordinator).
func (c *cronsNamespaceCleanup) wait() {
	c.registerWG.Wait()
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
			jobLogger.Errorf("namespace cleanup tick failed: %v", err)
		}
	}))
}

// RuntimeConfigHook re-reads the interval from config and, on change,
// pushes the new value to the registration loop. The whole read-compare-
// store-and-push runs under mu so concurrent callers can't interleave and
// leave the channel holding a different interval than currentInterval.
func (c *cronsNamespaceCleanup) RuntimeConfigHook() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	newInterval := c.configGetter().Namespaces.CleanupInterval.Get()
	if newInterval == c.currentInterval {
		return nil
	}
	c.currentInterval = newInterval

	// Drain the stale value (if any) before pushing. The buffer is size 1,
	// so the send stays non-blocking while we hold the lock.
	select {
	case <-c.intervalCh:
	default:
	}
	c.intervalCh <- newInterval
	return nil
}
