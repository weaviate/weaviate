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
	objectttl "github.com/weaviate/weaviate/usecases/object_ttl"
)

type configGetter func() config.Config

type Crons struct {
	objectsttl *cronsObjectsTTL

	logger            logrus.FieldLogger
	gocronLogger      gocron.Logger
	serverShutdownCtx context.Context
}

func NewCrons(serverShutdownCtx context.Context, logger logrus.FieldLogger, configGetter configGetter) *Crons {
	logger = logger.WithField("action", "cron")
	gocronLogger := cron.NewGoCronLogger(logger, logrus.DebugLevel)

	return &Crons{
		objectsttl:        newCronsObjectsTTL(serverShutdownCtx, logger, gocronLogger, configGetter),
		logger:            logger,
		gocronLogger:      gocronLogger,
		serverShutdownCtx: serverShutdownCtx,
	}
}

// blocking
func (c *Crons) Init(clusterService *cluster.Service, coordinator *objectttl.Coordinator) error {
	cr := gocron.New(
		gocron.WithContext(c.serverShutdownCtx),
		gocron.WithLogger(c.gocronLogger),
		gocron.WithChain(gocron.Recover(c.gocronLogger)),
		gocron.WithSeconds(),
	)

	if err := c.objectsttl.Init(cr, clusterService, coordinator); err != nil {
		return fmt.Errorf("init objects ttl cron: %w", err)
	}

	cr.Start()
	<-c.serverShutdownCtx.Done()
	cr.Stop()

	return nil
}

func (c *Crons) RuntimeConfigHooks() map[string]func() error {
	return map[string]func() error{
		"ObjectsTTL": c.objectsttl.RuntimeConfigHook,
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
	// Parser matching the Cron's WithSeconds() configuration, used to parse
	// spec strings into Schedule objects for UpdateEntryByName.
	parser := gocron.NewParser(
		gocron.Second | gocron.Minute | gocron.Hour | gocron.Dom | gocron.Month | gocron.Dow | gocron.Descriptor,
	)

	errors.GoWrapper(func() {
		jobName := "trigger_objects_ttl_deletion"
		jobLogger := c.logger.WithField("job", jobName)
		var cancel context.CancelFunc = func() {} // noop
		wgRunning := new(sync.WaitGroup)
		entryExists := false

		for {
			select {
			case schedule := <-c.scheduleCh:
				cancel()

				if schedule == "" {
					if cr.RemoveByName(jobName) {
						entryExists = false
						jobLogger.Info("cron job removed")
					}
					jobLogger.Info("cron job skipped, no schedule")
					continue
				}

				// ensure previous job is no longer running before replacing
				wgRunning.Wait()
				// ensure context still valid after waiting
				select {
				case <-c.serverShutdownCtx.Done():
					jobLogger.Debug("server shutdown context cancelled")
					return
				default:
				}

				var jobCtx context.Context
				jobCtx, cancel = context.WithCancel(c.serverShutdownCtx)
				job := c.createJob(jobCtx, jobLogger, c.gocronLogger, clusterService, coordinator, wgRunning)

				if entryExists {
					// Atomic schedule+job replacement — preserves EntryID, no race window
					sched, err := parser.Parse(schedule)
					if err != nil {
						jobLogger.WithError(err).Error("cron job schedule parse failed")
						continue
					}
					if err := cr.UpdateEntryByName(jobName, sched, job); err != nil {
						jobLogger.WithError(err).Error("cron job update failed")
						// Entry may have been removed externally; fall through to AddJob
						entryExists = false
					} else {
						jobLogger.WithField("schedule", schedule).Info("cron job updated")
						continue
					}
				}

				entryId, err := cr.AddJob(schedule, job, gocron.WithName(jobName))
				if err != nil {
					jobLogger.WithError(err).Error("cron job not added")
					continue
				}
				entryExists = true
				jobLogger.WithFields(logrus.Fields{
					"entry_id": entryId,
					"schedule": schedule,
				}).Info("cron job added")

			case <-c.serverShutdownCtx.Done():
				cancel()
				jobLogger.Debug("server shutdown context cancelled")
				return
			}
		}
	}, c.logger)

	return nil
}

func (c *cronsObjectsTTL) createJob(ctx context.Context, jobLogger logrus.FieldLogger, gocronLogger gocron.Logger,
	clusterService *cluster.Service, coordinator *objectttl.Coordinator, wgRunning *sync.WaitGroup,
) gocron.Job {
	return gocron.NewChain(
		gocron.SkipIfStillRunning(gocronLogger),
	).Then(gocron.FuncJob(func() {
		wgRunning.Add(1)
		defer wgRunning.Done()

		if !clusterService.IsLeader() {
			jobLogger.Debug("not a ttl scheduler - skipping")
			return
		}

		var err error
		started := time.Now()

		jobLogger.Info("trigger ttl deletion started")
		defer func() {
			jobLogger := jobLogger.WithField("took", time.Since(started))
			if err != nil {
				jobLogger.WithError(err).Error("trigger ttl deletion failed")
				return
			}
			jobLogger.Info("trigger ttl deletion finished")
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
