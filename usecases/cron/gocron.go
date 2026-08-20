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
	"slices"
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

// registrant is one cron job Crons owns.
type registrant interface {
	// jobName is the cron entry name, unique across registrants.
	jobName() string
	// hookKey prefix-matches runtime-config Go field names, so it must
	// prefix the field whose changes RuntimeConfigHook reacts to.
	hookKey() string
	RuntimeConfigHook() error
}

type Crons struct {
	objectsttl       *cronsObjectsTTL
	namespaceCleanup *cronsNamespaceCleanup

	registrations []registrant

	logger            logrus.FieldLogger
	gocronLogger      gocron.Logger
	serverShutdownCtx context.Context
}

func NewCrons(serverShutdownCtx context.Context, logger logrus.FieldLogger, configGetter configGetter) (*Crons, error) {
	logger = logger.WithField("action", "cron")
	gocronLogger := cron.NewGoCronLogger(logger, logrus.DebugLevel)

	namespaceCleanup, err := newCronsNamespaceCleanup(serverShutdownCtx, logger, gocronLogger, configGetter)
	if err != nil {
		return nil, err
	}

	c := &Crons{
		objectsttl:        newCronsObjectsTTL(serverShutdownCtx, logger, gocronLogger, configGetter),
		namespaceCleanup:  namespaceCleanup,
		logger:            logger,
		gocronLogger:      gocronLogger,
		serverShutdownCtx: serverShutdownCtx,
	}

	// Registering here rather than in Init is what makes RuntimeConfigHooks
	// complete: startup collects the hook map before the goroutine running
	// Init exists, so a slice filled in Init would hand it no keys at all.
	for _, r := range []registrant{c.objectsttl, c.namespaceCleanup} {
		if err := c.add(r); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// add appends a registrant, refusing a job name that is empty or already
// taken. Two registrants sharing a name would take over each other's entry on
// every reload, each seeing a successful registration, because
// DrainAndUpsertJob never reports a duplicate name.
func (c *Crons) add(r registrant) error {
	name := r.jobName()
	if name == "" {
		return fmt.Errorf("cron registrant has an empty job name")
	}
	if slices.ContainsFunc(c.registrations, func(other registrant) bool {
		return other.jobName() == name
	}) {
		return fmt.Errorf("cron job %q is already registered", name)
	}
	// RuntimeConfigHooks keeps one hook per key, so a second job holding a
	// key would replace the first and leave it reading a value nobody pushes
	// to it. Two jobs paced by one config field need that map to call every
	// job holding the key; this guard goes when it does.
	if key := r.hookKey(); slices.ContainsFunc(c.registrations, func(other registrant) bool {
		return other.hookKey() == key
	}) {
		return fmt.Errorf("cron job %q shares runtime config hook key %q with another job", name, key)
	}
	c.registrations = append(c.registrations, r)
	return nil
}

// blocking
func (c *Crons) Init(clusterService *cluster.Service, ttlCoordinator *objectttl.Coordinator,
	nsCleanupCoordinator *namespacecleanup.Coordinator,
) error {
	cr := initGoCron(c.serverShutdownCtx, c.gocronLogger)

	if err := c.initJobs(cr, clusterService, ttlCoordinator, nsCleanupCoordinator); err != nil {
		return err
	}

	cr.Start()
	<-c.serverShutdownCtx.Done()
	cr.Stop()
	// Await the namespace-cleanup registration goroutine before returning.
	c.namespaceCleanup.wait()

	return nil
}

// initJobs starts each job's registration loop on cr, handing namespace
// cleanup clusterService.IsLeader so only the leader ticks it. Each loop adds
// its entry on its own goroutine, and a job its config disables adds none.
// Init blocks on serverShutdownCtx straight after this call, so everything
// Init does before cr.Start() goes here.
func (c *Crons) initJobs(cr *gocron.Cron, clusterService *cluster.Service,
	ttlCoordinator *objectttl.Coordinator, nsCleanupCoordinator *namespacecleanup.Coordinator,
) error {
	if err := c.objectsttl.Init(cr, clusterService, ttlCoordinator); err != nil {
		return fmt.Errorf("init objects ttl cron: %w", err)
	}
	if err := c.namespaceCleanup.Init(cr, clusterService.IsLeader, nsCleanupCoordinator); err != nil {
		return fmt.Errorf("init namespace cleanup cron: %w", err)
	}
	return nil
}

func (c *Crons) RuntimeConfigHooks() map[string]func() error {
	hooks := make(map[string]func() error, len(c.registrations))
	for _, r := range c.registrations {
		hooks[r.hookKey()] = r.RuntimeConfigHook
	}
	return hooks
}

// ----------------------------------------------------------------------------

const objectsTTLJobName = "trigger_objects_ttl_deletion"

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
		jobLogger := c.logger.WithField("job", objectsTTLJobName)
		var jobCtx context.Context
		var cancel context.CancelFunc = func() {} // noop
		wgRunning := new(sync.WaitGroup)

		for {
			select {
			case schedule := <-c.scheduleCh:
				cancel()
				if cr.RemoveByName(objectsTTLJobName) {
					jobLogger.Info("cron job removed")
				}

				if schedule == "" {
					jobLogger.Info("cron job skipped, no schedule")
					continue
				}

				// ensure removed job is no longer running before adding one with new schedule
				wgRunning.Wait()
				// ensure context still valid after waiting
				select {
				case <-c.serverShutdownCtx.Done():
					jobLogger.Debug("server shutdown context cancelled")
					return
				default:
				}

				jobCtx, cancel = context.WithCancel(c.serverShutdownCtx)
				job := c.createJob(jobCtx, jobLogger, c.gocronLogger, clusterService, coordinator, wgRunning)

				entryId, err := cr.AddJob(schedule, job, gocron.WithName(objectsTTLJobName))
				if err != nil {
					jobLogger.WithError(err).Error("cron job not added")
					continue
				}
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

func (c *cronsObjectsTTL) jobName() string { return objectsTTLJobName }

func (c *cronsObjectsTTL) hookKey() string { return "ObjectsTTL" }

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
