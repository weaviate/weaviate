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

// cronStopTimeout bounds each of the two shutdown waits on its own: the tick
// drain, and then the registration join. One wedged tick can spend both, so
// the worst case is twice this. A var so a test can shorten it.
var cronStopTimeout = 30 * time.Second

type configGetter func() config.Config

// registrant is one cron job Crons owns.
type registrant interface {
	// jobName is the cron entry name, unique across registrants.
	jobName() string
	// hookKey prefix-matches runtime-config Go field names, so it must
	// prefix the field whose changes RuntimeConfigHook reacts to.
	hookKey() string
	RuntimeConfigHook() error
	// wait blocks until this registrant's registration goroutine has exited.
	wait()
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

	objectsTTL, err := newCronsObjectsTTL(serverShutdownCtx, logger, gocronLogger, configGetter)
	if err != nil {
		return nil, err
	}
	namespaceCleanup, err := newCronsNamespaceCleanup(serverShutdownCtx, logger, gocronLogger, configGetter)
	if err != nil {
		return nil, err
	}

	c := &Crons{
		objectsttl:        objectsTTL,
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

	if err := c.initJobs(cr, clusterService.IsLeader, ttlCoordinator, nsCleanupCoordinator); err != nil {
		return err
	}

	cr.Start()
	<-c.serverShutdownCtx.Done()
	// StopWithTimeout halts future fires and drains the ticks already
	// running. This and the join below are separately bounded, and the same
	// wedged tick blocks each of them, so the two waits run back to back.
	if !cr.StopWithTimeout(cronStopTimeout) {
		c.logger.Warnf("cron ticks still running after %s, shutting down anyway", cronStopTimeout)
	}
	c.waitForRegistrations()

	return nil
}

// waitForRegistrations joins every registration goroutine under the same bound
// as the drain, so a loop parked behind a tick that ignores its cancelled
// context delays the exit rather than holding it for good.
func (c *Crons) waitForRegistrations() {
	joined := make(chan string, len(c.registrations))
	for _, r := range c.registrations {
		errors.GoWrapper(func() {
			r.wait()
			joined <- r.jobName()
		}, c.logger)
	}

	stopped := map[string]bool{}
	deadline := time.After(cronStopTimeout)
	for range c.registrations {
		select {
		case name := <-joined:
			stopped[name] = true
		case <-deadline:
			var running []string
			for _, r := range c.registrations {
				if !stopped[r.jobName()] {
					running = append(running, r.jobName())
				}
			}
			c.logger.Warnf("cron registration loops still running after %s, shutting down anyway: %v",
				cronStopTimeout, running)
			return
		}
	}
}

// initJobs starts each job's registration loop on cr, handing each job
// isLeader as its tick gate. Init blocks on serverShutdownCtx straight after
// this call, so anything Init must do before cr.Start() goes here.
func (c *Crons) initJobs(cr *gocron.Cron, isLeader func() bool,
	ttlCoordinator *objectttl.Coordinator, nsCleanupCoordinator *namespacecleanup.Coordinator,
) error {
	if err := c.objectsttl.Init(cr, isLeader, ttlCoordinator); err != nil {
		return fmt.Errorf("init objects ttl cron: %w", err)
	}
	if err := c.namespaceCleanup.Init(cr, isLeader, nsCleanupCoordinator); err != nil {
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

// cronsObjectsTTL runs Coordinator.Start on the leader, on the schedule
// OBJECTS_TTL_DELETE_SCHEDULE configures. Start has no leadership check of its
// own, so the tick gate is the only thing keeping every node off it.
type cronsObjectsTTL struct {
	*cronsRegistration[string]
}

func newCronsObjectsTTL(serverShutdownCtx context.Context,
	logger logrus.FieldLogger, gocronLogger gocron.Logger, configGetter configGetter,
) (*cronsObjectsTTL, error) {
	registration, err := newCronsRegistration(cronsRegistrationConfig[string]{
		name:           objectsTTLJobName,
		runtimeHookKey: "ObjectsTTL",

		configuredValue: func() string { return configGetter().ObjectsTTLDeleteSchedule.Get() },
		// An empty schedule disables the job rather than falling back to a default.
		resolve: func(schedule string) (string, bool) { return schedule, schedule != "" },
		// A schedule change cancels the tick context. On the single-node path
		// that only stops the next collection from starting, so the barrier can
		// still wait out deletions already in flight.
		cancelOnChange: true,

		logger:            logger,
		gocronLogger:      gocronLogger,
		serverShutdownCtx: serverShutdownCtx,
	})
	if err != nil {
		return nil, err
	}

	return &cronsObjectsTTL{cronsRegistration: registration}, nil
}

// Init registers the deletion job and keeps it in step with the configured
// schedule. Rejects a nil coordinator.
func (c *cronsObjectsTTL) Init(cr *gocron.Cron, tickGate func() bool,
	coordinator *objectttl.Coordinator,
) error {
	if coordinator == nil {
		return fmt.Errorf("objects ttl coordinator is nil")
	}

	_, err := c.start(cr, tickGate, func(ctx context.Context) {
		started := time.Now()
		c.jobLogger.Debug("trigger ttl deletion started")

		err := coordinator.Start(ctx, false, started, started)

		jobLogger := c.jobLogger.WithField("took", time.Since(started))
		if err != nil {
			jobLogger.Errorf("trigger ttl deletion failed: %v", err)
			return
		}
		jobLogger.Debug("trigger ttl deletion finished")
	})
	return err
}

func initGoCron(ctx context.Context, logger gocron.Logger) *gocron.Cron {
	return gocron.New(
		gocron.WithContext(ctx),
		gocron.WithLogger(logger),
		gocron.WithChain(gocron.Recover(logger)),
		gocron.WithParser(cron.Parser()),
	)
}
