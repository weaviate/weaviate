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
	"sync/atomic"
	"time"

	gocron "github.com/netresearch/go-cron"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/cron"
	"github.com/weaviate/weaviate/usecases/config"
)

const suspendReconcileJobName = "suspend_reconcile"

// SuspendSweep unloads the local shards of suspended namespaces. The suspend
// reconcile job runs it once per tick.
type SuspendSweep interface {
	// Run is one tick. The unloads it starts may outlive it, and run on ctx.
	Run(ctx context.Context)
	// UnloadInFlight reports whether a Run dispatched the shard's unload and the
	// unload has not returned.
	UnloadInFlight(className, shardName string) bool
}

// NewSuspendSweep builds the sweep on the job's logger, so every line the sweep
// writes names the job.
type NewSuspendSweep func(logger *logrus.Entry) SuspendSweep

// cronsSuspendReconcile closes the local shards of suspended namespaces on every
// node, on the interval SUSPEND_RECONCILE_INTERVAL configures.
type cronsSuspendReconcile struct {
	*cronsRegistration[time.Duration]
	sweepLogger *logrus.Entry
	// sweep is nil until Init builds it, and stays nil when Init skips the job.
	sweep        atomic.Pointer[SuspendSweep]
	configGetter configGetter
}

func suspendReconcileInterval(cfg config.Config, logger logrus.FieldLogger) time.Duration {
	return intervalOrDefault(cfg.Namespaces.SuspendReconcileInterval.Get(),
		config.DefaultSuspendReconcileInterval, logger)
}

func newCronsSuspendReconcile(serverShutdownCtx context.Context,
	logger logrus.FieldLogger, gocronLogger gocron.Logger, configGetter configGetter,
) (*cronsSuspendReconcile, error) {
	jobLogger := logger.WithField("job", suspendReconcileJobName)
	registration, err := newCronsRegistration(cronsRegistrationConfig[time.Duration]{
		name:           suspendReconcileJobName,
		runtimeHookKey: "SuspendReconcile",

		// A metadata-only voter holds no shards to close.
		shouldRegister: func() bool {
			cfg := configGetter()
			return cfg.Namespaces.Enabled && !cfg.Raft.MetadataOnlyVoters
		},
		configuredValue: func() time.Duration {
			return suspendReconcileInterval(configGetter(), jobLogger)
		},
		resolve: func(interval time.Duration) (string, bool) {
			return cron.EverySpec(interval), true
		},

		logger:            logger,
		gocronLogger:      gocronLogger,
		serverShutdownCtx: serverShutdownCtx,
	})
	if err != nil {
		return nil, err
	}

	return &cronsSuspendReconcile{
		cronsRegistration: registration,
		sweepLogger:       jobLogger,
		configGetter:      configGetter,
	}, nil
}

// Init builds the sweep with newSweep, registers it and keeps it in step with
// the configured interval. It registers nothing when namespaces are disabled,
// when this node is a metadata-only voter, or when newSweep is nil, as it is
// without a namespaces license.
func (c *cronsSuspendReconcile) Init(cr *gocron.Cron, tickGate func() bool, newSweep NewSuspendSweep) error {
	if newSweep != nil && c.enabled() {
		sweep := newSweep(c.sweepLogger)
		c.sweep.Store(&sweep)
		_, err := c.start(cr, tickGate, sweep.Run)
		return err
	}
	cfg := c.configGetter()
	reason := "namespaces unlicensed"
	switch {
	case !cfg.Namespaces.Enabled:
		reason = "namespaces disabled"
	case cfg.Raft.MetadataOnlyVoters:
		reason = "this node is a metadata-only voter"
	}
	c.jobLogger.Info("cron job skipped, " + reason)
	return nil
}
