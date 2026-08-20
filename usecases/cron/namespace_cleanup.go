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
	"time"

	gocron "github.com/netresearch/go-cron"
	"github.com/sirupsen/logrus"

	namespacecleanup "github.com/weaviate/weaviate/usecases/namespace_cleanup"
)

const namespaceCleanupJobName = "namespace_cleanup"

// cronsNamespaceCleanup runs Coordinator.Tick on the leader, on the interval
// NAMESPACE_CLEANUP_INTERVAL configures. The tick gate only spares a follower
// the call; Tick re-checks leadership itself.
type cronsNamespaceCleanup struct {
	*cronsRegistration[time.Duration]
}

func newCronsNamespaceCleanup(serverShutdownCtx context.Context,
	logger logrus.FieldLogger, gocronLogger gocron.Logger, configGetter configGetter,
) (*cronsNamespaceCleanup, error) {
	registration, err := newCronsRegistration(cronsRegistrationConfig[time.Duration]{
		name:           namespaceCleanupJobName,
		runtimeHookKey: "NamespaceCleanup",

		shouldRegister:  func() bool { return configGetter().Namespaces.Enabled },
		configuredValue: func() time.Duration { return configGetter().Namespaces.CleanupInterval.Get() },
		resolve: func(interval time.Duration) (string, bool) {
			return fmt.Sprintf("@every %s", interval), interval > 0
		},

		logger:            logger,
		gocronLogger:      gocronLogger,
		serverShutdownCtx: serverShutdownCtx,
	})
	if err != nil {
		return nil, err
	}

	return &cronsNamespaceCleanup{cronsRegistration: registration}, nil
}

// Init registers the cleanup job and keeps it in step with the configured
// interval. Registers nothing when namespaces are disabled. Rejects a nil
// coordinator.
func (c *cronsNamespaceCleanup) Init(cr *gocron.Cron, tickGate func() bool,
	coordinator *namespacecleanup.Coordinator,
) error {
	if coordinator == nil {
		return fmt.Errorf("namespace cleanup coordinator is nil")
	}

	started, err := c.start(cr, tickGate, func(ctx context.Context) {
		if err := coordinator.Tick(ctx); err != nil {
			c.jobLogger.Errorf("namespace cleanup tick failed: %v", err)
		}
	})
	if err != nil {
		return err
	}
	if !started {
		c.jobLogger.Info("cron job skipped, namespaces disabled")
	}
	return nil
}
