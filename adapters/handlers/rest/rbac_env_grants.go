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

package rest

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
)

// roleAssigner submits a role assignment as a RAFT log entry.
type roleAssigner interface {
	AddRolesForUser(user string, roles []string) error
}

// envGrantBackoff bounds the env grant writer to 10 retries after the first
// round. The base wait starts at 1s and grows 1.5x per round up to 30s; each
// actual wait is the base ±50% (backoff v4 default multiplier and jitter).
func envGrantBackoff(ctx context.Context) backoff.BackOff {
	return backoff.WithContext(
		backoff.WithMaxRetries(
			backoff.NewExponentialBackOff(
				backoff.WithInitialInterval(time.Second),
				backoff.WithMaxInterval(30*time.Second),
				backoff.WithMaxElapsedTime(0),
			),
			10,
		),
		ctx,
	)
}

// bootEnvRoleGrants writes the admin and viewer grants from rbacConf to RAFT
// after wait returns nil. If RBAC is disabled or there are no grants, it calls
// neither wait nor the assigner.
func bootEnvRoleGrants(rbacConf rbacconf.Config, authNconf config.Authentication, assigner roleAssigner,
	wait func() error, b backoff.BackOff, logger logrus.FieldLogger,
) {
	if !rbacConf.Enabled {
		return
	}
	grants := rbac.AdminViewerGrants(rbacConf, authNconf)
	if len(grants) == 0 {
		return
	}
	if err := wait(); err != nil {
		logger.Errorf("writing env admin/viewer grants to RAFT skipped: %v", err)
		return
	}
	writeEnvRoleGrants(assigner, grants, b, logger)
}

// writeEnvRoleGrants submits each AdminUsers/ViewerUsers grant as a RAFT entry,
// so the grant replays on every restart path like an API assignment. The
// entries are idempotent upserts, so every node may submit them on every boot.
// revokeRoleFromUser refuses to revoke a grant the handling node's config lists.
func writeEnvRoleGrants(assigner roleAssigner, grants []rbac.RoleGrant, b backoff.BackOff, logger logrus.FieldLogger) {
	pending := grants
	err := backoff.Retry(func() error {
		var failed []rbac.RoleGrant
		var lastErr error
		for _, g := range pending {
			if err := assigner.AddRolesForUser(g.Subject, []string{g.Role}); err != nil {
				failed = append(failed, g)
				lastErr = err
			}
		}
		pending = failed
		return lastErr
	}, b)
	if err != nil {
		names := make([]string, len(pending))
		for i, g := range pending {
			names[i] = fmt.Sprintf("%s=%s", g.Subject, g.Role)
		}
		logger.WithField("pending_grants", names).
			Warnf("env admin/viewer grants not written to RAFT; this node holds them in memory only: %v", err)
		return
	}
	logger.WithField("grants", len(grants)).Info("env admin/viewer grants written to RAFT")
}
