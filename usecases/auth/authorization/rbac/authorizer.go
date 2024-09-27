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

package rbac

import (
	"fmt"

	"github.com/casbin/casbin/v2"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization/errors"
)

// authZ RBAC based
type authZ struct {
	enforcer *casbin.SyncedCachedEnforcer
	logger   logrus.FieldLogger
}

// New Authorizer using the AdminList method
func New(RBACEnforcer *casbin.SyncedCachedEnforcer, logger logrus.FieldLogger) *authZ {
	return &authZ{RBACEnforcer, logger}
}

// Authorize will give full access (to any resource!) if the user is part of
// the admin list or no access at all if they are not
func (a *authZ) Authorize(principal *models.Principal, verb, resource string) error {
	if a.enforcer == nil {
		return fmt.Errorf("rbac enforcer expected but not set up")
	}

	roles, err := a.enforcer.GetRolesForUser(principal.Username)
	if err != nil {
		a.logger.WithField("user", principal.Username).WithError(err).Error("failed to fetch roles for user")
		return err
	}

	// TODO: fine grained resources access
	// for now we will support READONLY/ADMIN on all resources
	// One user can potentially be assigned multiple roles
	for _, role := range roles {
		a.logger.WithFields(logrus.Fields{
			"role":     role,
			"resource": resource,
			"action":   verb,
		}).Debug("checking for role")
		allow, err := a.enforcer.Enforce(role, resource, verb)
		if err != nil {
			a.logger.WithFields(logrus.Fields{
				"role":     role,
				"resource": resource,
				"action":   verb,
			}).WithError(err).Error("failed to enforce policy")

			return err
		}

		if allow {
			// TODO print permission allowed which user accessing which resource with which
			// role
			return nil
		}
	}

	return errors.NewForbidden(principal, verb, resource)
}
