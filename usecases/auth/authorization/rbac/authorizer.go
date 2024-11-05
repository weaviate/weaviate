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

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization/errors"
)

type Authorizer struct {
	enforcer *Enforcer
	logger   logrus.FieldLogger
}

// New Authorizer using the AdminList method
func New(RBACEnforcer *Enforcer, logger logrus.FieldLogger) *Authorizer {
	return &Authorizer{RBACEnforcer, logger}
}

// Authorize will give full access (to any resource!) if the user is part of
// the admin list or no access at all if they are not
func (a *Authorizer) Authorize(principal *models.Principal, verb, resource string) error {
	if a.enforcer == nil {
		return fmt.Errorf("rbac enforcer expected but not set up")
	}

	a.logger.WithFields(logrus.Fields{
		"user":     principal.Username,
		"resource": resource,
		"action":   verb,
	}).Debug("checking for role")

	allow, err := a.enforcer.Enforce(principal.Username, resource, verb)
	if err != nil {
		a.logger.WithFields(logrus.Fields{
			"user":     principal.Username,
			"resource": resource,
			"action":   verb,
		}).WithError(err).Error("failed to enforce policy")
		return err
	}

	// TODO audit-log ?
	if allow {
		return nil
	}

	return errors.NewForbidden(principal, verb, resource)
}
