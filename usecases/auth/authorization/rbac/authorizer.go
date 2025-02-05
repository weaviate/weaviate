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
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/errors"
)

func (m *manager) authorize(principal *models.Principal, verb string, skipAudit bool, resources ...string) error {
	if principal == nil {
		return errors.NewUnauthenticated()
	}

	logger := m.logger.WithFields(logrus.Fields{
		"action":         "authorize",
		"user":           principal.Username,
		"component":      authorization.ComponentName,
		"request_action": verb,
	})
	if len(principal.Groups) > 0 {
		logger.WithFields(logrus.Fields{"groups": principal.Groups})
	}

	for _, resource := range resources {
		allowed, err := m.checkPermissions(principal, resource, verb)
		if err != nil {
			logger.WithFields(logrus.Fields{
				"resource": resource,
			}).WithError(err).Error("failed to enforce policy")
			return err
		}

		perm, err := conv.PathToPermission(verb, resource)
		if err != nil {
			return err
		}

		logger.WithFields(logrus.Fields{
			"resources": prettyPermissionsResources(perm),
			"results":   prettyStatus(allowed),
		})

		if !skipAudit {
			logger.Info()
		}

		if !allowed {
			return fmt.Errorf("rbac: %w", errors.NewForbidden(principal, prettyPermissionsActions(perm), prettyPermissionsResources(perm)))
		}
	}
	return nil
}

// Authorize verify if the user has access to a resource to do specific action
func (m *manager) Authorize(principal *models.Principal, verb string, resources ...string) error {
	return m.authorize(principal, verb, false, resources...)
}

// AuthorizeSilent verify if the user has access to a resource to do specific action without audit logs
// to be used internally
func (m *manager) AuthorizeSilent(principal *models.Principal, verb string, resources ...string) error {
	return m.authorize(principal, verb, true, resources...)
}
