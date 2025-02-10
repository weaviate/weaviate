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
	"strings"

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

	if len(resources) == 0 {
		return fmt.Errorf("at least 1 resource is required")
	}

	logger := m.logger.WithFields(logrus.Fields{
		"action":         "authorize",
		"user":           principal.Username,
		"component":      authorization.ComponentName,
		"request_action": verb,
	})
	if len(principal.Groups) > 0 {
		logger.Data["groups"] = principal.Groups
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

func (m *manager) FilterAuthorizedResources(principal *models.Principal, verb string, resources ...string) ([]string, error) {
	if principal == nil {
		return nil, errors.NewUnauthenticated()
	}

	if len(resources) == 0 {
		return nil, fmt.Errorf("at least 1 resource is required")
	}

	logger := m.logger.WithFields(logrus.Fields{
		"action":         "authorize",
		"user":           principal.Username,
		"component":      authorization.ComponentName,
		"request_action": verb,
	})

	if len(principal.Groups) > 0 {
		logger.Data["groups"] = principal.Groups
	}

	parts := strings.Split(resources[0], "/")
	parts[len(parts)-1] = "*"
	reconstructed := strings.Join(parts, "/")
	allowed, err := m.checkPermissions(principal, reconstructed, verb)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"resources": resources,
		}).WithError(err).Error("failed to enforce policy")
		return nil, err
	}

	perm, err := conv.PathToPermission(verb, reconstructed)
	if err != nil {
		return nil, err
	}

	logger.WithFields(logrus.Fields{
		"resources": prettyPermissionsResources(perm),
		"results":   prettyStatus(allowed),
	})

	if allowed {
		logger.Info()
		return resources, nil
	}

	var allowedResources []string
	for _, resource := range resources {
		allowed, err := m.checkPermissions(principal, resource, verb)
		if err != nil {
			logger.WithFields(logrus.Fields{
				"resource": resource,
			}).WithError(err).Error("failed to enforce policy")
			return nil, err
		}

		if allowed {
			allowedResources = append(allowedResources, resource)
		}

		perm, err := conv.PathToPermission(verb, resource)
		if err != nil {
			return nil, err
		}

		logger.WithFields(logrus.Fields{
			"resource": prettyPermissionsResources(perm),
			"results":  prettyStatus(allowed),
		})
	}

	logger.Info()

	return allowedResources, nil
}
