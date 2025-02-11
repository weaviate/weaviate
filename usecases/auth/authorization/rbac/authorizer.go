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
		logger = logger.WithField("groups", principal.Groups)
	}

	// Create a slice to store all permission results
	permResults := make([]logrus.Fields, 0, len(resources))

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

		// Store the results for this iteration
		permResults = append(permResults, logrus.Fields{
			"resource": prettyPermissionsResources(perm),
			"results":  prettyStatus(allowed),
		})

		if !allowed {
			if !skipAudit {
				logger.WithField("permissions", permResults).Error("authorization denied")
			}
			return fmt.Errorf("rbac: %w", errors.NewForbidden(principal, prettyPermissionsActions(perm), prettyPermissionsResources(perm)))
		}
	}

	// Log all results at once if audit is enabled
	if !skipAudit {
		logger.WithField("permissions", permResults).Info()
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
		logger = logger.WithField("groups", principal.Groups)
	}

	var permResults []logrus.Fields
	var allowedResources []string

	// First try with wildcard
	wildcardPath := authorization.WildcardPath(resources[0])
	authorization.Collections()
	allowed, err := m.checkPermissions(principal, wildcardPath, verb)
	if err != nil {
		logger.WithError(err).WithField("resource", wildcardPath).Error("failed to enforce policy")
		return nil, err
	}

	if allowed {
		// If wildcard is allowed, log permissions for all requested resources
		for _, resource := range resources {
			perm, err := conv.PathToPermission(verb, resource)
			if err != nil {
				return nil, err
			}

			permResults = append(permResults, logrus.Fields{
				"resource": prettyPermissionsResources(perm),
				"results":  prettyStatus(true),
			})
		}

		logger.WithField("permissions", permResults).Info()
		return resources, nil
	}

	// If wildcard check failed, check individual resources
	for _, resource := range resources {
		allowed, err := m.checkPermissions(principal, resource, verb)
		if err != nil {
			logger.WithError(err).WithField("resource", resource).Error("failed to enforce policy")
			return nil, err
		}

		perm, err := conv.PathToPermission(verb, resource)
		if err != nil {
			return nil, err
		}

		permResults = append(permResults, logrus.Fields{
			"resource": prettyPermissionsResources(perm),
			"results":  prettyStatus(allowed),
		})

		if allowed {
			allowedResources = append(allowedResources, resource)
		}
	}

	logger.WithField("permissions", permResults).Info()
	return allowedResources, nil
}
