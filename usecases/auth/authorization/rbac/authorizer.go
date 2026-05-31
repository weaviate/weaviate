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

package rbac

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/errors"
)

const AuditLogVersion = 2

// auditFields produces the per-request audit log fields shared between
// Authorize and FilterAuthorizedResources. On namespace enabled clusters it adds
// either namespace=<short> for namespace-bound principals or
// global_operator=true for operator-level principals; NS-disabled clusters
// emit neither field.
func (m *Manager) auditFields(principal *models.Principal) logrus.Fields {
	f := logrus.Fields{
		"action":           "authorize",
		"user":             principal.Username,
		"component":        authorization.ComponentName,
		"rbac_log_version": AuditLogVersion,
	}
	if m.namespacesEnabled {
		switch {
		case principal.IsGlobalOperator:
			f["global_operator"] = true
		case principal.Namespace != "":
			f["namespace"] = principal.Namespace
		default:
			// Defense-in-depth: every namespace enabled principal must be classified
			// as namespace-bound or global upstream. If a producer drifts,
			// default to global (no namespace claim is leaked) and warn so
			// the gap surfaces.
			m.logger.WithField("user", principal.Username).
				Warn("rbac: principal missing namespace and global classification on namespace enabled cluster; defaulting to global_operator")
			f["global_operator"] = true
		}
	}
	return f
}

func (m *Manager) authorize(ctx context.Context, principal *models.Principal, verb string, skipAudit bool, resources ...string) error {
	if principal == nil {
		return fmt.Errorf("rbac: %w", errors.NewUnauthenticated())
	}

	if len(resources) == 0 {
		return fmt.Errorf("at least 1 resource is required")
	}

	logger := m.logger.
		WithFields(m.auditFields(principal)).
		WithField("request_action", verb).
		WithField("audit_mode", "authorize")
	if !m.rbacConf.IpInAuditDisabled {
		sourceIp := ctx.Value("sourceIp")
		logger = logger.WithField("source_ip", sourceIp)
	}
	if clientIdentifier, _ := ctx.Value("clientIdentifier").(string); clientIdentifier != "" {
		logger = logger.WithField("client_identifier", clientIdentifier)
	}

	if len(principal.Groups) > 0 {
		logger = logger.WithField("groups", principal.Groups)
	}

	// Create a map to aggregate resources and their counts while preserving order
	resourceCounts := make(map[string]int)
	var uniqueResources []string
	for _, resource := range resources {
		if _, exists := resourceCounts[resource]; !exists {
			uniqueResources = append(uniqueResources, resource)
		}
		resourceCounts[resource]++
	}
	permResults := make([]logrus.Fields, 0, len(uniqueResources))

	for _, resource := range uniqueResources {
		allowed, err := m.checkPermissions(principal, resource, verb)
		if err != nil {
			logger.WithFields(logrus.Fields{
				"resource": resource,
			}).WithError(err).Error("failed to enforce policy")
			return err
		}

		perm, err := conv.PathToPermission(verb, resource)
		if err != nil {
			return fmt.Errorf("rbac: %w", err)
		}

		if allowed {
			permResults = append(permResults, logrus.Fields{
				"resource": prettyPermissionsResources(principal, perm),
				"results":  prettyStatus(allowed),
			})
		}

		if !allowed {
			if !skipAudit {
				logger.WithFields(logrus.Fields{
					"resource":    prettyPermissionsResources(principal, perm),
					"perm":        prettyPermissionsActions(perm),
					"permissions": permResults,
				}).Error("authorization denied")
			}
			return fmt.Errorf("rbac: %w", errors.NewForbidden(principal, prettyPermissionsActions(perm), prettyPermissionsResources(principal, perm)))
		}
	}

	// Log all results at once if audit is enabled
	if !skipAudit {
		logger.WithField("permissions", permResults).Info()
	}

	return nil
}

// Authorize verify if the user has access to a resource to do specific action
func (m *Manager) Authorize(ctx context.Context, principal *models.Principal, verb string, resources ...string) error {
	return m.authorize(ctx, principal, verb, false, resources...)
}

// AuthorizeSilent verify if the user has access to a resource to do specific action without audit logs
// to be used internally
func (m *Manager) AuthorizeSilent(ctx context.Context, principal *models.Principal, verb string, resources ...string) error {
	return m.authorize(ctx, principal, verb, true, resources...)
}

// FilterAuthorizedResources authorize the passed resources with best effort approach, it will return
// list of allowed resources, if none, it will return an empty slice
func (m *Manager) FilterAuthorizedResources(ctx context.Context, principal *models.Principal, verb string, resources ...string) ([]string, error) {
	if principal == nil {
		return nil, errors.NewUnauthenticated()
	}

	if len(resources) == 0 {
		return nil, fmt.Errorf("at least 1 resource is required")
	}

	logger := m.logger.
		WithFields(m.auditFields(principal)).
		WithField("request_action", verb).
		WithField("audit_mode", "filter")
	if !m.rbacConf.IpInAuditDisabled {
		sourceIp := ctx.Value("sourceIp")
		logger = logger.WithField("source_ip", sourceIp)
	}
	if clientIdentifier, _ := ctx.Value("clientIdentifier").(string); clientIdentifier != "" {
		logger = logger.WithField("client_identifier", clientIdentifier)
	}

	if len(principal.Groups) > 0 {
		logger = logger.WithField("groups", principal.Groups)
	}

	allowedResources := make([]string, 0, len(resources))

	// Create a map to aggregate resources and their counts while preserving order
	resourceCounts := make(map[string]int)
	var uniqueResources []string
	for _, resource := range resources {
		if _, exists := resourceCounts[resource]; !exists {
			uniqueResources = append(uniqueResources, resource)
		}
		resourceCounts[resource]++
	}

	permResults := make([]logrus.Fields, 0, len(uniqueResources))

	for _, resource := range uniqueResources {
		allowed, err := m.checkPermissions(principal, resource, verb)
		if err != nil {
			logger.WithError(err).WithField("resource", resource).Error("failed to enforce policy")
			return nil, err
		}

		if allowed {
			perm, err := conv.PathToPermission(verb, resource)
			if err != nil {
				return nil, err
			}

			permResults = append(permResults, logrus.Fields{
				"resource": prettyPermissionsResources(principal, perm),
				"results":  prettyStatus(allowed),
			})
			allowedResources = append(allowedResources, resource)
		}
	}

	logger.WithField("permissions", permResults).Info()
	return allowedResources, nil
}
