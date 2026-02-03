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
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/namespace"
)

const AuditLogVersion = 2

func (m *Manager) authorize(ctx context.Context, principal *models.Principal, verb string, skipAudit bool, resources ...string) error {
	if principal == nil {
		return fmt.Errorf("rbac: %w", errors.NewUnauthenticated())
	}

	if len(resources) == 0 {
		return fmt.Errorf("at least 1 resource is required")
	}

	logger := m.logger.WithFields(logrus.Fields{
		"action":           "authorize",
		"user":             principal.Username,
		"component":        authorization.ComponentName,
		"request_action":   verb,
		"rbac_log_version": AuditLogVersion,
	})
	if !m.rbacConf.IpInAuditDisabled {
		sourceIp := ctx.Value("sourceIp")
		logger = logger.WithField("source_ip", sourceIp)
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
		// Check if user has implicit access via namespace ownership
		if m.hasNamespaceAccess(principal, resource) {
			perm, err := conv.PathToPermission(verb, resource)
			if err != nil {
				return fmt.Errorf("rbac: %w", err)
			}
			permResults = append(permResults, logrus.Fields{
				"resource": prettyPermissionsResources(perm),
				"results":  "success (namespace)",
			})
			continue
		}

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
				"resource": prettyPermissionsResources(perm),
				"results":  prettyStatus(allowed),
			})
		}

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

	logger := m.logger.WithFields(logrus.Fields{
		"action":         "authorize",
		"user":           principal.Username,
		"component":      authorization.ComponentName,
		"request_action": verb,
	})

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
		// Check if user has implicit access via namespace ownership
		if m.hasNamespaceAccess(principal, resource) {
			perm, err := conv.PathToPermission(verb, resource)
			if err != nil {
				return nil, err
			}
			permResults = append(permResults, logrus.Fields{
				"resource": prettyPermissionsResources(perm),
				"results":  "success (namespace)",
			})
			allowedResources = append(allowedResources, resource)
			continue
		}

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
				"resource": prettyPermissionsResources(perm),
				"results":  prettyStatus(allowed),
			})
			allowedResources = append(allowedResources, resource)
		}
	}

	logger.WithField("permissions", permResults).Info()
	return allowedResources, nil
}

// hasNamespaceAccess checks if the user has implicit access to the resource
// via namespace ownership. Returns true if:
// - User is bound to a non-default namespace
// - Resource is namespace-scoped (schema, data, tenants, backups, aliases, replicate)
// - Resource's collection belongs to the user's namespace
func (m *Manager) hasNamespaceAccess(principal *models.Principal, resource string) bool {
	if principal == nil || m.namespaceLookup == nil {
		return false
	}

	// Get user's bound namespace
	boundNs, isAdmin := m.namespaceLookup(principal.Username)

	// Admins and users in default namespace use normal RBAC
	if isAdmin || boundNs == "" || boundNs == namespace.DefaultNamespace {
		return false
	}

	// Extract collection name from resource path
	className := extractClassFromResource(resource)
	if className == "" || className == "*" {
		return false // Can't determine class or wildcard, use RBAC
	}

	// Check if class belongs to user's namespace
	return namespace.BelongsToNamespace(className, boundNs)
}

// extractClassFromResource extracts the class name from a resource path.
// Resource formats:
//   - schema/collections/{class}/shards/#
//   - data/collections/{class}/shards/{shard}/objects/{id}
//   - tenants/collections/{class}/tenants/{tenant}
//   - backups/collections/{class}
//   - aliases/collections/{class}/aliases/{alias}
//   - replicate/collections/{class}/shards/{shard}
func extractClassFromResource(resource string) string {
	parts := strings.Split(resource, "/")
	if len(parts) < 3 {
		return ""
	}

	// Check if it's a namespace-scoped domain
	domain := parts[0]
	if !isNamespaceScopedDomain(domain) {
		return "" // Not namespace-scoped (e.g., roles, users, cluster)
	}

	if parts[1] != "collections" {
		return ""
	}

	return parts[2] // The class name (may include namespace prefix)
}

// isNamespaceScopedDomain returns true if the domain is namespace-scoped.
// Namespace-scoped domains contain resources that belong to specific namespaces.
func isNamespaceScopedDomain(domain string) bool {
	switch domain {
	case authorization.SchemaDomain, authorization.DataDomain, authorization.TenantsDomain,
		authorization.BackupsDomain, authorization.AliasesDomain, authorization.ReplicateDomain:
		return true
	default:
		return false
	}
}
