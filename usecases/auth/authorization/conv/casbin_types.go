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

package conv

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/weaviate/weaviate/entities/schema"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

const (
	// https://casbin.org/docs/rbac/#how-to-distinguish-role-from-user
	// ROLE_NAME_PREFIX to prefix role to help casbin to distinguish on Enforcing
	ROLE_NAME_PREFIX = "role:"
	// USER_NAME_PREFIX to prefix role to help casbin to distinguish on Enforcing
	USER_NAME_PREFIX = "user:"

	// CRUD allow all actions on a resource
	// this is internal for casbin to handle admin actions
	CRUD = "(C)|(R)|(U)|(D)"
	// CRU allow all actions on a resource except DELETE
	// this is internal for casbin to handle editor actions
	CRU = "(C)|(R)|(U)"
	// InternalPlaceHolder is a place holder to mark empty roles
	InternalPlaceHolder = "wv_internal_empty"
)

var (
	BuiltInPolicies = map[string]string{
		authorization.Viewer: authorization.READ,
		authorization.Admin:  CRUD,
	}
	actions = map[string]string{
		CRUD:                 "manage",
		CRU:                  "manage",
		authorization.CREATE: "create",
		authorization.READ:   "read",
		authorization.UPDATE: "update",
		authorization.DELETE: "delete",
	}
)

var resourcePatterns = []string{
	fmt.Sprintf(`^%s/.*$`, authorization.UsersDomain),
	fmt.Sprintf(`^%s/[^/]+$`, authorization.UsersDomain),
	fmt.Sprintf(`^%s/.*$`, authorization.RolesDomain),
	fmt.Sprintf(`^%s/[^/]+$`, authorization.RolesDomain),
	fmt.Sprintf(`^%s/.*$`, authorization.ClusterDomain),
	fmt.Sprintf(`^%s/verbosity/minimal$`, authorization.NodesDomain),
	fmt.Sprintf(`^%s/verbosity/verbose/collections/[^/]+$`, authorization.NodesDomain),
	fmt.Sprintf(`^%s/verbosity/verbose/collections/[^/]+$`, authorization.NodesDomain),
	fmt.Sprintf(`^%s/collections/.*$`, authorization.BackupsDomain),
	fmt.Sprintf(`^%s/collections/[^/]+$`, authorization.BackupsDomain),
	fmt.Sprintf(`^%s/collections/.*$`, authorization.SchemaDomain),
	fmt.Sprintf(`^%s/collections/[^/]+$`, authorization.SchemaDomain),
	fmt.Sprintf(`^%s/collections/[^/]+/shards/.*$`, authorization.SchemaDomain),
	fmt.Sprintf(`^%s/collections/[^/]+/shards/[^/]+/objects/.*$`, authorization.DataDomain),
	fmt.Sprintf(`^%s/collections/[^/]+/shards/[^/]+/objects/[^/]+$`, authorization.DataDomain),
}

func newPolicy(policy []string) *authorization.Policy {
	return &authorization.Policy{
		Resource: fromCasbinResource(policy[1]),
		Verb:     policy[2],
		Domain:   policy[3],
	}
}

func fromCasbinResource(resource string) string {
	return strings.ReplaceAll(resource, ".*", "*")
}

func CasbinClusters() string {
	return fmt.Sprintf("%s/.*", authorization.ClusterDomain)
}

func CasbinNodes(verbosity, class string) string {
	class = schema.UppercaseClassesNames(class)[0]
	if verbosity == "minimal" {
		return fmt.Sprintf("%s/verbosity/minimal", authorization.NodesDomain)
	}
	if class == "" {
		class = "*"
	}
	class = strings.ReplaceAll(class, "*", ".*")
	return fmt.Sprintf("%s/verbosity/verbose/collections/%s", authorization.NodesDomain, class)
}

func CasbinBackups(class string) string {
	class = schema.UppercaseClassesNames(class)[0]
	if class == "" {
		class = "*"
	}
	class = strings.ReplaceAll(class, "*", ".*")
	return fmt.Sprintf("%s/collections/%s", authorization.BackupsDomain, class)
}

func CasbinUsers(user string) string {
	if user == "" {
		user = "*"
	}
	user = strings.ReplaceAll(user, "*", ".*")
	return fmt.Sprintf("%s/%s", authorization.UsersDomain, user)
}

func CasbinRoles(role string) string {
	if role == "" {
		role = "*"
	}
	role = strings.ReplaceAll(role, "*", ".*")
	return fmt.Sprintf("%s/%s", authorization.RolesDomain, role)
}

func CasbinRolesWithScope(scope authorization.RoleScope, role string) string {
	if role == "" {
		role = "*"
	}
	role = strings.ReplaceAll(role, "*", ".*")

	return fmt.Sprintf("%s/name/%s/scope/%s", authorization.RolesDomain, role, string(scope))
}

func CasbinSchema(collection, shard string) string {
	collection = schema.UppercaseClassesNames(collection)[0]
	if collection == "" {
		collection = "*"
	}
	if shard == "" {
		shard = "*"
	}
	collection = strings.ReplaceAll(collection, "*", ".*")
	shard = strings.ReplaceAll(shard, "*", ".*")
	return fmt.Sprintf("%s/collections/%s/shards/%s", authorization.SchemaDomain, collection, shard)
}

func CasbinData(collection, shard, object string) string {
	collection = schema.UppercaseClassesNames(collection)[0]
	if collection == "" {
		collection = "*"
	}
	if shard == "" {
		shard = "*"
	}
	if object == "" {
		object = "*"
	}
	collection = strings.ReplaceAll(collection, "*", ".*")
	shard = strings.ReplaceAll(shard, "*", ".*")
	object = strings.ReplaceAll(object, "*", ".*")
	return fmt.Sprintf("%s/collections/%s/shards/%s/objects/%s", authorization.DataDomain, collection, shard, object)
}

func extractFromExtAction(inputAction string) (string, string, error) {
	action, domain, found := strings.Cut(inputAction, "_")
	if !found {
		return "", "", fmt.Errorf("invalid action: %s", inputAction)
	}
	verb := strings.ToUpper(action[:1])
	if verb == "M" {
		verb = CRUD
	}

	if !validVerb(verb) {
		return "", "", fmt.Errorf("invalid verb: %s", verb)
	}

	return verb, domain, nil
}

// casbinPolicyDomains decouples the endpoints domains
// from the casbin internal domains.
// e.g.
// [create_collections, create_tenants] -> schema domain
func casbinPolicyDomains(domain string) string {
	switch domain {
	case authorization.CollectionsDomain, authorization.TenantsDomain:
		return authorization.SchemaDomain
	default:
		return domain
	}
}

func policy(permission *models.Permission) ([]*authorization.Policy, error) {
	if permission.Action == nil {
		return []*authorization.Policy{{Resource: InternalPlaceHolder}}, nil
	}

	verb, domain, err := extractFromExtAction(*permission.Action)
	if err != nil {
		return nil, err
	}

	var resources []string
	switch domain {
	case authorization.UsersDomain:
		// do nothing TODO-RBAC: to be handled when dynamic users management gets added
		user := "*"
		resources = append(resources, CasbinUsers(user))
	case authorization.RolesDomain:
		role := "*"
		if permission.Roles != nil && permission.Roles.Role != nil {
			role = *permission.Roles.Role
		}
		resources = append(resources, CasbinRoles(role))

		// scope only relevant for manage
		if verb == CRUD {
			scope := authorization.RoleScopeMatch
			if permission.Roles != nil && permission.Roles.Scope != nil {
				scope, err = authorization.NewRoleScope(*permission.Roles.Scope)
				if err != nil {
					return nil, err
				}
			}

			resources = append(resources, CasbinRolesWithScope(scope, role))
		}
	case authorization.ClusterDomain:
		resources = append(resources, CasbinClusters())
	case authorization.CollectionsDomain:
		collection := "*"
		tenant := "#"
		if permission.Collections != nil && permission.Collections.Collection != nil {
			collection = schema.UppercaseClassName(*permission.Collections.Collection)
		}
		resources = append(resources, CasbinSchema(collection, tenant))

	case authorization.TenantsDomain:
		collection := "*"
		tenant := "*"
		if permission.Tenants != nil {
			if permission.Tenants.Collection != nil {
				collection = schema.UppercaseClassName(*permission.Tenants.Collection)
			}

			if permission.Tenants.Tenant != nil {
				tenant = *permission.Tenants.Tenant
			}
		}
		resources = append(resources, CasbinSchema(collection, tenant))
	case authorization.DataDomain:
		collection := "*"
		tenant := "*"
		object := "*"
		if permission.Data != nil && permission.Data.Collection != nil {
			collection = schema.UppercaseClassName(*permission.Data.Collection)
		}
		if permission.Data != nil && permission.Data.Tenant != nil {
			tenant = *permission.Data.Tenant
		}
		if permission.Data != nil && permission.Data.Object != nil {
			object = *permission.Data.Object
		}
		resources = append(resources, CasbinData(collection, tenant, object))
	case authorization.BackupsDomain:
		collection := "*"
		if permission.Backups != nil {
			if permission.Backups.Collection != nil {
				collection = schema.UppercaseClassName(*permission.Backups.Collection)
			}
		}
		resources = append(resources, CasbinBackups(collection))
	case authorization.NodesDomain:
		collection := "*"
		verbosity := "minimal"
		if permission.Nodes != nil {
			if permission.Nodes.Collection != nil {
				collection = schema.UppercaseClassName(*permission.Nodes.Collection)
			}
			if permission.Nodes.Verbosity != nil {
				verbosity = *permission.Nodes.Verbosity
			}
		}
		resources = append(resources, CasbinNodes(verbosity, collection))
	default:
		return nil, fmt.Errorf("invalid domain: %s", domain)

	}
	for _, resource := range resources {
		if !validResource(resource) {
			return nil, fmt.Errorf("invalid resource: %s", resource)
		}
	}

	policies := make([]*authorization.Policy, 0, len(resources))
	for _, resource := range resources {
		policies = append(policies, &authorization.Policy{
			Resource: resource,
			Verb:     verb,
			Domain:   casbinPolicyDomains(domain),
		})
	}

	return policies, nil
}

func weaviatePermissionAction(pathLastPart, verb, domain string) string {
	action := fmt.Sprintf("%s_%s", actions[verb], domain)
	action = strings.ReplaceAll(action, "_*", "")
	switch domain {
	case authorization.SchemaDomain:
		if pathLastPart == "#" {
			// e.g
			// schema/collections/ABC/shards/#    collection permission
			// schema/collections/ABC/shards/*    tenant permission
			action = fmt.Sprintf("%s_%s", actions[verb], authorization.CollectionsDomain)
		} else {
			action = fmt.Sprintf("%s_%s", actions[verb], authorization.TenantsDomain)
		}
		return action
	default:
		return action
	}
}

func permission(policy []string, validatePath bool) (*models.Permission, error) {
	mapped := newPolicy(policy)

	if mapped.Resource == InternalPlaceHolder {
		return &models.Permission{}, nil
	}

	if !validVerb(mapped.Verb) {
		return nil, fmt.Errorf("invalid verb: %s", mapped.Verb)
	}

	permission := &models.Permission{}

	splits := strings.Split(mapped.Resource, "/")

	// validating the resource can be expensive (regexp!)
	if validatePath && !validResource(mapped.Resource) {
		return nil, fmt.Errorf("invalid resource: %s", mapped.Resource)
	}

	switch mapped.Domain {
	case authorization.SchemaDomain:
		if splits[4] == "#" {
			permission.Collections = &models.PermissionCollections{
				Collection: &splits[2],
			}
		} else {
			permission.Tenants = &models.PermissionTenants{
				Collection: &splits[2],
				Tenant:     &splits[4],
			}
		}
	case authorization.DataDomain:
		permission.Data = &models.PermissionData{
			Collection: &splits[2],
			Tenant:     &splits[4],
			Object:     &splits[6],
		}
	case authorization.RolesDomain:
		if mapped.Verb == CRUD {
			if len(splits) == 5 {
				permission.Roles = &models.PermissionRoles{Role: &splits[2], Scope: &splits[4]}
			} else {
				// we create two paths for the role permission with scope. The full one which contains all needed
				// info to recreate the permission from it and a shortened one that only contains the role name.
				// We can skip the second one, as we already create the full permission from the first one.
				return nil, nil
			}
		} else if mapped.Verb == authorization.READ {
			permission.Roles = &models.PermissionRoles{Role: &splits[1]}
		}
	case authorization.NodesDomain:
		verbosity := splits[2]
		var collection *string
		if verbosity == "minimal" {
			collection = nil
		} else {
			collection = &splits[4]
		}
		permission.Nodes = &models.PermissionNodes{
			Collection: collection,
			Verbosity:  &verbosity,
		}
	case authorization.BackupsDomain:
		permission.Backups = &models.PermissionBackups{
			Collection: &splits[2],
		}
	case *authorization.All:
		permission.Backups = authorization.AllBackups
		permission.Data = authorization.AllData
		permission.Nodes = authorization.AllNodes
		permission.Roles = authorization.AllRoles
		permission.Collections = authorization.AllCollections
		permission.Tenants = authorization.AllTenants
	case authorization.ClusterDomain, authorization.UsersDomain:
		// do nothing
	default:
		return nil, fmt.Errorf("invalid domain: %s", mapped.Domain)
	}

	permission.Action = authorization.String(weaviatePermissionAction(splits[len(splits)-1], mapped.Verb, mapped.Domain))
	return permission, nil
}

func validResource(input string) bool {
	for _, pattern := range resourcePatterns {
		matched, err := regexp.MatchString(pattern, input)
		if err != nil {
			return false
		}
		if matched {
			return true
		}
	}
	return false
}

func validVerb(input string) bool {
	return regexp.MustCompile(CRUD).MatchString(input)
}

func PrefixRoleName(name string) string {
	if strings.HasPrefix(name, ROLE_NAME_PREFIX) {
		return name
	}
	return fmt.Sprintf("%s%s", ROLE_NAME_PREFIX, name)
}

func PrefixUserName(name string) string {
	if strings.HasPrefix(name, USER_NAME_PREFIX) {
		return name
	}
	return fmt.Sprintf("%s%s", USER_NAME_PREFIX, name)
}

func TrimRoleNamePrefix(name string) string {
	return strings.TrimPrefix(name, ROLE_NAME_PREFIX)
}

func TrimUserNamePrefix(name string) string {
	return strings.TrimPrefix(name, USER_NAME_PREFIX)
}
