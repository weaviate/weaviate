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

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

const (
	// https://casbin.org/docs/rbac/#how-to-distinguish-role-from-user
	// ROLE_NAME_PREFIX to prefix role to help casbin to distinguish on Enforcing
	ROLE_NAME_PREFIX = "role" + PREFIX_SEPARATOR
	// GROUP_NAME_PREFIX to prefix role to help casbin to distinguish on Enforcing
	GROUP_NAME_PREFIX = "group" + PREFIX_SEPARATOR
	PREFIX_SEPARATOR  = ":"

	// CRUD allow all actions on a resource
	// this is internal for casbin to handle admin actions
	CRUD = "(C)|(R)|(U)|(D)"
	// CRU allow all actions on a resource except DELETE
	// this is internal for casbin to handle editor actions
	CRU         = "(C)|(R)|(U)"
	VALID_VERBS = "(C)|(R)|(U)|(D)|(A)"
	// InternalPlaceHolder is a place holder to mark empty roles
	InternalPlaceHolder = "wv_internal_empty"
)

var (
	BuiltInPolicies = map[string]string{
		authorization.Viewer: authorization.READ,
		authorization.Admin:  VALID_VERBS,
		authorization.Root:   VALID_VERBS,
	}
	weaviate_actions_prefixes = map[string]string{
		CRUD:                                 "manage",
		CRU:                                  "manage",
		authorization.ROLE_SCOPE_MATCH:       "manage",
		authorization.CREATE:                 "create",
		authorization.READ:                   "read",
		authorization.UPDATE:                 "update",
		authorization.DELETE:                 "delete",
		authorization.USER_ASSIGN_AND_REVOKE: "assign_and_revoke",
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
	fmt.Sprintf(`^%s/collections/[^/]+/shards/[^/]+$`, authorization.ReplicateDomain),
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

func CasbinReplicate(collection, shard string) string {
	collection = schema.UppercaseClassesNames(collection)[0]
	if collection == "" {
		collection = "*"
	}
	if shard == "" {
		shard = "*"
	}
	collection = strings.ReplaceAll(collection, "*", ".*")
	shard = strings.ReplaceAll(shard, "*", ".*")
	return fmt.Sprintf("%s/collections/%s/shards/%s", authorization.ReplicateDomain, collection, shard)
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
	splits := strings.Split(inputAction, "_")
	if len(splits) < 2 {
		return "", "", fmt.Errorf("invalid action: %s", inputAction)
	}
	domain := splits[len(splits)-1]
	verb := strings.ToUpper(splits[0][:1])
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

func policy(permission *models.Permission) (*authorization.Policy, error) {
	if permission.Action == nil {
		return &authorization.Policy{Resource: InternalPlaceHolder}, nil
	}

	verb, domain, err := extractFromExtAction(*permission.Action)
	if err != nil {
		return nil, err
	}

	var resource string
	switch domain {
	case authorization.UsersDomain:
		user := "*"
		if permission.Users != nil && permission.Users.Users != nil {
			user = *permission.Users.Users
		}
		resource = CasbinUsers(user)
	case authorization.RolesDomain:
		role := "*"
		// default verb for role to handle cases where role is nil
		origVerb := verb
		verb = authorization.VerbWithScope(verb, authorization.ROLE_SCOPE_MATCH)
		if permission.Roles != nil && permission.Roles.Role != nil {
			role = *permission.Roles.Role
			if permission.Roles.Scope != nil {
				verb = authorization.VerbWithScope(origVerb, strings.ToUpper(*permission.Roles.Scope))
			}
		}
		resource = CasbinRoles(role)
	case authorization.ClusterDomain:
		resource = CasbinClusters()
	case authorization.CollectionsDomain:
		collection := "*"
		tenant := "#"
		if permission.Collections != nil && permission.Collections.Collection != nil {
			collection = schema.UppercaseClassName(*permission.Collections.Collection)
		}
		resource = CasbinSchema(collection, tenant)

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
		resource = CasbinSchema(collection, tenant)
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
		resource = CasbinData(collection, tenant, object)
	case authorization.BackupsDomain:
		collection := "*"
		if permission.Backups != nil {
			if permission.Backups.Collection != nil {
				collection = schema.UppercaseClassName(*permission.Backups.Collection)
			}
		}
		resource = CasbinBackups(collection)
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
		resource = CasbinNodes(verbosity, collection)
	case authorization.ReplicateDomain:
		collection := "*"
		shard := "*"
		if permission.Replicate != nil {
			if permission.Replicate.Collection != nil {
				collection = schema.UppercaseClassName(*permission.Replicate.Collection)
			}
			if permission.Replicate.Shard != nil {
				shard = *permission.Replicate.Shard
			}
		}
		resource = CasbinReplicate(collection, shard)
	default:
		return nil, fmt.Errorf("invalid domain: %s", domain)

	}
	if !validResource(resource) {
		return nil, fmt.Errorf("invalid resource: %s", resource)
	}

	return &authorization.Policy{
		Resource: resource,
		Verb:     verb,
		Domain:   casbinPolicyDomains(domain),
	}, nil
}

func weaviatePermissionAction(pathLastPart, verb, domain string) string {
	action := fmt.Sprintf("%s_%s", weaviate_actions_prefixes[verb], domain)
	action = strings.ReplaceAll(action, "_*", "")
	switch domain {
	case authorization.SchemaDomain:
		if pathLastPart == "#" {
			// e.g
			// schema/collections/ABC/shards/#    collection permission
			// schema/collections/ABC/shards/*    tenant permission
			action = fmt.Sprintf("%s_%s", weaviate_actions_prefixes[verb], authorization.CollectionsDomain)
		} else {
			action = fmt.Sprintf("%s_%s", weaviate_actions_prefixes[verb], authorization.TenantsDomain)
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
		permission.Roles = &models.PermissionRoles{
			Role: &splits[1],
		}

		verbSplits := strings.Split(mapped.Verb, "_")
		mapped.Verb = verbSplits[0]
		scope := strings.ToLower(verbSplits[1])
		permission.Roles.Scope = &scope

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
	case authorization.UsersDomain:
		permission.Users = &models.PermissionUsers{
			Users: &splits[1],
		}
	case authorization.ReplicateDomain:
		permission.Replicate = &models.PermissionReplicate{
			Collection: &splits[2],
			Shard:      &splits[4],
		}
	case *authorization.All:
		permission.Backups = authorization.AllBackups
		permission.Data = authorization.AllData
		permission.Nodes = authorization.AllNodes
		permission.Roles = authorization.AllRoles
		permission.Collections = authorization.AllCollections
		permission.Tenants = authorization.AllTenants
		permission.Users = authorization.AllUsers
		permission.Replicate = authorization.AllReplicate
	case authorization.ClusterDomain:
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
	return regexp.MustCompile(VALID_VERBS).MatchString(input)
}

func PrefixRoleName(name string) string {
	if strings.HasPrefix(name, ROLE_NAME_PREFIX) {
		return name
	}
	return fmt.Sprintf("%s%s", ROLE_NAME_PREFIX, name)
}

func PrefixGroupName(name string) string {
	if strings.HasPrefix(name, GROUP_NAME_PREFIX) {
		return name
	}
	return fmt.Sprintf("%s%s", GROUP_NAME_PREFIX, name)
}

func NameHasPrefix(name string) bool {
	return strings.Contains(name, PREFIX_SEPARATOR)
}

func UserNameWithTypeFromPrincipal(principal *models.Principal) string {
	return fmt.Sprintf("%s:%s", principal.UserType, principal.Username)
}

func UserNameWithTypeFromId(username string, userType models.UserTypeInput) string {
	return fmt.Sprintf("%s:%s", userType, username)
}

func TrimRoleNamePrefix(name string) string {
	return strings.TrimPrefix(name, ROLE_NAME_PREFIX)
}

func GetUserAndPrefix(name string) (string, string) {
	splits := strings.Split(name, PREFIX_SEPARATOR)
	return splits[1], splits[0]
}
