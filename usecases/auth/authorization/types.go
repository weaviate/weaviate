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

package authorization

import (
	"fmt"
	"strings"

	"github.com/go-openapi/strfmt"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/verbosity"
)

const (
	// CREATE Represents the action to create a new resource.
	CREATE = "C"
	// READ Represents the action to retrieve a resource.
	READ = "R"
	// UPDATE Represents the action to update an existing resource.
	UPDATE = "U"
	// DELETE Represents the action to delete a resource.
	DELETE = "D"

	ROLE_SCOPE_ALL   = "ALL"
	ROLE_SCOPE_MATCH = "MATCH"

	USER_ASSIGN_AND_REVOKE = "A"
)

const (
	UsersDomain       = "users"
	RolesDomain       = "roles"
	ClusterDomain     = "cluster"
	NodesDomain       = "nodes"
	BackupsDomain     = "backups"
	SchemaDomain      = "schema"
	CollectionsDomain = "collections"
	TenantsDomain     = "tenants"
	DataDomain        = "data"
	ReplicateDomain   = "replicate"
)

var (
	All = String("*")

	AllBackups = &models.PermissionBackups{
		Collection: All,
	}
	AllData = &models.PermissionData{
		Collection: All,
		Tenant:     All,
		Object:     All,
	}
	AllTenants = &models.PermissionTenants{
		Collection: All,
		Tenant:     All,
	}
	AllNodes = &models.PermissionNodes{
		Verbosity:  String(verbosity.OutputVerbose),
		Collection: All,
	}
	AllRoles = &models.PermissionRoles{
		Role:  All,
		Scope: String(models.PermissionRolesScopeAll),
	}
	AllUsers = &models.PermissionUsers{
		Users: All,
	}
	AllCollections = &models.PermissionCollections{
		Collection: All,
	}
	AllReplicate = &models.PermissionReplicate{
		Collection: All,
		Shard:      All,
	}

	ComponentName = "RBAC"

	// Note:  if a new action added, don't forget to add it to availableWeaviateActions
	// to be added to built in roles
	// any action has to contain of `{verb}_{domain}` verb: CREATE, READ, UPDATE, DELETE domain: roles, users, cluster, collections, data
	ReadRoles   = "read_roles"
	CreateRoles = "create_roles"
	UpdateRoles = "update_roles"
	DeleteRoles = "delete_roles"

	ReadCluster = "read_cluster"
	ReadNodes   = "read_nodes"

	AssignAndRevokeUsers = "assign_and_revoke_users"
	CreateUsers          = "create_users"
	ReadUsers            = "read_users"
	UpdateUsers          = "update_users"
	DeleteUsers          = "delete_users"

	ManageBackups = "manage_backups"

	CreateCollections = "create_collections"
	ReadCollections   = "read_collections"
	UpdateCollections = "update_collections"
	DeleteCollections = "delete_collections"

	CreateData = "create_data"
	ReadData   = "read_data"
	UpdateData = "update_data"
	DeleteData = "delete_data"

	CreateTenants = "create_tenants"
	ReadTenants   = "read_tenants"
	UpdateTenants = "update_tenants"
	DeleteTenants = "delete_tenants"

	CreateReplicate = "create_replicate"
	ReadReplicate   = "read_replicate"
	UpdateReplicate = "update_replicate"
	DeleteReplicate = "delete_replicate"

	availableWeaviateActions = []string{
		// Roles domain
		CreateRoles,
		ReadRoles,
		UpdateRoles,
		DeleteRoles,

		// Backups domain
		ManageBackups,

		// Users domain
		AssignAndRevokeUsers,
		CreateUsers,
		ReadUsers,
		UpdateUsers,
		DeleteUsers,

		// Cluster domain
		ReadCluster,

		// Nodes domain
		ReadNodes,

		// Collections domain
		CreateCollections,
		ReadCollections,
		UpdateCollections,
		DeleteCollections,

		// Data domain
		CreateData,
		ReadData,
		UpdateData,
		DeleteData,

		// Tenant domain
		CreateTenants,
		ReadTenants,
		UpdateTenants,
		DeleteTenants,

		// Replicate domain
		CreateReplicate,
		ReadReplicate,
		UpdateReplicate,
		DeleteReplicate,
	}
)

var (
	Viewer       = "viewer"
	Admin        = "admin"
	Root         = "root"
	BuiltInRoles = []string{Viewer, Admin, Root}

	// viewer : can view everything , roles, users, schema, data
	// editor : can create/read/update everything , roles, users, schema, data
	// Admin : aka basically super Admin or root
	BuiltInPermissions = map[string][]*models.Permission{
		Viewer: viewerPermissions(),
		Admin:  adminPermissions(),
		Root:   adminPermissions(),
	}
)

type Policy struct {
	Resource string
	Verb     string
	Domain   string
}

// Cluster returns a string representing the cluster authorization scope.
// The returned string is "cluster/*", which can be used to specify that
// the authorization applies to all resources within the cluster.
func Cluster() string {
	return fmt.Sprintf("%s/*", ClusterDomain)
}

func nodes(verbosity, class string) string {
	if verbosity == "" {
		verbosity = "minimal"
	}
	if verbosity == "minimal" {
		return fmt.Sprintf("%s/verbosity/%s", NodesDomain, verbosity)
	}
	return fmt.Sprintf("%s/verbosity/%s/collections/%s", NodesDomain, verbosity, class)
}

func Nodes(verbosity string, classes ...string) []string {
	classes = schema.UppercaseClassesNames(classes...)

	if len(classes) == 0 || (len(classes) == 1 && (classes[0] == "" || classes[0] == "*")) {
		return []string{nodes(verbosity, "*")}
	}

	resources := make([]string, len(classes))
	for idx := range classes {
		if classes[idx] == "" {
			resources[idx] = nodes(verbosity, "*")
		} else {
			resources[idx] = nodes(verbosity, classes[idx])
		}
	}

	return resources
}

// Users generates a list of user resource strings based on the provided user names.
// If no user names are provided, it returns a default user resource string "users/*".
//
// Parameters:
//
//	users - A variadic parameter representing the user names.
//
// Returns:
//
//	A slice of strings where each string is a formatted user resource string.
func Users(users ...string) []string {
	if len(users) == 0 || (len(users) == 1 && (users[0] == "" || users[0] == "*")) {
		return []string{
			fmt.Sprintf("%s/*", UsersDomain),
		}
	}

	resources := make([]string, len(users))
	for idx := range users {
		resources[idx] = fmt.Sprintf("%s/%s", UsersDomain, users[idx])
	}

	return resources
}

// Roles generates a list of role resource strings based on the provided role names.
// If no role names are provided, it returns a default role resource string "roles/*".
//
// Parameters:
//
//	roles - A variadic parameter representing the role names.
//
// Returns:
//
//	A slice of strings where each string is a formatted role resource string.
func Roles(roles ...string) []string {
	if len(roles) == 0 || (len(roles) == 1 && (roles[0] == "" || roles[0] == "*")) {
		return []string{
			fmt.Sprintf("%s/*", RolesDomain),
		}
	}

	resources := make([]string, len(roles))
	for idx := range roles {
		resources[idx] = fmt.Sprintf("%s/%s", RolesDomain, roles[idx])
	}

	return resources
}

// CollectionsMetadata generates a list of resource strings for the given classes.
// If no classes are provided, it returns a default resource string "collections/*".
// Each class is formatted as "collection/{class}".
//
// Parameters:
//
//	classes - a variadic parameter representing the class names.
//
// Returns:
//
//	A slice of strings representing the resource paths.
func CollectionsMetadata(classes ...string) []string {
	classes = schema.UppercaseClassesNames(classes...)

	if len(classes) == 0 || (len(classes) == 1 && (classes[0] == "" || classes[0] == "*")) {
		return []string{fmt.Sprintf("%s/collections/*/shards/#", SchemaDomain)}
	}

	resources := make([]string, len(classes))
	for idx := range classes {
		if classes[idx] == "" {
			resources[idx] = fmt.Sprintf("%s/collections/*/shards/#", SchemaDomain)
		} else {
			resources[idx] = fmt.Sprintf("%s/collections/%s/shards/#", SchemaDomain, classes[idx])
		}
	}

	return resources
}

func CollectionsData(classes ...string) []string {
	classes = schema.UppercaseClassesNames(classes...)

	if len(classes) == 0 || (len(classes) == 1 && (classes[0] == "" || classes[0] == "*")) {
		return []string{Objects("*", "*", "*")}
	}

	var paths []string
	for _, class := range classes {
		paths = append(paths, Objects(class, "*", "*"))
	}
	return paths
}

func Collections(classes ...string) []string {
	classes = schema.UppercaseClassesNames(classes...)
	return append(CollectionsData(classes...), CollectionsMetadata(classes...)...)
}

// ShardsMetadata generates a list of shard resource strings for a given class and shards.
// If the class is an empty string, it defaults to "*". If no shards are provided,
// it returns a single resource string with a wildcard for shards. If shards are
// provided, it returns a list of resource strings for each shard.
//
// Parameters:
//   - class: The class name for the resource. If empty, defaults to "*".
//   - shards: A variadic list of shard names. If empty, it will replace it with '#' to mark it as collection only check
//
// Returns:
//
//	A slice of strings representing the resource paths for the given class and shards.
func ShardsMetadata(class string, shards ...string) []string {
	class = schema.UppercaseClassesNames(class)[0]
	if class == "" {
		class = "*"
	}

	if len(shards) == 0 || (len(shards) == 1 && (shards[0] == "" || shards[0] == "*")) {
		return []string{fmt.Sprintf("%s/collections/%s/shards/*", SchemaDomain, class)}
	}

	resources := make([]string, len(shards))
	for idx := range shards {
		if shards[idx] == "" {
			resources[idx] = fmt.Sprintf("%s/collections/%s/shards/*", SchemaDomain, class)
		} else {
			resources[idx] = fmt.Sprintf("%s/collections/%s/shards/%s", SchemaDomain, class, shards[idx])
		}
	}

	return resources
}

func ShardsData(class string, shards ...string) []string {
	class = schema.UppercaseClassesNames(class)[0]
	var paths []string
	for _, shard := range shards {
		paths = append(paths, Objects(class, shard, "*"))
	}
	return paths
}

// Objects generates a string representing a path to objects within a collection and shard.
// The path format varies based on the provided class, shard, and id parameters.
//
// Parameters:
// - class: the class of the collection (string)
// - shard: the shard identifier (string)
// - id: the unique identifier of the object (strfmt.UUID)
//
// Returns:
// - A string representing the path to the objects, with wildcards (*) used for any empty parameters.
//
// Example outputs:
// - "collections/*/shards/*/objects/*" if all parameters are empty
// - "collections/*/shards/*/objects/{id}" if only id is provided
// - "collections/{class}/shards/{shard}/objects/{id}" if all parameters are provided
func Objects(class, shard string, id strfmt.UUID) string {
	class = schema.UppercaseClassesNames(class)[0]
	if class == "" {
		class = "*"
	}
	if shard == "" {
		shard = "*"
	}
	if id == "" {
		id = "*"
	}
	return fmt.Sprintf("%s/collections/%s/shards/%s/objects/%s", DataDomain, class, shard, id)
}

// Backups generates a resource string for the given classes.
// If the backend is an empty string, it defaults to "*".

// Parameters:
// - class: the class name (string)

// Returns:
// - A string representing the resource path for the given classes.

// Example outputs:
// - "backups/*" if the backend is an empty string
// - "backups/{backend}" for the provided backend
func Backups(classes ...string) []string {
	classes = schema.UppercaseClassesNames(classes...)
	if len(classes) == 0 || (len(classes) == 1 && (classes[0] == "" || classes[0] == "*")) {
		return []string{fmt.Sprintf("%s/collections/*", BackupsDomain)}
	}

	resources := make([]string, len(classes))
	for idx := range classes {
		if classes[idx] == "" {
			resources[idx] = fmt.Sprintf("%s/collections/*", BackupsDomain)
		} else {
			resources[idx] = fmt.Sprintf("%s/collections/%s", BackupsDomain, classes[idx])
		}
	}

	return resources
}

// Replications generates a replication resource string for a given class and shard.
//
// Parameters:
//   - class: The class name for the resource. If empty, defaults to "*".
//   - shard: The shard name for the resource. If empty, defaults to "*".
//
// Returns:
//
//	A slice of strings representing the resource paths for the given class and shards.
func Replications(class, shard string) string {
	class = schema.UppercaseClassName(class)
	if class == "" {
		class = "*"
	}
	if shard == "" {
		shard = "*"
	}
	return fmt.Sprintf("%s/collections/%s/shards/%s", ReplicateDomain, class, shard)
}

// WildcardPath returns the appropriate wildcard path based on the domain and original resource path.
// The domain is expected to be the first part of the resource path.
func WildcardPath(resource string) string {
	parts := strings.Split(resource, "/")
	parts[len(parts)-1] = "*"
	return strings.Join(parts, "/")
}

func String(s string) *string {
	return &s
}

// viewer : can view everything , roles, users, schema, data
func viewerPermissions() []*models.Permission {
	perms := []*models.Permission{}
	for _, action := range availableWeaviateActions {
		if strings.ToUpper(action)[0] != READ[0] {
			continue
		}

		perms = append(perms, &models.Permission{
			Action:      &action,
			Backups:     AllBackups,
			Data:        AllData,
			Nodes:       AllNodes,
			Roles:       AllRoles,
			Collections: AllCollections,
			Tenants:     AllTenants,
			Users:       AllUsers,
		})
	}

	return perms
}

// Admin : aka basically super Admin or root
func adminPermissions() []*models.Permission {
	// TODO ignore CRUD if there is manage
	perms := []*models.Permission{}
	for _, action := range availableWeaviateActions {
		perms = append(perms, &models.Permission{
			Action:      &action,
			Backups:     AllBackups,
			Data:        AllData,
			Nodes:       AllNodes,
			Roles:       AllRoles,
			Collections: AllCollections,
			Tenants:     AllTenants,
			Users:       AllUsers,
		})
	}

	return perms
}

func VerbWithScope(verb, scope string) string {
	if strings.Contains(verb, "_") {
		return verb
	}

	return fmt.Sprintf("%s_%s", verb, scope)
}
