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
)

const (
	// CRUD allow all actions on a resource
	CRUD = "(C)|(R)|(U)|(D)"
	// CRU allow all actions on a resource except DELETE
	CRU = "(C)|(R)|(U)"
	// CREATE Represents the action to create a new resource.
	CREATE = "C"
	// READ Represents the action to retrieve a resource.
	READ = "R"
	// UPDATE Represents the action to update an existing resource.
	UPDATE = "U"
	// DELETE Represents the action to delete a resource.
	DELETE = "D"
)

const (
	UsersDomain   = "users"
	RolesDomain   = "roles"
	ClusterDomain = "cluster"
	BackupsDomain = "backups"
	SchemaDomain  = "schema"
	DataDomain    = "data"
)

var Actions = map[string]string{
	CRUD:   "manage",
	CRU:    "manage",
	CREATE: "create",
	READ:   "read",
	UPDATE: "update",
	DELETE: "delete",
}

var (
	All = String("*")

	ComponentName = "RBAC"

	// Note:  if a new action added, don't forget to add it to availableWeaviateActions
	// to be added to built in roles
	// any action has to contain of `{verb}_{domain}` verb: CREATE, READ, UPDATE, DELETE domain: roles, users, cluster, schema, data
	ManageRoles   = "manage_roles"
	ReadRoles     = "read_roles"
	ManageUsers   = "manage_users"
	ManageCluster = "manage_cluster"

	ManageBackups = "manage_backups"
	ReadBackups   = "read_backups"

	CreateSchema = "create_schema"
	ReadSchema   = "read_schema"
	UpdateSchema = "update_schema"
	DeleteSchema = "delete_schema"

	CreateData = "create_data"
	ReadData   = "read_data"
	UpdateData = "update_data"
	DeleteData = "delete_data"

	availableWeaviateActions = []string{
		// Roles domain
		ManageRoles,
		ReadRoles,

		// Users domain
		ManageUsers,

		// Cluster domain
		ManageCluster,

		// Schema domain
		CreateSchema,
		ReadSchema,
		UpdateSchema,
		DeleteSchema,

		// Data domain
		CreateData,
		ReadData,
		UpdateData,
		DeleteData,
	}
)

var (
	viewer          = "viewer"
	editor          = "editor"
	admin           = "admin"
	BuiltInRoles    = []string{viewer, editor, admin}
	BuiltInPolicies = map[string]string{
		viewer: READ,
		editor: CRU,
		admin:  CRUD,
	}

	// viewer : can view everything , roles, users, schema, data
	// editor : can create/read/update everything , roles, users, schema, data
	// admin : aka basically super admin or root
	BuiltInPermissions = map[string][]*models.Permission{
		viewer: viewerPermissions(),
		editor: editorPermissions(),
		admin:  adminPermissions(),
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
	if len(classes) == 0 || (len(classes) == 1 && (classes[0] == "" || classes[0] == "*")) {
		return []string{fmt.Sprintf("%s/collections/*/shards/*", SchemaDomain)}
	}

	resources := make([]string, len(classes))
	for idx := range classes {
		if classes[idx] == "" {
			resources[idx] = fmt.Sprintf("%s/collections/*/shards/*", SchemaDomain)
		} else {
			resources[idx] = fmt.Sprintf("%s/collections/%s/shards/*", SchemaDomain, classes[idx])
		}
	}

	return resources
}

func CollectionsData(classes ...string) []string {
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
	return append(CollectionsData(classes...), CollectionsMetadata(classes...)...)
}

// ShardsMetadata generates a list of shard resource strings for a given class and shards.
// If the class is an empty string, it defaults to "*". If no shards are provided,
// it returns a single resource string with a wildcard for shards. If shards are
// provided, it returns a list of resource strings for each shard.
//
// Parameters:
//   - class: The class name for the resource. If empty, defaults to "*".
//   - shards: A variadic list of shard names. If empty, a wildcard is used.
//
// Returns:
//
//	A slice of strings representing the resource paths for the given class and shards.
func ShardsMetadata(class string, shards ...string) []string {
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

// Backups generates a resource string for the given backend.
// If the backend is an empty string, it defaults to "*".

// Parameters:
// - backend: the backend name (string)

// Returns:
// - A string representing the resource path for the given backend.

// Example outputs:
// - "backups/*" if the backend is an empty string
// - "backups/{backend}" for the provided backend
func Backups(backend string) string {
	if backend == "" {
		backend = "*"
	}
	return fmt.Sprintf("%s/backends/%s", BackupsDomain, backend)
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
			Action:     &action,
			Collection: All,
			Tenant:     All,
			Role:       All,
			User:       All,
		})
	}

	return perms
}

// editor : can create/read/update everything , roles, users, schema, data
func editorPermissions() []*models.Permission {
	perms := []*models.Permission{}
	for _, action := range availableWeaviateActions {
		if strings.ToUpper(action)[0] == DELETE[0] {
			continue
		}

		perms = append(perms, &models.Permission{
			Action:     &action,
			Collection: All,
			Tenant:     All,
			Role:       All,
			User:       All,
		})
	}

	return perms
}

// admin : aka basically super admin or root
func adminPermissions() []*models.Permission {
	perms := []*models.Permission{}
	for _, action := range availableWeaviateActions {
		perms = append(perms, &models.Permission{
			Action:     &action,
			Collection: All,
			Tenant:     All,
			Role:       All,
			User:       All,
		})
	}

	return perms
}
