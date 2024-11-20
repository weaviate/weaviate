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
	UsersDomain              = "users"
	RolesDomain              = "roles"
	ClusterDomain            = "cluster"
	CollectionsDomain        = "meta_collections"
	TenantsDomain            = "meta_tenants"
	ObjectsCollectionsDomain = "data_collection_objects"
	ObjectsTenantsDomain     = "data_tenant_objects"
)

var Actions = map[string]string{
	CRUD:   "manage",
	CRU:    "manage",
	CREATE: "create",
	READ:   "read",
	UPDATE: "update",
	DELETE: "delete",
}

const (
	ManageRoles   = "manage_roles"
	ReadRoles     = "read_roles"
	ManageUsers   = "manage_users"
	ManageCluster = "manage_cluster"

	CreateCollections = "create_meta_collections"
	ReadCollections   = "read_meta_collections"
	UpdateCollections = "update_meta_collections"
	DeleteCollections = "delete_meta_collections"

	CreateTenants = "create_meta_tenants"
	ReadTenants   = "read_meta_tenants"
	UpdateTenants = "update_meta_tenants"
	DeleteTenants = "delete_meta_tenants"

	CreateObjectsCollection = "create_data_collection_objects"
	ReadObjectsCollection   = "read_data_collection_objects"
	UpdateObjectsCollection = "update_data_collection_objects"
	DeleteObjectsCollection = "delete_data_collection_objects"

	CreateObjectsTenant = "create_data_tenant_objects"
	ReadObjectsTenant   = "read_data_tenant_objects"
	UpdateObjectsTenant = "update_data_tenant_objects"
	DeleteObjectsTenant = "delete_data_tenant_objects"
)

var (
	All = String("*")

	manageAllUsers = &models.Permission{
		Action: String(ManageUsers),
		Role:   All,
	}
	manageAllRoles = &models.Permission{
		Action: String(ManageRoles),
		Role:   All,
	}
	manageAllCluster = &models.Permission{
		Action: String(ManageCluster),
	}

	createAllCollections = &models.Permission{
		Action:     String(CreateCollections),
		Collection: All,
	}
	readAllCollections = &models.Permission{
		Action:     String(ReadCollections),
		Collection: All,
	}
	updateAllCollections = &models.Permission{
		Action:     String(UpdateCollections),
		Collection: All,
	}
	deleteAllCollections = &models.Permission{
		Action:     String(DeleteCollections),
		Collection: All,
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
	BuiltInPermissions = map[string][]*models.Permission{
		viewer: {readAllCollections},
		editor: {createAllCollections, readAllCollections, updateAllCollections},
		admin:  {manageAllUsers, manageAllRoles, manageAllCluster, createAllCollections, readAllCollections, updateAllCollections, deleteAllCollections},
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
	return "meta/cluster/*"
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
			"meta/users/*",
		}
	}

	resources := make([]string, len(users))
	for idx := range users {
		resources[idx] = fmt.Sprintf("meta/users/%s", users[idx])
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
			"meta/roles/*",
		}
	}

	resources := make([]string, len(roles))
	for idx := range roles {
		resources[idx] = fmt.Sprintf("meta/roles/%s", roles[idx])
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
		return []string{"meta/collections/*/*"}
	}

	resources := make([]string, len(classes))
	for idx := range classes {
		if classes[idx] == "" {
			resources[idx] = "meta/collections/*/*"
		} else {
			resources[idx] = fmt.Sprintf("meta/collections/%s/*", classes[idx])
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
		return []string{fmt.Sprintf("meta/collections/%s/shards/*/*", class)}
	}

	resources := make([]string, len(shards))
	for idx := range shards {
		if shards[idx] == "" {
			resources[idx] = fmt.Sprintf("meta/collections/%s/shards/*/*", class)
		} else {
			resources[idx] = fmt.Sprintf("meta/collections/%s/shards/%s/*", class, shards[idx])
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

func Shards(class string, shards ...string) []string {
	return append(ShardsData(class, shards...), ShardsMetadata(class, shards...)...)
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
	return fmt.Sprintf("data/collections/%s/shards/%s/objects/%s", class, shard, id)
}

func String(s string) *string {
	return &s
}
