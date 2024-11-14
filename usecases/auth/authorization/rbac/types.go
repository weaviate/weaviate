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

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

const (
	rolesD            = "roles"
	cluster           = "cluster"
	collections       = "collections"
	tenants           = "tenants"
	objectsCollection = "objects_collection"
	objectsTenant     = "objects_tenant"
	gql_schema        = "gql_schema"

	// rolePrefix = "r_"
	// userPrefix = "u_"
)

const (
	manageRoles   = "manage_roles"
	manageCluster = "manage_cluster"

	read_gql_schema = "read_gql_schema"

	createCollections = "create_collections"
	readCollections   = "read_collections"
	updateCollections = "update_collections"
	deleteCollections = "delete_collections"

	createTenants = "create_tenants"
	readTenants   = "read_tenants"
	updateTenants = "update_tenants"
	deleteTenants = "delete_tenants"

	createObjectsCollection = "create_objects_collection"
	readObjectsCollection   = "read_objects_collection"
	updateObjectsCollection = "update_objects_collection"
	deleteObjectsCollection = "delete_objects_collection"

	createObjectsTenant = "create_objects_tenant"
	readObjectsTenant   = "read_objects_tenant"
	updateObjectsTenant = "update_objects_tenant"
	deleteObjectsTenant = "delete_objects_tenant"
)

var (
	all = String("*")

	manageAllRoles = &models.Permission{
		Action: String(manageRoles),
		Role:   all,
	}
	manageAllCluster = &models.Permission{
		Action: String(manageCluster),
	}

	createAllCollections = &models.Permission{
		Action:     String(createCollections),
		Collection: all,
	}
	readAllCollections = &models.Permission{
		Action:     String(readCollections),
		Collection: all,
	}
	updateAllCollections = &models.Permission{
		Action:     String(updateCollections),
		Collection: all,
	}
	deleteAllCollections = &models.Permission{
		Action:     String(deleteCollections),
		Collection: all,
	}
	readGQLSchema = &models.Permission{Action: String(read_gql_schema)}
)

var (
	viewer          = "viewer"
	editor          = "editor"
	admin           = "admin"
	BuiltInRoles    = []string{viewer, editor, admin}
	builtInPolicies = map[string]string{
		viewer: authorization.READ,
		editor: authorization.CRU,
		admin:  authorization.CRUD,
	}
	builtInPermissions = map[string][]*models.Permission{
		viewer: {readAllCollections, readGQLSchema},
		editor: {createAllCollections, readAllCollections, updateAllCollections, readGQLSchema},
		admin:  {manageAllRoles, manageAllCluster, createAllCollections, readAllCollections, updateAllCollections, deleteAllCollections, readGQLSchema},
	}
)

type Policy struct {
	resource string
	verb     string
	domain   string
}

func newPolicy(policy []string) *Policy {
	return &Policy{
		resource: policy[1],
		verb:     policy[2],
		domain:   policy[3],
	}
}

func pRoles(role string) string {
	if role == "" {
		role = "*"
	}
	role = strings.ReplaceAll(role, "*", ".*")
	return fmt.Sprintf("roles/%s", role)
}

func pCollections(collection string) string {
	if collection == "" {
		collection = "*"
	}
	collection = strings.ReplaceAll(collection, "*", ".*")
	return fmt.Sprintf("collections/%s/*", collection)
}

func pShards(collection, shard string) string {
	if collection == "" {
		collection = "*"
	}
	if shard == "" {
		shard = "*"
	}
	collection = strings.ReplaceAll(collection, "*", ".*")
	shard = strings.ReplaceAll(shard, "*", ".*")

	return fmt.Sprintf("collections/%s/shards/%s/*", collection, shard)
}

func pObjects(collection, shard, object string) string {
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
	collection = strings.ReplaceAll(collection, "*", ".*")
	object = strings.ReplaceAll(object, "*", ".*")
	return fmt.Sprintf("collections/%s/shards/%s/objects/%s", collection, shard, object)
}

func policy(permission *models.Permission) (*Policy, error) {
	// TODO verify slice position to avoid panics
	if permission.Action == nil {
		return nil, fmt.Errorf("missing action")
	}
	action, domain, found := strings.Cut(*permission.Action, "_")
	if !found {
		return nil, fmt.Errorf("invalid action: %s", *permission.Action)
	}
	verb := strings.ToUpper(action[:1])
	if verb == "M" {
		verb = authorization.CRUD
	}
	var resource string
	switch domain {
	case rolesD:
		role := "*"
		if permission.Role != nil {
			role = *permission.Role
		}
		resource = pRoles(role)
	case cluster:
		resource = authorization.Cluster()
	case gql_schema:
		resource = authorization.GQLSchema()
	case collections:
		collection := "*"
		if permission.Collection != nil {
			collection = *permission.Collection
		}
		resource = pCollections(collection)
	case tenants:
		collection := "*"
		tenant := "*"
		if permission.Collection != nil {
			collection = *permission.Collection
		}
		if permission.Tenant != nil {
			tenant = *permission.Tenant
		}
		resource = pShards(collection, tenant)
	case objectsCollection:
		collection := "*"
		object := "*"
		if permission.Collection != nil {
			collection = *permission.Collection
		}
		if permission.Object != nil {
			object = *permission.Object
		}
		resource = pObjects(collection, "*", object)
	case objectsTenant:
		collection := "*"
		tenant := "*"
		object := "*"
		if permission.Collection != nil {
			collection = *permission.Collection
		}
		if permission.Tenant != nil {
			tenant = *permission.Tenant
		}
		if permission.Object != nil {
			object = *permission.Object
		}
		resource = pObjects(collection, tenant, object)
	default:
		return nil, fmt.Errorf("invalid domain: %s", domain)
	}

	return &Policy{
		resource: resource,
		verb:     verb,
		domain:   domain,
	}, nil
}

func permission(policy []string) *models.Permission {
	mapped := newPolicy(policy)

	action := fmt.Sprintf("%s_%s", authorization.Actions[mapped.verb], mapped.domain)
	action = strings.ReplaceAll(action, "_*", "")
	permission := &models.Permission{
		Action: &action,
	}

	splits := strings.Split(mapped.resource, "/")
	all := "*"

	switch mapped.domain {
	case collections:
		permission.Collection = &splits[1]
	case tenants:
		permission.Collection = &splits[1]
		permission.Tenant = &splits[3]
	case objectsCollection, objectsTenant:
		permission.Collection = &splits[1]
		permission.Tenant = &splits[3]
		permission.Object = &splits[5]
	case rolesD:
		permission.Role = &splits[1]
	// case cluster:
	// case gql_schema:

	case "*":
		permission.Collection = &all
		permission.Tenant = &all
		permission.Object = &all
		permission.Role = &all
	}

	return permission
}

func String(s string) *string {
	return &s
}
