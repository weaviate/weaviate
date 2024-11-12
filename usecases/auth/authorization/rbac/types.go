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

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

const (
	rolesD             = "roles"
	cluster            = "cluster"
	collections        = "collections"
	tenants            = "tenants"
	objects_collection = "objects_collection"
	objects_tenant     = "objects_tenant"

	// rolePrefix = "r_"
	// userPrefix = "u_"
)

const (
	manageRoles   = "manage_roles"
	manageCluster = "manage_cluster"

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

var builtInRoles = map[string]string{
	"viewer": authorization.READ,
	"editor": authorization.CRU,
	"admin":  authorization.CRUD,
}

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

func policy(permission *models.Permission) *Policy {
	var resource, verb, domain string
	// TODO verify slice position to avoid panics
	_, domain, _ = strings.Cut(*permission.Action, "_")
	verb = strings.ToUpper(string(string(*permission.Action)[0]))
	if verb == "M" {
		verb = authorization.CRUD
	}

	switch domain {
	case rolesD:
		role := "*"
		if permission.Role != nil {
			role = *permission.Role
		}
		resource = authorization.Roles(role)[0]
	case cluster:
		resource = authorization.Cluster()
	case collections:
		collection := "*"
		if permission.Collection != nil {
			collection = *permission.Collection
		}
		resource = authorization.Collections(collection)[0]
	case tenants:
		collection := "*"
		tenant := "*"
		if permission.Collection != nil {
			collection = *permission.Collection
		}
		if permission.Tenant != nil {
			tenant = *permission.Tenant
		}
		resource = authorization.Shards(collection, tenant)[0]
	case objects_collection:
		collection := "*"
		object := "*"
		if permission.Collection != nil {
			collection = *permission.Collection
		}
		if permission.Object != nil {
			object = *permission.Object
		}
		resource = authorization.Objects(collection, "*", strfmt.UUID(object))
	case objects_tenant:
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
		resource = authorization.Objects(collection, tenant, strfmt.UUID(object))
	}
	return &Policy{
		resource: resource,
		verb:     verb,
		domain:   domain,
	}
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
	case objects_collection, objects_tenant:
		permission.Collection = &splits[1]
		permission.Tenant = &splits[3]
		permission.Object = &splits[5]
	case rolesD:
		permission.Role = &splits[1]
	// case cluster:

	case "*":
		permission.Collection = &all
		permission.Tenant = &all
		permission.Object = &all
		permission.Role = &all
	}

	return permission
}
