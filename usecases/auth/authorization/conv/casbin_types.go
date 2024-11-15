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

	// rolePrefix = "r_"
	// userPrefix = "u_"
)

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

func CasbinRoles(role string) string {
	if role == "" {
		role = "*"
	}
	role = strings.ReplaceAll(role, "*", ".*")
	return fmt.Sprintf("roles/%s", role)
}

func CasbinCollections(collection string) string {
	if collection == "" {
		collection = "*"
	}
	collection = strings.ReplaceAll(collection, "*", ".*")
	return fmt.Sprintf("collections/%s/*", collection)
}

func CasbinShards(collection, shard string) string {
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

func CasbinObjects(collection, shard, object string) string {
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
	return fmt.Sprintf("collections/%s/shards/%s/objects/%s", collection, shard, object)
}

func policy(permission *models.Permission) (*authorization.Policy, error) {
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
		resource = CasbinRoles(role)
	case cluster:
		resource = authorization.Cluster()
	case collections:
		collection := "*"
		if permission.Collection != nil {
			collection = *permission.Collection
		}
		resource = CasbinCollections(collection)
	case tenants:
		collection := "*"
		tenant := "*"
		if permission.Collection != nil {
			collection = *permission.Collection
		}
		if permission.Tenant != nil {
			tenant = *permission.Tenant
		}
		resource = CasbinShards(collection, tenant)
	case objectsCollection:
		collection := "*"
		object := "*"
		if permission.Collection != nil {
			collection = *permission.Collection
		}
		if permission.Object != nil {
			object = *permission.Object
		}
		resource = CasbinObjects(collection, "*", object)
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
		resource = CasbinObjects(collection, tenant, object)
	default:
		return nil, fmt.Errorf("invalid domain: %s", domain)
	}

	return &authorization.Policy{
		Resource: resource,
		Verb:     verb,
		Domain:   domain,
	}, nil
}

func permission(policy []string) *models.Permission {
	mapped := newPolicy(policy)

	action := fmt.Sprintf("%s_%s", authorization.Actions[mapped.Verb], mapped.Domain)
	action = strings.ReplaceAll(action, "_*", "")
	permission := &models.Permission{
		Action: &action,
	}

	splits := strings.Split(mapped.Resource, "/")
	all := "*"

	switch mapped.Domain {
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

	case "*":
		permission.Collection = &all
		permission.Tenant = &all
		permission.Object = &all
		permission.Role = &all
	}

	return permission
}
