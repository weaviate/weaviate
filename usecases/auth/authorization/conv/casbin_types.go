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
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

const (
// rolePrefix = "r_"
// userPrefix = "u_"
)

var (
	resourcePatterns = []string{
		`^meta/users/.*$`,
		`^meta/users/[^/]+$`,
		`^meta/roles/.*$`,
		`^meta/roles/[^/]+$`,
		`^meta/cluster/.*$`,
		`^meta/collections/.*$`,
		`^meta/collections/[^/]+$`,
		`^meta/collections/[^/]+/shards/.*$`,
		`^data/collections/[^/]+/shards/[^/]+/objects/.*$`,
		`^data/collections/[^/]+/shards/[^/]+/objects/[^/]+$`,
	}
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

func CasbinClusters() string {
	return "meta/cluster/.*"
}

func CasbinUsers(user string) string {
	if user == "" {
		user = "*"
	}
	user = strings.ReplaceAll(user, "*", ".*")
	return fmt.Sprintf("meta/users/%s", user)
}

func CasbinRoles(role string) string {
	if role == "" {
		role = "*"
	}
	role = strings.ReplaceAll(role, "*", ".*")
	return fmt.Sprintf("meta/roles/%s", role)
}

func CasbinCollections(collection string) string {
	if collection == "" {
		collection = "*"
	}
	collection = strings.ReplaceAll(collection, "*", ".*")
	return fmt.Sprintf("meta/collections/%s/*", collection)
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

	return fmt.Sprintf("meta/collections/%s/shards/%s/*", collection, shard)
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
	return fmt.Sprintf("data/collections/%s/shards/%s/objects/%s", collection, shard, object)
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

	if !validVerb(verb) {
		return nil, fmt.Errorf("invalid verb: %s", verb)
	}

	var resource string
	switch domain {
	case authorization.UsersDomain:
		user := "*"
		if permission.User != nil {
			user = *permission.User
		}
		resource = CasbinUsers(user)
	case authorization.RolesDomain:
		role := "*"
		if permission.Role != nil {
			role = *permission.Role
		}
		resource = CasbinRoles(role)
	case authorization.ClusterDomain:
		resource = CasbinClusters()
	case authorization.CollectionsDomain:
		collection := "*"
		if permission.Collection != nil {
			collection = *permission.Collection
		}
		resource = CasbinCollections(collection)
	case authorization.TenantsDomain:
		collection := "*"
		tenant := "*"
		if permission.Collection != nil {
			collection = *permission.Collection
		}
		if permission.Tenant != nil {
			tenant = *permission.Tenant
		}
		resource = CasbinShards(collection, tenant)
	case authorization.ObjectsCollectionsDomain:
		collection := "*"
		object := "*"
		if permission.Collection != nil {
			collection = *permission.Collection
		}
		if permission.Object != nil {
			object = *permission.Object
		}
		resource = CasbinObjects(collection, "*", object)
	case authorization.ObjectsTenantsDomain:
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

	if !validResource(resource) {
		return nil, fmt.Errorf("invalid resource: %s", resource)
	}

	return &authorization.Policy{
		Resource: resource,
		Verb:     verb,
		Domain:   domain,
	}, nil
}

func permission(policy []string) (*models.Permission, error) {
	mapped := newPolicy(policy)

	if !validVerb(mapped.Verb) {
		return nil, fmt.Errorf("invalid verb: %s", mapped.Verb)
	}

	action := fmt.Sprintf("%s_%s", authorization.Actions[mapped.Verb], mapped.Domain)
	action = strings.ReplaceAll(action, "_*", "")
	permission := &models.Permission{
		Action: &action,
	}

	splits := strings.Split(mapped.Resource, "/")
	if !validResource(mapped.Resource) {
		return nil, fmt.Errorf("invalid resource: %s", mapped.Resource)
	}

	switch mapped.Domain {
	case authorization.CollectionsDomain:
		permission.Collection = &splits[2]
	case authorization.TenantsDomain:
		permission.Collection = &splits[2]
		permission.Tenant = &splits[4]
	case authorization.ObjectsCollectionsDomain, authorization.ObjectsTenantsDomain:
		permission.Collection = &splits[2]
		permission.Tenant = &splits[4]
		permission.Object = &splits[6]
	case authorization.RolesDomain:
		permission.Role = &splits[2]
	case authorization.UsersDomain:
		permission.User = &splits[2]
	case authorization.ClusterDomain:
		// do nothing
	case *authorization.All:
		permission.Collection = authorization.All
		permission.Tenant = authorization.All
		permission.Object = authorization.All
		permission.Role = authorization.All
		permission.User = authorization.All
	default:
		return nil, fmt.Errorf("invalid domain: %s", mapped.Domain)
	}

	return permission, nil
}

func validResource(input string) bool {
	for _, pattern := range resourcePatterns {
		matched, err := regexp.MatchString(pattern, input)
		if err != nil {
			fmt.Printf("Error matching pattern: %v\n", err)
			return false
		}
		if matched {
			return true
		}
	}
	return false
}

func validVerb(input string) bool {
	return regexp.MustCompile(authorization.CRUD).MatchString(input)
}
