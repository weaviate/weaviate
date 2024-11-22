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
// rolePrefix = "r_"
// userPrefix = "u_"
)

var resourcePatterns = []string{
	`^meta/users/.*$`,
	`^meta/users/[^/]+$`,
	`^meta/roles/.*$`,
	`^meta/roles/[^/]+$`,
	`^meta/cluster/.*$`,
	`^meta/backups/.*/ids/.*$`,
	`^meta/backups/[^/]+/ids/.*$`,
	`^meta/backups/.*/ids/[^/]+$`,
	`^meta/backups/[^/]+/ids/[^/]+$`,
	`^meta/collections/.*$`,
	`^meta/collections/[^/]+$`,
	`^meta/collections/[^/]+/shards/.*$`,
	`^data/collections/[^/]+/shards/[^/]+/objects/.*$`,
	`^data/collections/[^/]+/shards/[^/]+/objects/[^/]+$`,
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
	return "meta/cluster/.*"
}

func CasbinBackups(backend, id string) string {
	if backend == "" {
		backend = "*"
	}
	if id == "" {
		id = "*"
	}
	backend = strings.ReplaceAll(backend, "*", ".*")
	id = strings.ReplaceAll(id, "*", ".*")
	return fmt.Sprintf("meta/backups/%s/ids/%s", backend, id)
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

func CasbinSchema(collection, shard string) string {
	if collection == "" {
		collection = "*"
	}
	if shard == "" {
		shard = "*"
	}
	collection = strings.ReplaceAll(collection, "*", ".*")
	shard = strings.ReplaceAll(shard, "*", ".*")
	return fmt.Sprintf("meta/collections/%s/shards/%s", collection, shard)
}

func CasbinData(collection, shard, object string) string {
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

	if permission.Collection != nil {
		*permission.Collection = schema.UppercaseClassName(*permission.Collection)
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
	case authorization.SchemaDomain:
		collection := "*"
		tenant := "*"
		if permission.Collection != nil {
			collection = *permission.Collection
		}
		if permission.Tenant != nil {
			tenant = *permission.Tenant
		}
		resource = CasbinSchema(collection, tenant)
	case authorization.DataDomain:
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
		resource = CasbinData(collection, tenant, object)
	case authorization.BackupsDomain:
		backend := "*"
		id := "*"
		if permission.Backup != nil {
			if permission.Backup.Backend != nil {
				backend = *permission.Backup.Backend
			}
			if permission.Backup.ID != nil {
				id = *permission.Backup.ID
			}
		}
		resource = CasbinBackups(backend, id)
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
	case authorization.SchemaDomain:
		permission.Collection = &splits[2]
		permission.Tenant = &splits[4]
	case authorization.DataDomain:
		permission.Collection = &splits[2]
		permission.Tenant = &splits[4]
		permission.Object = &splits[6]
	case authorization.RolesDomain:
		permission.Role = &splits[2]
	case authorization.UsersDomain:
		permission.User = &splits[2]
	case authorization.ClusterDomain:
		// do nothing
	case authorization.BackupsDomain:
		permission.Backup = &models.PermissionBackup{
			Backend: &splits[2],
			ID:      &splits[4],
		}
	case *authorization.All:
		permission.Backup = &models.PermissionBackup{
			Backend: authorization.All,
			ID:      authorization.All,
		}
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
