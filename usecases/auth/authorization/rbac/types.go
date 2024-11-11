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
	rolesD      = "roles"
	cluster     = "cluster"
	collections = "collections"
	tenants     = "tenants"
	objects     = "objects"

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
	domain = strings.Split(*permission.Action, "_")[1]
	verb = strings.ToUpper(string(string(*permission.Action)[0]))
	if verb == "M" {
		verb = authorization.CRUD
	}

	switch domain {
	case rolesD:
		if permission.Role == nil {
			resource = authorization.Roles()[0]
		} else {
			resource = authorization.Roles(*permission.Role)[0]
		}
	case cluster:
		resource = authorization.Cluster()
	case collections:
		if permission.Collection == nil {
			resource = authorization.Collections("*")[0]
		} else {
			resource = authorization.Collections(*permission.Collection)[0]
		}
	case tenants:
		if permission.Collection == nil && permission.Tenant == nil {
			resource = authorization.Shards("*", "*")[0]
		} else if permission.Collection != nil && permission.Tenant == nil {
			resource = authorization.Shards(*permission.Collection, "*")[0]
		} else if permission.Collection == nil && permission.Tenant != nil {
			resource = authorization.Shards("*", *permission.Tenant)[0]
		} else {
			resource = authorization.Shards(*permission.Collection, *permission.Tenant)[0]
		}
	case objects:
		if permission.Collection == nil && permission.Tenant == nil && permission.Object == nil {
			resource = authorization.Objects("*", "*", "*")
		} else if permission.Collection != nil && permission.Tenant == nil && permission.Object == nil {
			resource = authorization.Objects(*permission.Collection, "*", "*")
		} else if permission.Collection != nil && permission.Tenant != nil && permission.Object == nil {
			resource = authorization.Objects(*permission.Collection, *permission.Tenant, "*")
		} else {
			resource = authorization.Objects(*permission.Collection, *permission.Tenant, strfmt.UUID(*permission.Object))
		}
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
	case objects:
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
