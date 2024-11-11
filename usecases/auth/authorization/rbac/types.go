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

var builtInRoles = map[string]string{
	"viewer": authorization.READ,
	"editor": authorization.CRU,
	"admin":  authorization.CRUD,
}

func policy(permission *models.Permission) (resource, verb, domain string) {
	// TODO verify slice position to avoid panics
	domain = strings.Split(*permission.Action, "_")[1]
	verb = strings.ToUpper(string(string(*permission.Action)[0]))
	if verb == "M" {
		verb = authorization.CRUD
	}

	resource = ""
	switch domain {
	case rolesD:
		resource = authorization.Roles()[0]
	case cluster:
		resource = authorization.Cluster()
	case collections:
		if permission.Collection == nil {
			return
		}
		resource = authorization.Collections(*permission.Collection)[0]
	case tenants:
		if permission.Collection == nil || permission.Tenant == nil {
			return
		}

		resource = authorization.Shards(*permission.Collection, *permission.Tenant)[0]
	case objects:
		if permission.Collection == nil || permission.Tenant == nil || permission.Object == nil {
			return
		}

		resource = authorization.Objects(*permission.Collection, *permission.Tenant, strfmt.UUID(*permission.Object))
	}
	return
}

func permission(policy []string) *models.Permission {
	domain := policy[3]
	action := fmt.Sprintf("%s_%s", authorization.Actions[policy[2]], domain)
	if domain == "objects" {
		action += "_collection"
	} else if domain == "tenants" {
		action += "_tenants"
	}

	action = strings.ReplaceAll(action, "_*", "")
	permission := &models.Permission{
		Action: &action,
	}

	noResource := policy[1] == ""
	splits := strings.Split(policy[1], "/")
	all := "*"

	if noResource {
		permission.Collection = &all
		permission.Tenant = &all
		permission.Object = &all
		permission.Role = &all
	} else {
		switch domain {
		case collections:
			permission.Collection = &splits[1]
		case tenants:
			permission.Tenant = &splits[3]
		case objects:
			permission.Object = &splits[4]
		case rolesD:
			permission.Role = &splits[4]
		// case cluster:

		case "*":
			permission.Collection = &all
			permission.Tenant = &all
			permission.Object = &all
			permission.Role = &all
		}
	}

	return permission
}
