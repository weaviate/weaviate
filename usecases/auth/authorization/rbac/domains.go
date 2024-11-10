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
	rolesD      = "roles"
	cluster     = "cluster"
	collections = "collections"
	tenants     = "tenants"
	objects     = "objects"

	rolePrefix = "r_"
	userPrefix = "u_"
)

var builtInRoles = map[string]string{
	"viewer": authorization.READ,
	"editor": authorization.CRU,
	"admin":  authorization.CRUD,
}

func permission(policy []string) *models.Permission {
	domain := policy[3]
	action := fmt.Sprintf("%s_%s", authorization.Actions[policy[2]], domain)
	action = strings.ReplaceAll(action, "_*", "")
	permission := &models.Permission{
		Action: &action,
	}

	splits := strings.Split(policy[1], "/")
	switch domain {
	case collections:
		permission.Collection = &splits[1]
	case tenants:
		permission.Tenant = &splits[3]
	case objects:
		permission.Object = &splits[4]
	case rolesD:
	case cluster:

	case "*":
		all := "*"
		permission.Collection = &all
		permission.Tenant = &all
		permission.Object = &all
		// permission.Roles = &splits[4]
	}

	return permission
}
