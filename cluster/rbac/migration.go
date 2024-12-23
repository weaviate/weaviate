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
	"strings"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

func MigrateCreateRolesRequest(incomingRequest *cmd.CreateRolesRequest) *cmd.CreateRolesRequest {
	switch incomingRequest.Version {
	case 0:
		return migrateCreateRolesRequestV0(incomingRequest)
	default:
		return incomingRequest
	}
}

func migrateCreateRolesRequestV0(incomingRequest *cmd.CreateRolesRequest) *cmd.CreateRolesRequest {
	for i := range incomingRequest.Roles {
		for j := range incomingRequest.Roles[i] {
			if incomingRequest.Roles[i][j].Domain != "schema" {
				continue
			}

			parts := strings.Split(incomingRequest.Roles[i][j].Resource, "/shards/")
			incomingRequest.Roles[i][j].Resource = parts[0]
		}
	}

	return incomingRequest
}

func MigrateRemovePermissionsRequest(incomingRequest *cmd.RemovePermissionsRequest) *cmd.RemovePermissionsRequest {
	switch incomingRequest.Version {
	case 0:
		return migrateRemovePermissionsRequest(incomingRequest)
	default:
		return incomingRequest
	}
}

func migrateRemovePermissionsRequest(incomingRequest *cmd.RemovePermissionsRequest) *cmd.RemovePermissionsRequest {
	for i := range incomingRequest.Permissions {
		if incomingRequest.Permissions[i].Domain != "schema" {
			continue
		}

		parts := strings.Split(incomingRequest.Permissions[i].Resource, "/shards/")
		incomingRequest.Permissions[i].Resource = parts[0]

	}

	return incomingRequest
}
