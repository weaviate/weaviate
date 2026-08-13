//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package cluster

import (
	"encoding/json"
	"fmt"

	"github.com/weaviate/weaviate/cluster/dynusers"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/rbac"
	usecasesNamespaces "github.com/weaviate/weaviate/usecases/namespaces"
)

// applyRestoreRolesAndUsers validates both payloads before mutating either store: a
// failure halfway would leave the backup's users running against pre-restore
// roles. Roles apply first because after validation the user apply cannot
// fail, while the role apply can, so a role failure leaves the user store
// untouched.
func applyRestoreRolesAndUsers(c *api.ApplyRequest, authZ *rbac.Manager, dynUsers *dynusers.Manager,
	ns usecasesNamespaces.Exister,
) error {
	req := &api.RestoreRolesAndUsersRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("unmarshal restore-roles-and-users subcommand: %w", err)
	}
	if err := authZ.ValidateBackupSnapshot(req, ns); err != nil {
		return err
	}
	if err := dynUsers.ValidateBackupSnapshot(req, ns); err != nil {
		return err
	}
	if err := authZ.RestoreFromBackup(req); err != nil {
		return err
	}
	return dynUsers.RestoreFromBackup(req)
}
