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
	"context"
	"encoding/json"
	"fmt"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/schema"
)

// RestoreRolesAndUsers applies a backup's role and user snapshots cluster-wide
// in one RAFT entry. A request with both blobs empty is rejected.
func (s *Raft) RestoreRolesAndUsers(ctx context.Context, roles, users []byte, stripNamespaces bool) error {
	if len(roles) == 0 && len(users) == 0 {
		return fmt.Errorf("no roles or users to restore: %w", schema.ErrBadRequest)
	}

	req := cmd.RestoreRolesAndUsersRequest{Roles: roles, Users: users, StripNamespaces: stripNamespaces}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_RESTORE_ROLES_AND_USERS,
		SubCommand: subCommand,
	}
	if _, err := s.Execute(ctx, command); err != nil {
		return err
	}
	return nil
}
