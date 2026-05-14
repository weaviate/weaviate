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
)

// AddNamespace proposes an AddNamespace RAFT command and returns the apply
// version. The apply side rejects duplicates with
// [namespaces.ErrAlreadyExists] and invalid names with
// [namespaces.ErrBadRequest]. Callers that need a follow-up local read on
// a non-leader node should pass the returned version to WaitForUpdate.
func (s *Raft) AddNamespace(ctx context.Context, ns cmd.Namespace) (uint64, error) {
	req := cmd.AddNamespaceRequest{
		Namespace: ns,
		Version:   cmd.NamespaceLatestCommandPolicyVersion,
	}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_ADD_NAMESPACE,
		SubCommand: subCommand,
	}
	return s.Execute(ctx, command)
}

// ChangeNamespaceState proposes a ChangeNamespaceState RAFT command and
// returns the apply version. The apply side returns [namespaces.ErrNotFound]
// for unknown namespaces and [namespaces.ErrInvalidStateTransition] for
// forbidden transitions; same-state transitions are idempotent.
func (s *Raft) ChangeNamespaceState(ctx context.Context, name string, target cmd.NamespaceState) (uint64, error) {
	req := cmd.ChangeNamespaceStateRequest{
		Name:        name,
		TargetState: target,
		Version:     cmd.NamespaceLatestCommandPolicyVersion,
	}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_CHANGE_NAMESPACE_STATE,
		SubCommand: subCommand,
	}
	return s.Execute(ctx, command)
}

// RemoveNamespaceEntity proposes a RemoveNamespaceEntity RAFT command and
// returns the apply version. The apply side returns [namespaces.ErrNotFound]
// for unknown namespaces and [namespaces.ErrInvalidState] when called on an
// active namespace.
func (s *Raft) RemoveNamespaceEntity(ctx context.Context, name string) (uint64, error) {
	req := cmd.RemoveNamespaceEntityRequest{
		Name:    name,
		Version: cmd.NamespaceLatestCommandPolicyVersion,
	}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_REMOVE_NAMESPACE_ENTITY,
		SubCommand: subCommand,
	}
	return s.Execute(ctx, command)
}
