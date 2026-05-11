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

// AddNamespace proposes an AddNamespace RAFT command. The apply side rejects
// duplicates with [namespaces.ErrAlreadyExists] and invalid names with
// [namespaces.ErrBadRequest]; callers that need to distinguish those should
// use errors.Is.
//
// On success the call blocks until the local node has applied the new entry
// so a follow-up local read (e.g. controller.Exists) on the same node sees
// the change. On a follower this absorbs the leader→follower replication
// delay; on the leader it returns immediately.
func (s *Raft) AddNamespace(ctx context.Context, ns cmd.Namespace) error {
	req := cmd.AddNamespaceRequest{
		Namespace: ns,
		Version:   cmd.NamespaceLatestCommandPolicyVersion,
	}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_ADD_NAMESPACE,
		SubCommand: subCommand,
	}
	version, err := s.Execute(ctx, command)
	if err != nil {
		return err
	}
	if err := s.WaitForUpdate(ctx, version); err != nil {
		return fmt.Errorf("wait for local apply: %w", err)
	}
	return nil
}

// ChangeNamespaceState proposes a ChangeNamespaceState RAFT command. The
// apply side returns [namespaces.ErrNotFound] when the namespace does not
// exist and [namespaces.ErrInvalidStateTransition] when the transition is
// forbidden; same-state transitions are idempotent.
//
// On success the call blocks until the local node has applied the entry so
// subsequent local reads observe the new state.
func (s *Raft) ChangeNamespaceState(ctx context.Context, name string, target cmd.NamespaceState) error {
	req := cmd.ChangeNamespaceStateRequest{
		Name:        name,
		TargetState: target,
		Version:     cmd.NamespaceLatestCommandPolicyVersion,
	}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_CHANGE_NAMESPACE_STATE,
		SubCommand: subCommand,
	}
	version, err := s.Execute(ctx, command)
	if err != nil {
		return err
	}
	if err := s.WaitForUpdate(ctx, version); err != nil {
		return fmt.Errorf("wait for local apply: %w", err)
	}
	return nil
}

// RemoveNamespaceEntity proposes a RemoveNamespaceEntity RAFT command,
// removing the namespace map entry. The apply side returns
// [namespaces.ErrNotFound] when the namespace does not exist and
// [namespaces.ErrInvalidState] when called on an active namespace.
//
// On success the call blocks until the local node has applied the entry so
// subsequent local reads observe the namespace as gone.
func (s *Raft) RemoveNamespaceEntity(ctx context.Context, name string) error {
	req := cmd.RemoveNamespaceEntityRequest{
		Name:    name,
		Version: cmd.NamespaceLatestCommandPolicyVersion,
	}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_REMOVE_NAMESPACE_ENTITY,
		SubCommand: subCommand,
	}
	version, err := s.Execute(ctx, command)
	if err != nil {
		return err
	}
	if err := s.WaitForUpdate(ctx, version); err != nil {
		return fmt.Errorf("wait for local apply: %w", err)
	}
	return nil
}
