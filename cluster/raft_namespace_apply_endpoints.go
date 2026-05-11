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
	"strings"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	usecasesNamespaces "github.com/weaviate/weaviate/usecases/namespaces"
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
func (s *Raft) AddNamespace(ns cmd.Namespace) error {
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
	version, err := s.Execute(context.Background(), command)
	if err != nil {
		return rewrapNamespaceApplyError(err)
	}
	if err := s.WaitForUpdate(context.Background(), version); err != nil {
		return fmt.Errorf("wait for local apply: %w", err)
	}
	return nil
}

// DeleteNamespace proposes a DeleteNamespace RAFT command. The apply side
// returns [namespaces.ErrNotFound] when the target namespace does not exist;
// callers that want idempotent semantics should swallow that error.
//
// On success the call blocks until the local node has applied the entry so
// subsequent local reads observe the namespace as gone.
func (s *Raft) DeleteNamespace(name string) error {
	req := cmd.DeleteNamespaceRequest{
		Name:    name,
		Version: cmd.NamespaceLatestCommandPolicyVersion,
	}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_DELETE_NAMESPACE,
		SubCommand: subCommand,
	}
	version, err := s.Execute(context.Background(), command)
	if err != nil {
		return rewrapNamespaceApplyError(err)
	}
	if err := s.WaitForUpdate(context.Background(), version); err != nil {
		return fmt.Errorf("wait for local apply: %w", err)
	}
	return nil
}

// rewrapNamespaceApplyError restores typed sentinels on FSM errors that
// flowed back through the follower→leader gRPC hop. The RPC layer serializes
// the error to a plain status string and so erases errors.Is chains. The
// handler matches usecasesNamespaces.Err* via errors.Is, so we string-match
// known sentinels and rewrap. Mirrors the pattern in
// cluster/raft_replication_apply_endpoints.go.
func rewrapNamespaceApplyError(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, usecasesNamespaces.ErrAlreadyExists.Error()):
		return fmt.Errorf("%w: %s", usecasesNamespaces.ErrAlreadyExists, msg)
	case strings.Contains(msg, usecasesNamespaces.ErrNotFound.Error()):
		return fmt.Errorf("%w: %s", usecasesNamespaces.ErrNotFound, msg)
	case strings.Contains(msg, usecasesNamespaces.ErrBadRequest.Error()):
		return fmt.Errorf("%w: %s", usecasesNamespaces.ErrBadRequest, msg)
	default:
		return err
	}
}
