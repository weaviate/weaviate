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
	if _, err := s.Execute(context.Background(), command); err != nil {
		return err
	}
	return nil
}

// DeleteNamespace proposes a DeleteNamespace RAFT command. The apply side
// returns [namespaces.ErrNotFound] when the target namespace does not exist;
// callers that want idempotent semantics should swallow that error.
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
	if _, err := s.Execute(context.Background(), command); err != nil {
		return err
	}
	return nil
}
