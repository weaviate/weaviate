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

// Package namespaces is the RAFT FSM adapter for namespace control-plane
// state. It translates *cmd.ApplyRequest / *cmd.QueryRequest payloads into
// typed calls on the shared controller, which owns the state.
package namespaces

import (
	"encoding/json"
	"fmt"

	"github.com/sirupsen/logrus"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	usecasesNamespaces "github.com/weaviate/weaviate/usecases/namespaces"
)

// Manager is the RAFT FSM adapter. It does not own state.
type Manager struct {
	controller *usecasesNamespaces.Controller
	logger     logrus.FieldLogger
}

// NewManager wraps the provided controller. A nil controller is a wiring
// bug — panic so a missing controller fails loudly at startup rather than
// letting the Count()==0 startup invariant pass against an uninitialized
// state map.
func NewManager(controller *usecasesNamespaces.Controller, logger logrus.FieldLogger) *Manager {
	if controller == nil {
		panic("cluster/namespaces: controller must not be nil")
	}
	return &Manager{controller: controller, logger: logger}
}

// Add applies an AddNamespace RAFT command. It rejects malformed payloads
// and invalid names with [usecasesNamespaces.ErrBadRequest], and duplicates
// with [usecasesNamespaces.ErrAlreadyExists].
func (m *Manager) Add(c *cmd.ApplyRequest) error {
	req := &cmd.AddNamespaceRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", usecasesNamespaces.ErrBadRequest, err)
	}
	return m.controller.Create(req.Namespace)
}

// Delete applies a DeleteNamespace RAFT command. It returns
// [usecasesNamespaces.ErrNotFound] when the target namespace does not
// exist, and [usecasesNamespaces.ErrBadRequest] when the payload is
// malformed.
func (m *Manager) Delete(c *cmd.ApplyRequest) error {
	req := &cmd.DeleteNamespaceRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", usecasesNamespaces.ErrBadRequest, err)
	}
	return m.controller.Delete(req.Name)
}

// Get handles a QueryGetNamespaces query. An empty Names slice returns all
// known namespaces; otherwise only the named ones that exist are returned
// (missing names are silently omitted).
func (m *Manager) Get(req *cmd.QueryRequest) ([]byte, error) {
	subCommand := cmd.QueryGetNamespacesRequest{}
	if err := json.Unmarshal(req.SubCommand, &subCommand); err != nil {
		return nil, fmt.Errorf("%w: %w", usecasesNamespaces.ErrBadRequest, err)
	}

	out := m.controller.Get(subCommand.Names...)
	payload, err := json.Marshal(cmd.QueryGetNamespacesResponse{Namespaces: out})
	if err != nil {
		return nil, fmt.Errorf("could not marshal query response: %w", err)
	}
	return payload, nil
}

// Count returns the number of known namespaces. Used by the startup
// invariant check.
func (m *Manager) Count() int {
	return m.controller.Count()
}

// List returns a snapshot copy of all namespaces.
func (m *Manager) List() []cmd.Namespace {
	return m.controller.List()
}

// Snapshot serializes the entire namespace map.
func (m *Manager) Snapshot() ([]byte, error) {
	return m.controller.Snapshot()
}

// Restore replaces the current state with the snapshot contents.
func (m *Manager) Restore(snapshot []byte) error {
	return m.controller.Restore(snapshot)
}
