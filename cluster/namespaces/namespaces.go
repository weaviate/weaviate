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

// SchemaNamespaceLister returns the classes and aliases that belong to a
// namespace.
type SchemaNamespaceLister interface {
	ClassesInNamespace(namespace string) []string
	AliasesInNamespace(namespace string) []string
}

// DynusersNamespaceLister returns the dynamic DB users that belong to a
// namespace.
type DynusersNamespaceLister interface {
	UsersInNamespace(namespace string) []string
}

// Manager is the RAFT FSM adapter. It does not own state.
type Manager struct {
	controller *usecasesNamespaces.Controller
	schema     SchemaNamespaceLister
	dynusers   DynusersNamespaceLister
	logger     logrus.FieldLogger
}

// NewManager wraps the controller and the listers RemoveEntity consults
// to verify the namespace is empty. Panics on a nil controller or nil
// schema lister. The dynusers lister is optional: deployments without
// dynamic users pass nil and the user check is skipped.
func NewManager(
	controller *usecasesNamespaces.Controller,
	schema SchemaNamespaceLister,
	dynusers DynusersNamespaceLister,
	logger logrus.FieldLogger,
) *Manager {
	if controller == nil {
		panic("cluster/namespaces: controller must not be nil")
	}
	if schema == nil {
		panic("cluster/namespaces: schema lister must not be nil")
	}
	return &Manager{
		controller: controller,
		schema:     schema,
		dynusers:   dynusers,
		logger:     logger,
	}
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

// ChangeState applies a ChangeNamespaceState RAFT command, transitioning
// the namespace into the target state. Returns
// [usecasesNamespaces.ErrBadRequest] for malformed payloads or unknown
// target states, [usecasesNamespaces.ErrNotFound] when the namespace does
// not exist, and [usecasesNamespaces.ErrInvalidStateTransition] when the
// requested transition is forbidden.
func (m *Manager) ChangeState(c *cmd.ApplyRequest) error {
	req := &cmd.ChangeNamespaceStateRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", usecasesNamespaces.ErrBadRequest, err)
	}
	return m.controller.ChangeState(req.Name, req.TargetState)
}

// RemoveEntity applies a RemoveNamespaceEntity RAFT command. Returns
// [usecasesNamespaces.ErrBadRequest] for malformed payloads,
// [usecasesNamespaces.ErrNotFound] when the namespace does not exist,
// [usecasesNamespaces.ErrInvalidState] when called on an active namespace,
// and [usecasesNamespaces.ErrNamespaceNotEmpty] when classes, aliases, or
// users still remain in the namespace.
func (m *Manager) RemoveEntity(c *cmd.ApplyRequest) error {
	req := &cmd.RemoveNamespaceEntityRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", usecasesNamespaces.ErrBadRequest, err)
	}
	if classes := m.schema.ClassesInNamespace(req.Name); len(classes) > 0 {
		return fmt.Errorf("%w: %d class(es) remain in %q", usecasesNamespaces.ErrNamespaceNotEmpty, len(classes), req.Name)
	}
	if aliases := m.schema.AliasesInNamespace(req.Name); len(aliases) > 0 {
		return fmt.Errorf("%w: %d alias(es) remain in %q", usecasesNamespaces.ErrNamespaceNotEmpty, len(aliases), req.Name)
	}
	if m.dynusers != nil {
		if users := m.dynusers.UsersInNamespace(req.Name); len(users) > 0 {
			return fmt.Errorf("%w: %d user(s) remain in %q", usecasesNamespaces.ErrNamespaceNotEmpty, len(users), req.Name)
		}
	}
	return m.controller.RemoveEntity(req.Name)
}

// Exists proxies to the controller so the apply switch can satisfy
// [usecasesNamespaces.Exister] with a single namespaceManager reference
// instead of threading the controller through every call site.
func (m *Manager) Exists(name string) bool {
	return m.controller.Exists(name)
}

// IsActive proxies to the controller. Same rationale as Exists.
func (m *Manager) IsActive(name string) bool {
	return m.controller.IsActive(name)
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
