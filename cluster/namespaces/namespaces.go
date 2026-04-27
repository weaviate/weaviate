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

// Package namespaces implements the RAFT FSM adapter for cluster-level
// namespace control-plane state. The Manager owns the namespace map
// directly; current consumers (log-to-RAFT, snapshot, list-for-operator-API,
// count-for-startup-invariant) do not need a fast non-RAFT reader. When a
// later request-path consumer needs direct reads, the state here will be
// lifted into a dedicated controller and this Manager will shrink to a
// pass-through adapter.
package namespaces

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sync"

	"github.com/sirupsen/logrus"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

var (
	// ErrBadRequest signals a malformed RAFT command payload so the apply
	// path can classify it distinctly from legitimate-but-rejected
	// operations.
	ErrBadRequest = errors.New("bad request")

	// ErrAlreadyExists is returned by Add when a namespace with the given
	// name is already present. Callers that need a distinct status for
	// duplicates (e.g. an HTTP handler mapping to 409) should check with
	// errors.Is rather than string-matching the error message.
	ErrAlreadyExists = errors.New("namespace already exists")

	// ErrNotFound is returned by Delete when the target namespace does not
	// exist. Callers that need a distinct status for missing entries
	// (e.g. an HTTP handler mapping to 404) should check with errors.Is.
	ErrNotFound = errors.New("namespace not found")
)

// Name validation contract (kept tight so the operator API gives
// predictable results and so name-in-URL round-tripping is unambiguous):
//
//   - Must start with a lowercase ASCII letter.
//   - Subsequent characters are lowercase letters or digits.
//   - Length in [NameMinLength, NameMaxLength].
//   - Must not collide with a reserved name (see reservedNames).
//
// Reserved names are held back for platform/system use (e.g. a future
// "default" namespace or routing sentinels) and are refused at Add time.
const (
	NameMinLength = 3
	NameMaxLength = 36
)

var namespaceNameRegex = regexp.MustCompile(`^[a-z][a-z0-9]*$`)

// reservedNames are refused at Add time. Kept as a package variable (not a
// const) so tests can inspect it; not mutated at runtime.
var reservedNames = map[string]struct{}{
	"admin":    {},
	"system":   {},
	"default":  {},
	"internal": {},
	"weaviate": {},
	"global":   {},
	"public":   {},
}

// Manager is the RAFT FSM adapter for namespace control-plane state.
//
// Concurrency contract: hashicorp RAFT invokes Snapshot from a goroutine
// that may run concurrently with Apply, so the RLock inside Snapshot is
// load-bearing, not cosmetic. Applies are serialized by RAFT (write-lock
// semantics); queries take the read-lock. Do not remove the Snapshot
// RLock in a future refactor.
type Manager struct {
	mu         sync.RWMutex
	namespaces map[string]*cmd.Namespace
	logger     logrus.FieldLogger
}

// NewManager returns an empty, ready-to-use manager.
func NewManager(logger logrus.FieldLogger) *Manager {
	return &Manager{
		namespaces: make(map[string]*cmd.Namespace),
		logger:     logger,
	}
}

// Add applies an AddNamespace RAFT command. It rejects malformed payloads
// and invalid names with [ErrBadRequest], and duplicates with
// [ErrAlreadyExists].
func (m *Manager) Add(c *cmd.ApplyRequest) error {
	req := &cmd.AddNamespaceRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	if err := ValidateName(req.Namespace.Name); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.namespaces[req.Namespace.Name]; ok {
		return fmt.Errorf("%w: %q", ErrAlreadyExists, req.Namespace.Name)
	}

	ns := req.Namespace
	m.namespaces[ns.Name] = &ns
	return nil
}

// Delete applies a DeleteNamespace RAFT command. It returns [ErrNotFound]
// when the target namespace does not exist, and [ErrBadRequest] when the
// payload is malformed. Non-idempotent by design: callers that need
// idempotent semantics should swallow ErrNotFound at their layer.
func (m *Manager) Delete(c *cmd.ApplyRequest) error {
	req := &cmd.DeleteNamespaceRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.namespaces[req.Name]; !ok {
		return fmt.Errorf("%w: %q", ErrNotFound, req.Name)
	}
	delete(m.namespaces, req.Name)
	return nil
}

// Get handles a QueryGetNamespaces query. An empty Names slice returns all
// known namespaces; otherwise only the named ones that exist are returned
// (missing names are silently omitted).
func (m *Manager) Get(req *cmd.QueryRequest) ([]byte, error) {
	subCommand := cmd.QueryGetNamespacesRequest{}
	if err := json.Unmarshal(req.SubCommand, &subCommand); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	var out []cmd.Namespace
	if len(subCommand.Names) == 0 {
		out = make([]cmd.Namespace, 0, len(m.namespaces))
		for _, ns := range m.namespaces {
			out = append(out, *ns)
		}
	} else {
		out = make([]cmd.Namespace, 0, len(subCommand.Names))
		for _, name := range subCommand.Names {
			if ns, ok := m.namespaces[name]; ok {
				out = append(out, *ns)
			}
		}
	}

	payload, err := json.Marshal(cmd.QueryGetNamespacesResponse{Namespaces: out})
	if err != nil {
		return nil, fmt.Errorf("could not marshal query response: %w", err)
	}
	return payload, nil
}

// Count returns the number of known namespaces. Used by the startup
// invariant check. Reads local FSM state.
func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.namespaces)
}

// List returns a snapshot copy of all namespaces. Intended for callers that
// need to iterate without holding the lock.
func (m *Manager) List() []cmd.Namespace {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]cmd.Namespace, 0, len(m.namespaces))
	for _, ns := range m.namespaces {
		out = append(out, *ns)
	}
	return out
}

// Snapshot serializes the entire namespace map. See the Manager godoc for
// why the read lock is required even though Apply is single-threaded.
func (m *Manager) Snapshot() ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return json.Marshal(m.namespaces)
}

// Restore replaces the current state with the snapshot contents. A nil or
// empty snapshot leaves state empty (fresh bootstrap). Unknown JSON fields
// are tolerated to preserve forward-compatibility with future entity
// additions (e.g. a state field).
func (m *Manager) Restore(snapshot []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(snapshot) == 0 {
		m.namespaces = make(map[string]*cmd.Namespace)
		return nil
	}

	restored := make(map[string]*cmd.Namespace)
	if err := json.Unmarshal(snapshot, &restored); err != nil {
		m.logger.Errorf("restoring namespaces from snapshot failed with: %v", err)
		return err
	}
	m.namespaces = restored
	m.logger.Info("successfully restored namespaces from snapshot")
	return nil
}

// ValidateName enforces the package's naming contract. It is the single
// source of truth for namespace name validation and is called both from the
// REST handler (for fast 422 rejection without a RAFT round-trip) and from
// the apply path (as a defense-in-depth check).
func ValidateName(name string) error {
	if l := len(name); l < NameMinLength || l > NameMaxLength {
		return fmt.Errorf("namespace name %q must be %d-%d characters", name, NameMinLength, NameMaxLength)
	}
	if !namespaceNameRegex.MatchString(name) {
		return fmt.Errorf("namespace name %q must start with a lowercase letter and contain only lowercase letters and digits", name)
	}
	if _, reserved := reservedNames[name]; reserved {
		return fmt.Errorf("namespace name %q is reserved", name)
	}
	return nil
}
