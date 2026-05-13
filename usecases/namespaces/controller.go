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

// Package namespaces owns the namespace control-plane state and exposes a
// typed domain API for callers that need direct existence checks without
// reaching for RAFT subcommand types.
package namespaces

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/sirupsen/logrus"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	entschema "github.com/weaviate/weaviate/entities/schema"
)

var (
	// ErrBadRequest signals a malformed RAFT command payload or an invalid
	// argument so the apply path can classify it distinctly from
	// legitimate-but-rejected operations.
	ErrBadRequest = errors.New("bad request")

	// ErrAlreadyExists is returned by Create when a namespace with the given
	// name is already present. Callers that need a distinct status for
	// duplicates (e.g. an HTTP handler mapping to 409) should check with
	// errors.Is rather than string-matching the error message.
	ErrAlreadyExists = errors.New("namespace already exists")

	// ErrNotFound is returned by ChangeState and RemoveEntity when the
	// target namespace does not exist. Callers that need a distinct status
	// for missing entries (e.g. an HTTP handler mapping to 404) should
	// check with errors.Is.
	ErrNotFound = errors.New("namespace not found")

	// ErrNamespaceDeleting is returned when a create-like operation targets
	// a namespace that exists but is currently being torn down. Distinct
	// from ErrAlreadyExists so REST can render a different conflict message.
	ErrNamespaceDeleting = errors.New("namespace is being deleted")

	// ErrNamespaceGone is returned by apply-time checks when a namespace
	// the caller validated earlier no longer exists.
	ErrNamespaceGone = errors.New("namespace no longer exists")

	// ErrNamespaceNotEmpty is returned by RemoveEntity at the apply layer
	// when the namespace still owns classes, aliases, or DB users.
	ErrNamespaceNotEmpty = errors.New("namespace still has owned resources")

	// ErrInvalidState is a defense-in-depth sentinel for operations called
	// on a namespace whose current state forbids them (e.g. RemoveEntity on
	// an active namespace).
	ErrInvalidState = errors.New("namespace is in an invalid state for this operation")

	// ErrInvalidStateTransition is returned by ChangeState when the target
	// state is unreachable from the namespace's current state.
	ErrInvalidStateTransition = errors.New("invalid namespace state transition")
)

// reservedNames are refused at Create time. Kept as a package variable (not a
// const) so tests can inspect it; not mutated at runtime. The lowercase/length
// regex lives in entities/schema.ValidateNamespaceNameSyntax so the
// name-resolver can reuse it without forming an import cycle.
var reservedNames = map[string]struct{}{
	"admin":    {},
	"system":   {},
	"default":  {},
	"internal": {},
	"weaviate": {},
	"global":   {},
	"public":   {},
}

// Exister exposes read-only access to namespace state. Exists matches any
// state; IsActive excludes the deleting state.
type Exister interface {
	Exists(name string) bool
	IsActive(name string) bool
	GetNamespace(name string) (cmd.Namespace, bool)
}

// Controller owns the namespace control-plane state.
//
// Concurrency contract: hashicorp RAFT invokes Snapshot from a goroutine
// that may run concurrently with Apply, so the RLock inside Snapshot is
// load-bearing, not cosmetic. Applies are serialized by RAFT (write-lock
// semantics); queries take the read-lock. Do not remove the Snapshot
// RLock in a future refactor.
type Controller struct {
	mu         sync.RWMutex
	namespaces map[string]*cmd.Namespace
	logger     logrus.FieldLogger
}

// NewController returns an empty, ready-to-use controller.
func NewController(logger logrus.FieldLogger) *Controller {
	return &Controller{
		namespaces: make(map[string]*cmd.Namespace),
		logger:     logger,
	}
}

// Create inserts a namespace in the [cmd.NamespaceStateActive] state; the
// input's State is ignored. Returns [ErrBadRequest] for invalid names or an
// empty HomeNode, [ErrAlreadyExists] when the name maps to an active
// namespace, and [ErrNamespaceDeleting] when the name is currently being
// torn down. HomeNode is rejected as empty here as defense in depth; the
// REST/RAFT request paths fill it before propose.
func (c *Controller) Create(ns cmd.Namespace) error {
	if err := ValidateName(ns.Name); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}
	if ns.HomeNode == "" {
		return fmt.Errorf("%w: home_node is required", ErrBadRequest)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if existing, ok := c.namespaces[ns.Name]; ok {
		if existing.State == cmd.NamespaceStateDeleting {
			return fmt.Errorf("%w: %q", ErrNamespaceDeleting, ns.Name)
		}
		return fmt.Errorf("%w: %q", ErrAlreadyExists, ns.Name)
	}

	ns.State = cmd.NamespaceStateActive
	c.namespaces[ns.Name] = &ns
	return nil
}

// Update overwrites the stored HomeNode for an existing namespace. Returns
// [ErrBadRequest] for an empty HomeNode and [ErrNotFound] when the
// namespace does not exist. Only HomeNode is mutable through this path;
// callers cannot use Update to change Name or State.
func (c *Controller) Update(ns cmd.Namespace) error {
	if ns.HomeNode == "" {
		return fmt.Errorf("%w: home_node is required", ErrBadRequest)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	existing, ok := c.namespaces[ns.Name]
	if !ok {
		return fmt.Errorf("%w: %q", ErrNotFound, ns.Name)
	}
	existing.HomeNode = ns.HomeNode
	return nil
}

// ChangeState transitions a namespace into target. Same-state transitions
// are idempotent and return nil. Returns [ErrBadRequest] when target is not
// a recognized state, [ErrNotFound] when the namespace does not exist, and
// [ErrInvalidStateTransition] when the transition is forbidden (e.g.
// deleting back to active).
func (c *Controller) ChangeState(name string, target cmd.NamespaceState) error {
	switch target {
	case cmd.NamespaceStateActive, cmd.NamespaceStateDeleting:
	default:
		return fmt.Errorf("%w: unknown namespace state %q", ErrBadRequest, target)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	ns, ok := c.namespaces[name]
	if !ok {
		return fmt.Errorf("%w: %q", ErrNotFound, name)
	}
	if ns.State == target {
		return nil
	}
	// deleting is terminal: re-entry only via RemoveEntity + fresh Create.
	if ns.State == cmd.NamespaceStateDeleting {
		return fmt.Errorf("%w: %q is %s, cannot transition to %s",
			ErrInvalidStateTransition, name, ns.State, target)
	}
	ns.State = target
	return nil
}

// RemoveEntity removes the namespace map entry. Callable only on a
// namespace already marked for deletion; an active namespace returns
// [ErrInvalidState]. Returns [ErrNotFound] when the namespace does not exist.
func (c *Controller) RemoveEntity(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	ns, ok := c.namespaces[name]
	if !ok {
		return fmt.Errorf("%w: %q", ErrNotFound, name)
	}
	if ns.State != cmd.NamespaceStateDeleting {
		return fmt.Errorf("%w: %q is not in deleting state", ErrInvalidState, name)
	}
	delete(c.namespaces, name)
	return nil
}

// Get returns the named namespaces. An empty Names slice returns all known
// namespaces; otherwise only the named ones that exist are returned (missing
// names are silently omitted).
func (c *Controller) Get(names ...string) []cmd.Namespace {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(names) == 0 {
		out := make([]cmd.Namespace, 0, len(c.namespaces))
		for _, ns := range c.namespaces {
			out = append(out, *ns)
		}
		return out
	}

	out := make([]cmd.Namespace, 0, len(names))
	for _, name := range names {
		if ns, ok := c.namespaces[name]; ok {
			out = append(out, *ns)
		}
	}
	return out
}

// List returns a snapshot copy of all namespaces. Intended for callers that
// need to iterate without holding the lock.
func (c *Controller) List() []cmd.Namespace {
	return c.Get()
}

// Count returns the number of known namespaces. Used by the startup
// invariant check.
func (c *Controller) Count() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.namespaces)
}

// Exists reports whether a namespace with the given name is known.
// Intended for non-cluster callers (REST handlers, OIDC claim resolution)
// that need a fast existence check without constructing a RAFT subcommand.
// Returns true for entries in any state.
func (c *Controller) Exists(name string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.namespaces[name]
	return ok
}

// GetNamespace returns the namespace by name. ok is false when the
// namespace does not exist. ns is a snapshot copy; mutating it does not
// affect controller state.
func (c *Controller) GetNamespace(name string) (ns cmd.Namespace, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	got, ok := c.namespaces[name]
	if !ok {
		return cmd.Namespace{}, false
	}
	return *got, true
}

// IsActive reports whether the named namespace exists and is in the
// [cmd.NamespaceStateActive] state.
func (c *Controller) IsActive(name string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ns, ok := c.namespaces[name]
	if !ok {
		return false
	}
	return ns.State == cmd.NamespaceStateActive
}

// ListDeleting returns the names of namespaces currently in the deleting
// state, sorted lexicographically.
func (c *Controller) ListDeleting() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]string, 0)
	for name, ns := range c.namespaces {
		if ns.State == cmd.NamespaceStateDeleting {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// Snapshot serializes the entire namespace map. See the Controller godoc for
// why the read lock is required even though Apply is single-threaded.
func (c *Controller) Snapshot() ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return json.Marshal(c.namespaces)
}

// Restore replaces the current state with the snapshot contents. A nil or
// empty snapshot leaves state empty (fresh bootstrap). Unknown JSON fields
// are tolerated. Entries with empty State are normalized to
// [cmd.NamespaceStateActive]; entries with an unknown State return an
// error so a future binary's snapshot is not silently mis-classified.
func (c *Controller) Restore(snapshot []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(snapshot) == 0 {
		c.namespaces = make(map[string]*cmd.Namespace)
		return nil
	}

	restored := make(map[string]*cmd.Namespace)
	if err := json.Unmarshal(snapshot, &restored); err != nil {
		c.logger.Errorf("restoring namespaces from snapshot failed with: %v", err)
		return err
	}
	for name, ns := range restored {
		if ns.State == "" {
			ns.State = cmd.NamespaceStateActive
			continue
		}
		switch ns.State {
		case cmd.NamespaceStateActive, cmd.NamespaceStateDeleting:
		default:
			return fmt.Errorf("namespace %q has unknown state %q in snapshot", name, ns.State)
		}
	}
	c.namespaces = restored
	c.logger.Info("successfully restored namespaces from snapshot")
	return nil
}

// ValidateName enforces the package's naming contract. It is the single
// source of truth for namespace name validation and is called both from the
// REST handler (for fast 422 rejection without a RAFT round-trip) and from
// the apply path (as a defense-in-depth check).
func ValidateName(name string) error {
	if err := entschema.ValidateNamespaceNameSyntax(name); err != nil {
		return err
	}
	if _, reserved := reservedNames[name]; reserved {
		return fmt.Errorf("namespace name %q is reserved", name)
	}
	return nil
}
