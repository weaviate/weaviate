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
	"regexp"
	"sync"

	"github.com/sirupsen/logrus"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
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
// "default" namespace or routing sentinels) and are refused at Create time.
const (
	NameMinLength = 3
	NameMaxLength = 36
)

var namespaceNameRegex = regexp.MustCompile(`^[a-z][a-z0-9]*$`)

// reservedNames are refused at Create time. Kept as a package variable (not a
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

// Exister is the minimal namespace existence check consumers depend on for
// dependency injection. *Controller satisfies it. Defined here (alongside
// the producer) rather than on each consumer side so we can share a single
// generated mock across the apply-layer and REST handler test suites.
type Exister interface {
	Exists(name string) bool
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

// Create inserts a namespace. It rejects invalid names with [ErrBadRequest]
// and duplicates with [ErrAlreadyExists].
func (c *Controller) Create(ns cmd.Namespace) error {
	if err := ValidateName(ns.Name); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.namespaces[ns.Name]; ok {
		return fmt.Errorf("%w: %q", ErrAlreadyExists, ns.Name)
	}

	c.namespaces[ns.Name] = &ns
	return nil
}

// Delete removes a namespace. Returns [ErrNotFound] when the target
// namespace does not exist. Non-idempotent by design: callers that need
// idempotent semantics should swallow ErrNotFound at their layer.
func (c *Controller) Delete(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.namespaces[name]; !ok {
		return fmt.Errorf("%w: %q", ErrNotFound, name)
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

// Exists reports whether a namespace with the given name is known. Intended
// for non-cluster callers (REST handlers, OIDC claim resolution) that need
// a fast existence check without constructing a RAFT subcommand.
func (c *Controller) Exists(name string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.namespaces[name]
	return ok
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
// are tolerated to preserve forward-compatibility with future entity
// additions (e.g. a state field).
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
	c.namespaces = restored
	c.logger.Info("successfully restored namespaces from snapshot")
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
