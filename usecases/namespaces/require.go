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

package namespaces

import (
	"fmt"
	"slices"
	"strings"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

// RequireActive returns nil for an empty name or an active namespace, and the
// error for the namespace's actual state otherwise. Pass "" only for an entity
// that belongs to no namespace, never for one whose namespace is unknown.
func RequireActive(e Exister, name string) error {
	if name == "" {
		return nil
	}
	ns, ok := e.GetNamespace(name)
	if !ok {
		return ErrNamespaceGone
	}
	return stateError(ns.State)
}

// AdmitDestructiveApply returns nil for an empty name and for the active and
// deleting states, and an error for every other state and for a missing
// namespace. Deleting must pass so the cleanup cascade can empty a namespace.
// A miss refuses rather than admits, so the answer does not rest on nothing
// being able to exist under a prefix naming no live namespace.
func AdmitDestructiveApply(e Exister, name string) error {
	if name == "" {
		return nil
	}
	ns, ok := e.GetNamespace(name)
	if !ok {
		return ErrNamespaceGone
	}
	switch ns.State {
	case cmd.NamespaceStateDeleting:
		return nil
	case cmd.NamespaceStateActive, cmd.NamespaceStateSuspended, cmd.NamespaceStateResuming:
		return stateError(ns.State)
	default:
		return ErrInvalidState
	}
}

// stateError returns nil for the active state and the sentinel for every other.
func stateError(state cmd.NamespaceState) error {
	switch state {
	case cmd.NamespaceStateActive:
		return nil
	case cmd.NamespaceStateSuspended:
		return ErrNamespaceSuspended
	case cmd.NamespaceStateResuming:
		return ErrNamespaceResuming
	case cmd.NamespaceStateDeleting:
		return ErrNamespaceDeleting
	default:
		return ErrInvalidState
	}
}

// RequireExisting returns nil for an empty name or a namespace that exists and
// is not deleting, and refuses anything else: rows written for a namespace that
// is gone or going would outlive every cleanup path. Restore uses this rather
// than RequireActive because a suspended namespace keeps its rows.
func RequireExisting(e Exister, name string) error {
	if name == "" {
		return nil
	}
	ns, ok := e.GetNamespace(name)
	if !ok {
		return ErrNamespaceGone
	}
	switch ns.State {
	case cmd.NamespaceStateActive, cmd.NamespaceStateSuspended, cmd.NamespaceStateResuming:
		return nil
	case cmd.NamespaceStateDeleting:
		return ErrNamespaceDeleting
	default:
		return ErrInvalidState
	}
}

// RequireAllExisting returns nil when every name passes
// [RequireExisting], otherwise one error naming every failing namespace and
// its state, sorted so the message is stable.
func RequireAllExisting(e Exister, names []string) error {
	var offenders []string
	for _, name := range names {
		if err := RequireExisting(e, name); err != nil {
			offenders = append(offenders, fmt.Sprintf("%q: %v", name, err))
		}
	}
	if len(offenders) == 0 {
		return nil
	}
	slices.Sort(offenders)
	return fmt.Errorf("namespace missing or deleting: %s", strings.Join(offenders, "; "))
}
