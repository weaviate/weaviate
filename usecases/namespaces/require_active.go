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
// deleting states. Every other state, and a missing namespace, gets an error.
// Deleting must pass so the cleanup cascade can empty a namespace.
//
// A miss refuses like its two siblings rather than admitting, so the answer
// does not depend on nothing being able to exist under a prefix naming no live
// namespace. That holds today by way of three separate rules — RequireActive
// refuses a create under an unknown prefix, RemoveEntity refuses while any
// class, alias, user or RBAC row remains, and the live delete matches the class
// name exactly — and admitting a miss would silently stop guarding destructive
// commands if any of them changed.
func AdmitDestructiveApply(e Exister, name string) error {
	if name == "" {
		return nil
	}
	ns, ok := e.GetNamespace(name)
	if !ok {
		return ErrNamespaceGone
	}
	// No default: arm, so a new state fails the exhaustive linter here.
	switch ns.State {
	case cmd.NamespaceStateDeleting:
		return nil
	case cmd.NamespaceStateActive, cmd.NamespaceStateSuspended, cmd.NamespaceStateResuming:
		return stateError(ns.State)
	}
	return ErrInvalidState
}

// stateError returns nil for the active state and the sentinel for every other.
// No default: arm, so a new state fails the exhaustive linter until decided
// here; the trailing return answers a state this binary doesn't know.
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
	}
	return ErrInvalidState
}
