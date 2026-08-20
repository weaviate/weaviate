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
	switch ns.State {
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
