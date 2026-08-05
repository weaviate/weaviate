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

// The three functions below decide what may happen to a namespace's shards in a
// given state. No switch has a default: arm, so a new state fails the exhaustive
// linter; the return after each switch answers a state this binary doesn't know.

// ShardsShouldBeOpen reports whether this namespace's state allows a node to
// hold its shards open. A resuming namespace allows it: the shards have to
// reopen for the resume to finish.
func ShardsShouldBeOpen(state cmd.NamespaceState) bool {
	switch state {
	case cmd.NamespaceStateActive:
		return true
	case cmd.NamespaceStateSuspended:
		return false
	case cmd.NamespaceStateResuming:
		return true
	case cmd.NamespaceStateDeleting:
		return false
	}
	return false
}

// RequireShardLoadable returns nil when a request may load one of this
// namespace's shards. Resuming is refused even though its shards stay open: the
// namespace is not serving requests yet, so only the resume path may load them.
func RequireShardLoadable(state cmd.NamespaceState) error {
	return stateError(state)
}

// AdmitReplicationTarget returns nil in every state but deleting, so suspending
// or resuming a namespace does not fail a replica movement loading or writing to
// its target shard. It does not verify that a movement is under way, and it
// decides the target only: a movement that starts while suspended is still
// refused when it reads its source shard.
func AdmitReplicationTarget(state cmd.NamespaceState) error {
	switch state {
	case cmd.NamespaceStateActive, cmd.NamespaceStateSuspended, cmd.NamespaceStateResuming:
		return nil
	case cmd.NamespaceStateDeleting:
		return ErrNamespaceDeleting
	}
	return ErrInvalidState
}
