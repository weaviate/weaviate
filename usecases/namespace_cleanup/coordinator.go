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

// Package namespacecleanup runs the leader-side cascade that empties a
// namespace once it has been marked for deletion.
//
// Deletion is split into a fast synchronous half and a slow
// asynchronous half. The DELETE handler flips the namespace to
// deleting, drains its DB users, and returns 202. This package then
// removes the rest on a periodic tick: aliases, then classes, then
// the namespace entry itself. Aliases come before classes because an
// alias points at a class. Class deletion is the slow part, which is
// why it lives here rather than in the request path.
//
// The split is driven by two concerns. First, class deletion can run
// long on large collections and would otherwise risk request
// timeouts. Second, a node can crash mid-delete at any time, so the
// cluster needs an after-the-fact cleanup path regardless; once that
// path exists, routing the bulk of the work through it is cheaper
// than duplicating the logic in the request handler.
//
// The user delete is re-run on every tick as a safety net in case the
// handler crashed between marking and the eager drain.
//
// Every step is a separate replicated command, so any failure is
// retried on the next tick. Because the coordinator's view of what
// still belongs to a namespace can lag behind the leader, the final
// entity removal is re-checked at apply time and rejected if anything
// still owns the namespace; the next tick retries.
package namespacecleanup

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/usecases/namespaces"
)

// namespaceLister lists namespaces in the deleting state.
type namespaceLister interface {
	ListDeleting() []string
}

// schemaLister returns the classes and aliases that belong to a namespace.
type schemaLister interface {
	ClassesInNamespace(namespace string) ([]string, error)
	AliasesInNamespace(namespace string) []string
}

// raftExecutor is the subset of cluster.Raft used here.
type raftExecutor interface {
	DeleteUsersInNamespace(ctx context.Context, name string) error
	DeleteAlias(ctx context.Context, alias string) (uint64, error)
	DeleteClass(ctx context.Context, name string) (uint64, error)
	RemoveNamespaceEntity(ctx context.Context, name string) (uint64, error)
}

// Coordinator runs one cleanup pass per Tick on the leader. isLeader is
// re-checked before every RAFT write so the old leader stops issuing
// writes once leadership moves.
type Coordinator struct {
	namespaces namespaceLister
	schema     schemaLister
	raft       raftExecutor
	isLeader   func() bool
	logger     logrus.FieldLogger
	ongoing    atomic.Bool
}

func NewCoordinator(
	nsLister namespaceLister,
	schema schemaLister,
	raft raftExecutor,
	isLeader func() bool,
	logger logrus.FieldLogger,
) *Coordinator {
	if nsLister == nil {
		panic("namespacecleanup: namespace lister must not be nil")
	}
	if schema == nil {
		panic("namespacecleanup: schema lister must not be nil")
	}
	if raft == nil {
		panic("namespacecleanup: raft executor must not be nil")
	}
	if isLeader == nil {
		panic("namespacecleanup: isLeader must not be nil")
	}
	return &Coordinator{
		namespaces: nsLister,
		schema:     schema,
		raft:       raft,
		isLeader:   isLeader,
		logger:     logger,
	}
}

// Tick cleans up every namespace currently in the deleting state. A
// per-namespace error is logged and the loop moves on; a not-leader
// error stops the tick so the new leader can take over. A cancelled ctx
// (server shutdown) stops the tick cleanly between namespaces and mid
// cleanup; the next tick resumes the rest.
//
// Returns an error if another Tick is already running.
func (c *Coordinator) Tick(ctx context.Context) error {
	if !c.ongoing.CompareAndSwap(false, true) {
		return fmt.Errorf("namespace cleanup already ongoing")
	}
	defer c.ongoing.Store(false)

	if !c.isLeader() {
		return nil
	}
	for _, ns := range c.namespaces.ListDeleting() {
		if ctx.Err() != nil {
			return nil
		}
		if err := c.cleanupSingleNamespace(ctx, ns); err != nil {
			if errors.Is(err, types.ErrNotLeader) || ctx.Err() != nil {
				return nil
			}
			c.logger.WithField("namespace", ns).Error(err)
		}
	}
	return nil
}

// cleanupSingleNamespace deletes the users, aliases, and classes (in this order) that
// belong to ns, then removes the namespace entry. If the entry is not
// empty when RemoveNamespaceEntity reaches the apply path, the error is
// ignored and the next tick retries. ctx is re-checked before every RAFT
// write so a long cleanup stops on shutdown.
func (c *Coordinator) cleanupSingleNamespace(ctx context.Context, ns string) error {
	if !c.isLeader() {
		return types.ErrNotLeader
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.raft.DeleteUsersInNamespace(ctx, ns); err != nil {
		return err
	}
	for _, alias := range c.schema.AliasesInNamespace(ns) {
		if !c.isLeader() {
			return types.ErrNotLeader
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := c.raft.DeleteAlias(ctx, alias); err != nil {
			return err
		}
	}
	classes, err := c.schema.ClassesInNamespace(ns)
	if err != nil {
		return fmt.Errorf("list classes in namespace %q: %w", ns, err)
	}
	for _, cls := range classes {
		if !c.isLeader() {
			return types.ErrNotLeader
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := c.raft.DeleteClass(ctx, cls); err != nil {
			return err
		}
	}
	if !c.isLeader() {
		return types.ErrNotLeader
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := c.raft.RemoveNamespaceEntity(ctx, ns); err != nil {
		// Apply re-checks emptiness against authoritative state; the
		// next tick retries if anything still owns the namespace.
		if errors.Is(err, namespaces.ErrNamespaceNotEmpty) {
			return nil
		}
		return err
	}
	return nil
}
