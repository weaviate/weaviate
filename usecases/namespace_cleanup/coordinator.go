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

// Package namespacecleanup deletes the contents of a namespace in the
// deleting state and then removes the namespace entry. Runs on the
// leader only.
package namespacecleanup

import (
	"context"
	"errors"
	"fmt"
	"strings"
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
	ClassesInNamespace(namespace string) []string
	AliasesInNamespace(namespace string) []string
}

// raftExecutor is the subset of cluster.Raft used here.
type raftExecutor interface {
	DeleteUsersInNamespace(name string) error
	DeleteAlias(ctx context.Context, alias string) (uint64, error)
	DeleteClass(ctx context.Context, name string) (uint64, error)
	RemoveNamespaceEntity(name string) error
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
// error stops the tick so the new leader can take over.
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
		if err := c.cleanupSingleNamespace(ctx, ns); err != nil {
			if errors.Is(err, types.ErrNotLeader) {
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
// ignored and the next tick retries.
func (c *Coordinator) cleanupSingleNamespace(ctx context.Context, ns string) error {
	if !c.isLeader() {
		return types.ErrNotLeader
	}
	if err := c.raft.DeleteUsersInNamespace(ns); err != nil {
		return err
	}
	// Aliases first: DeleteClass refuses to drop a class that still has
	// aliases pointing at it.
	for _, alias := range c.schema.AliasesInNamespace(ns) {
		if !c.isLeader() {
			return types.ErrNotLeader
		}
		if _, err := c.raft.DeleteAlias(ctx, alias); err != nil {
			return err
		}
	}
	for _, cls := range c.schema.ClassesInNamespace(ns) {
		if !c.isLeader() {
			return types.ErrNotLeader
		}
		if _, err := c.raft.DeleteClass(ctx, cls); err != nil {
			return err
		}
	}
	if !c.isLeader() {
		return types.ErrNotLeader
	}
	if err := c.raft.RemoveNamespaceEntity(ns); err != nil {
		// This can happen when an in-flight requests creates a new entity between cleanup start and here
		if isNamespaceNotEmpty(err) {
			return nil
		}
		return err
	}
	return nil
}

// isNamespaceNotEmpty returns true for ErrNamespaceNotEmpty, including
// the case where RAFT serialization stripped the wrapping and only the
// error message survives.
func isNamespaceNotEmpty(err error) bool {
	if errors.Is(err, namespaces.ErrNamespaceNotEmpty) {
		return true
	}
	return strings.Contains(err.Error(), namespaces.ErrNamespaceNotEmpty.Error())
}
