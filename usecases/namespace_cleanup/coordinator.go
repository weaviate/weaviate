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
// The user delete is re-run as a safety net whenever leftover users remain,
// in case the handler crashed between marking and the eager drain.
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
	"github.com/weaviate/weaviate/usecases/auth/authentication"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac"
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

// userLister returns the DB users bound to a namespace.
type userLister interface {
	UsersInNamespace(namespace string) []string
}

// raftExecutor is the subset of cluster.Raft used here. The RBAC writes
// replicate through RAFT like the rest, so a follower's store is cleaned too.
type raftExecutor interface {
	DeleteUsersInNamespace(ctx context.Context, name string) error
	DeleteAlias(ctx context.Context, alias string) (uint64, error)
	DeleteClass(ctx context.Context, name string) (uint64, error)
	RemoveNamespaceEntity(ctx context.Context, name string) (uint64, error)
	DeleteRoles(names ...string) error
	RevokeRolesForUser(user string, roles ...string) error
}

// RBACLister reads the roles and role assignments that belong to a namespace.
// Nil when RBAC is disabled (only possible on a non-namespace cluster, which has
// nothing to clean). Reads run on the leader; the deletes go through raftExecutor.
type RBACLister interface {
	NamespaceLocalRBAC(namespace string) (roles []string, subjects []rbac.NamespaceSubject, err error)
	GetRolesForUserOrGroup(user string, authMethod authentication.AuthType, isGroup bool) (map[string][]authorization.Policy, error)
}

// Coordinator runs one cleanup pass per Tick on the leader. isLeader is
// re-checked before every RAFT write so the old leader stops issuing
// writes once leadership moves.
type Coordinator struct {
	namespaces namespaceLister
	schema     schemaLister
	users      userLister
	raft       raftExecutor
	rbac       RBACLister
	isLeader   func() bool
	logger     logrus.FieldLogger
	ongoing    atomic.Bool
}

func NewCoordinator(
	nsLister namespaceLister,
	schema schemaLister,
	users userLister,
	raft raftExecutor,
	rbac RBACLister,
	isLeader func() bool,
	logger logrus.FieldLogger,
) *Coordinator {
	if nsLister == nil {
		panic("namespacecleanup: namespace lister must not be nil")
	}
	if schema == nil {
		panic("namespacecleanup: schema lister must not be nil")
	}
	if users == nil {
		panic("namespacecleanup: user lister must not be nil")
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
		users:      users,
		raft:       raft,
		rbac:       rbac,
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
	for _, namespace := range c.namespaces.ListDeleting() {
		if ctx.Err() != nil {
			return nil
		}
		if err := c.cleanupSingleNamespace(ctx, namespace); err != nil {
			if errors.Is(err, types.ErrNotLeader) || ctx.Err() != nil {
				return nil
			}
			c.logger.WithField("namespace", namespace).Error(err)
		}
	}
	return nil
}

// cleanupSingleNamespace deletes the users, aliases, and classes (in this order) that
// belong to namespace, then removes the namespace entry. If the entry is not
// empty when RemoveNamespaceEntity reaches the apply path, the error is
// ignored and the next tick retries. ctx is re-checked before every RAFT
// write so a long cleanup stops on shutdown.
func (c *Coordinator) cleanupSingleNamespace(ctx context.Context, namespace string) error {
	if !c.isLeader() {
		return types.ErrNotLeader
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Skip the no-op RAFT entry when no users remain to redrain.
	if len(c.users.UsersInNamespace(namespace)) > 0 {
		if err := c.raft.DeleteUsersInNamespace(ctx, namespace); err != nil {
			return err
		}
	}
	for _, alias := range c.schema.AliasesInNamespace(namespace) {
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
	classes, err := c.schema.ClassesInNamespace(namespace)
	if err != nil {
		return fmt.Errorf("list classes in namespace %q: %w", namespace, err)
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
	if err := c.cleanupNamespaceRBAC(ctx, namespace); err != nil {
		return err
	}
	if !c.isLeader() {
		return types.ErrNotLeader
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := c.raft.RemoveNamespaceEntity(ctx, namespace); err != nil {
		// Apply re-checks emptiness against authoritative state; the
		// next tick retries if anything still owns the namespace.
		if errors.Is(err, namespaces.ErrNamespaceNotEmpty) {
			return nil
		}
		return err
	}
	return nil
}

// cleanupNamespaceRBAC removes the namespace's RBAC state: first the role
// assignments of its direct principals (which may also hold a global role),
// then the local roles themselves. Revocations run before role deletes so no
// assignment outlives its role.
//
// The assign layer only lets a local role reach its own namespace's principals,
// all of which are revoked above, so DeleteRoles dropping any grouping row that
// still points at a deleted local role is a defensive backstop. No-op when RBAC
// is disabled (see RBACLister).
func (c *Coordinator) cleanupNamespaceRBAC(ctx context.Context, namespace string) error {
	if c.rbac == nil {
		return nil
	}
	localRoles, subjects, err := c.rbac.NamespaceLocalRBAC(namespace)
	if err != nil {
		return fmt.Errorf("list namespace-local RBAC: %w", err)
	}
	for _, subject := range subjects {
		if !c.isLeader() {
			return types.ErrNotLeader
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		roles, err := c.rbac.GetRolesForUserOrGroup(subject.ID, subject.AuthType, false)
		if err != nil {
			return fmt.Errorf("list roles for %q: %w", subject.ID, err)
		}
		if len(roles) == 0 {
			continue
		}
		roleNames := make([]string, 0, len(roles))
		for name := range roles {
			roleNames = append(roleNames, name)
		}
		if err := c.raft.RevokeRolesForUser(conv.UserNameWithTypeFromId(subject.ID, subject.AuthType), roleNames...); err != nil {
			return fmt.Errorf("revoke roles from %q: %w", subject.ID, err)
		}
	}

	if len(localRoles) > 0 {
		if !c.isLeader() {
			return types.ErrNotLeader
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := c.raft.DeleteRoles(localRoles...); err != nil {
			return fmt.Errorf("delete local roles: %w", err)
		}
	}
	return nil
}
