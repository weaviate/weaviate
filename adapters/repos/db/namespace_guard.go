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

package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/namespaces"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
	"github.com/weaviate/weaviate/usecases/sharding"
)

var (
	// errNamespaceUnknownLocally is returned when a class resolves to a namespace
	// that the namespace map does not hold. Every shard decision refuses on it.
	errNamespaceUnknownLocally = errors.New("namespace not known on this node")

	// errNoNamespaceLookup is returned when a namespaced class has no namespace
	// lookup to consult, which only a lost wiring line can produce.
	errNoNamespaceLookup = errors.New("no namespace lookup for a namespaced class")

	// errShardNamespaceClosed is returned when a caller asks to load a shard of a
	// namespace that keeps none open.
	errShardNamespaceClosed = errors.New("namespace keeps no shards open")

	// errUnknownShardLoadCaller is returned for a caller the load decision has no
	// case for, so a wiring fault does not read as a namespace state.
	errUnknownShardLoadCaller = errors.New("unknown shard load caller")
)

// shardLoadCaller says who wants a shard loaded. Each caller gets a different
// part of the namespace check.
type shardLoadCaller int

const (
	// callerUserRequest is a load driven by a user request. It gets the full check.
	callerUserRequest shardLoadCaller = iota
	// callerResume is a resuming namespace reopening its own shards. It is still
	// refused for a namespace that keeps no shards open, so a stale reopen cannot
	// revive a suspended one.
	callerResume
	// callerMovement is a replica movement loading its target shard.
	// Suspending or resuming must not fail a movement already under way.
	callerMovement
	// callerNewReplica is the apply that records this node as a replica of a
	// shard, with no movement under way.
	callerNewReplica
	// callerTenantProcess is the apply that records a finished offload or onload.
	callerTenantProcess
	// callerTenantAdd is the apply that records a new HOT tenant.
	callerTenantAdd
	// callerTenantActivation is the apply that records a tenant turning HOT,
	// whether the user asked for it or a write on a cold tenant did.
	callerTenantActivation
	// callerReload is the reload replaying committed schema. It is decided by
	// ShardsShouldBeOpen rather than RequireShardLoadable, so a resuming
	// namespace's shards reopen instead of being refused.
	callerReload
)

// refuseShardDecision logs why a shard decision is being refused and returns
// the reason, so both refusals name the class and the namespace the same way.
func refuseShardDecision(logger logrus.FieldLogger, namespace, class string, reason error) error {
	logger.WithFields(logrus.Fields{"class": class, "namespace": namespace}).
		Errorf("refusing shard materialization: %v", reason)
	return reason
}

// stateForShardDecision returns the namespace state a shard decision should use.
// An unqualified class name yields active — every class on a cluster running with
// namespaces off — so such a cluster never reaches the lookup and never takes its
// read lock. A returned error is already logged, and every decision refuses on it
// rather than reading as an active namespace.
func stateForShardDecision(e namespaces.Exister, namespace, class string, logger logrus.FieldLogger) (api.NamespaceState, error) {
	if namespace == "" {
		return api.NamespaceStateActive, nil
	}
	if e == nil {
		return "", refuseShardDecision(logger, namespace, class, errNoNamespaceLookup)
	}
	ns, ok := e.GetNamespace(namespace)
	if !ok {
		return "", refuseShardDecision(logger, namespace, class, errNamespaceUnknownLocally)
	}
	return ns.State, nil
}

// shardStatusOpen reports whether an activity status should keep its shard open.
// It is the filter initAndStoreShards applies at startup, so a shard reported
// open is one startup also registers. An empty status counts as HOT.
//
// The namespace state is not consulted here. forEachDesiredOpenLocalShard gates
// on it once before it enumerates, so a large tenant set pays that check once
// rather than per shard.
func shardStatusOpen(status string) bool {
	return schema.ActivityStatus(status) == models.TenantActivityStatusHOT
}

// forEachDesiredOpenLocalShard calls fn for each HOT shard this node should hold
// open, among those the sharding state lists it as a replica of. A single-tenant
// shard carries no status, which counts as HOT. A class in no namespace is
// decided as active.
//
// fn is called in map order, so a caller whose output has to be reproducible
// must sort what it collects. It runs under the class's schema read lock, which
// every schema apply for that class waits behind, so it must not block, do I/O,
// or take another lock.
//
// A shard left out is not one that may be unloaded. A replica movement holds its
// target shard on this node before it adds that shard to the sharding state, so
// the shard is missing here while still being wanted. A caller comparing this
// against the shards it holds must intersect with the listed replicas rather
// than unload everything this omits.
func (db *DB) forEachDesiredOpenLocalShard(className string, fn func(name string)) error {
	namespace := namespacing.NamespaceFromQualified(className)
	state, err := stateForShardDecision(db.namespacesExister, namespace, className, db.logger)
	if err != nil {
		return err
	}
	if !namespaces.ShardsShouldBeOpen(state) {
		// Nothing is desired open, so the shards need not be enumerated.
		return nil
	}

	return db.schemaReader.Read(className, true, func(_ *models.Class, shardingState *sharding.State) error {
		if shardingState == nil {
			// Walking nothing would read as "keep none open", which a sweep would
			// act on by unloading the class.
			return fmt.Errorf("no sharding state for class %q", className)
		}
		for name, physical := range shardingState.Physical {
			if shardingState.IsLocalPhysical(physical) && shardStatusOpen(physical.Status) {
				fn(name)
			}
		}
		return nil
	})
}

// DesiredOpenLocalShardCount returns how many HOT shards this node should hold
// open for the class. Startup progress polls it per class on a ticker, so it
// counts in place rather than materializing the names.
func (db *DB) DesiredOpenLocalShardCount(className string) (int, error) {
	var count int
	if err := db.forEachDesiredOpenLocalShard(className, func(string) { count++ }); err != nil {
		return 0, err
	}
	return count, nil
}

// ReopenShard loads a shard on behalf of a resuming namespace, which the request
// path refuses to do while the namespace comes back. The shard is loaded outright
// rather than registered lazily, since no request would come along to load it. A
// namespace that keeps no shards open is still refused, so a stale reopen cannot
// revive a suspended one.
func (db *DB) ReopenShard(ctx context.Context, className, shardName string) error {
	index := db.GetIndex(schema.ClassName(className))
	if index == nil {
		return fmt.Errorf("index for class %q not found locally", className)
	}
	return index.initLocalShardWithForcedLoading(ctx, index.getClass(), shardName, true, false, callerResume)
}

// namespaceState binds this index's own namespace to the shared state lookup.
func (i *Index) namespaceState() (api.NamespaceState, error) {
	return stateForShardDecision(i.namespacesExister, i.namespace, i.Config.ClassName.String(), i.logger)
}

// requireNamespaceAllowsShardLoad returns nil when the namespace's state lets
// this caller load a shard. A caller with no case below is refused.
func (i *Index) requireNamespaceAllowsShardLoad(caller shardLoadCaller) error {
	state, err := i.namespaceState()
	if err != nil {
		return err
	}
	switch caller {
	case callerUserRequest:
		return namespaces.RequireShardLoadable(state)
	case callerResume, callerReload:
		if !namespaces.ShardsShouldBeOpen(state) {
			return errShardNamespaceClosed
		}
		return nil
	case callerNewReplica, callerTenantProcess, callerTenantAdd, callerTenantActivation:
		// Each records a shard this node owns before it opens it, so
		// AppliedChangeMayOpenShard says why a suspended namespace opens it.
		if !namespaces.AppliedChangeMayOpenShard(state) {
			return errShardNamespaceClosed
		}
		return nil
	case callerMovement:
		return namespaces.AdmitReplicationTarget(state)
	}
	return errUnknownShardLoadCaller
}
