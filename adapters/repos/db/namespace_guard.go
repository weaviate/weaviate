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
	// ErrNamespaceUnknownLocally is returned when a class resolves to a namespace
	// that the namespace map does not hold. Every shard decision refuses on it.
	ErrNamespaceUnknownLocally = errors.New("namespace not known on this node")

	// errNoNamespaceLookup is returned when a namespaced class has no namespace
	// lookup to consult, which only a lost wiring line can produce.
	errNoNamespaceLookup = errors.New("no namespace lookup for a namespaced class")

	// errShardNamespaceClosed is returned when a caller asks to load a shard of a
	// namespace that keeps none open.
	errShardNamespaceClosed = errors.New("namespace keeps no shards open")

	// errUnknownShardLoadCaller is returned for a caller the load decision has no
	// case for, so a wiring fault does not read as a namespace state.
	errUnknownShardLoadCaller = errors.New("unknown shard load caller")

	// errUnknownNamespaceState is returned for a namespace state that is not a key
	// of stateTransitions. readDesiredOpenLocalShards refuses on it rather than
	// reading it as a namespace that keeps none of its shards open.
	errUnknownNamespaceState = errors.New("unknown namespace state")

	// errNoShardingState is returned when the schema holds the class but carries
	// no sharding state for it. It is a fault, not a class going away.
	errNoShardingState = errors.New("no sharding state for class")
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

// stateForShardDecision returns the namespace state a shard decision should use.
// An empty namespace yields active — the shape every class carries on a cluster
// running with namespaces off — so such a cluster never reaches the lookup and
// never takes its read lock. Errors come back unlogged, and every decision refuses on one rather
// than reading it as an active namespace.
func stateForShardDecision(e namespaces.Exister, namespace string) (api.NamespaceState, error) {
	if namespace == "" {
		return api.NamespaceStateActive, nil
	}
	if e == nil {
		return "", errNoNamespaceLookup
	}
	ns, ok := e.GetNamespace(namespace)
	if !ok {
		return "", ErrNamespaceUnknownLocally
	}
	return ns.State, nil
}

// requireKnownNamespaceState returns nil when state is a key of stateTransitions.
// ShardsShouldBeOpen reads a state it has no case for as "keep none open", so an
// unknown one is refused here instead. A default: arm on that switch would end
// the lint that keeps the two in step, and this check would not stand in for it.
func requireKnownNamespaceState(state api.NamespaceState) error {
	if !namespaces.IsKnownState(state) {
		return fmt.Errorf("%w: %q", errUnknownNamespaceState, state)
	}
	return nil
}

// readDesiredOpenLocalShards calls use with the class's sharding state when the
// namespace keeps its shards open, and returns the namespace state. SchemaReader.Read
// retries, so use may run more than once, even when an error comes back. use runs
// under the class's schema read lock, so it must not block, do I/O or take a lock.
func (db *DB) readDesiredOpenLocalShards(className string, retryIfClassNotFound bool,
	use func(*sharding.State) error,
) (api.NamespaceState, error) {
	state, err := db.namespaceState(className)
	if err != nil {
		return "", err
	}
	if err := requireKnownNamespaceState(state); err != nil {
		return "", err
	}
	if !namespaces.ShardsShouldBeOpen(state) {
		// Nothing is desired open, so the shards need not be enumerated.
		return state, nil
	}

	if err := db.schemaReader.Read(className, retryIfClassNotFound,
		func(_ *models.Class, shardingState *sharding.State) error {
			if shardingState == nil {
				// Walking nothing would read as "keep none open", which a sweep would
				// act on by unloading the class.
				return fmt.Errorf("%w: %q", errNoShardingState, className)
			}
			return use(shardingState)
		}); err != nil {
		return "", err
	}
	return state, nil
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
	_, err := db.readDesiredOpenLocalShards(className, true, func(shardingState *sharding.State) error {
		for name, physical := range shardingState.Physical {
			if shardingState.IsLocalOpenPhysical(physical) {
				fn(name)
			}
		}
		return nil
	})
	return err
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

// DesiredOpenLocalShardNames returns the HOT shards this node should hold open
// for the class, as sharding-state map keys in map order, and the namespace
// state they were computed under. A class in no namespace is decided as active.
//
// An empty slice is an answer, and nil comes only with an error. A caller that
// diffs that nil against the shards it holds unloads the whole class. Of the
// errors only cluster/schema's ErrClassNotFound is a routine skip.
//
// A shard missing here may still be wanted. A replica movement holds its target
// before the sharding state lists it, and a namespace resumed after the read
// leaves this empty while the node holds every shard. Intersect with
// SchemaReader.LocalShards, which keys on Physical.Name rather than the map key,
// and re-read before acting on an empty set.
func (db *DB) DesiredOpenLocalShardNames(className string) ([]string, api.NamespaceState, error) {
	names := []string{}
	state, err := db.readDesiredOpenLocalShards(className, false, func(shardingState *sharding.State) error {
		// Assigned rather than appended per shard, so a retried read cannot
		// return each name twice.
		names = shardingState.AllLocalOpenPhysicalShards()
		return nil
	})
	if err != nil {
		return nil, "", err
	}
	return names, state, nil
}

// NamespaceStateForClass returns the state of the class's namespace, always one
// of the four constants or the zero value beside an error. It never reports a
// class as unknown — an unqualified name answers active, which reads as "keep
// the shards open" — so a consumer must resolve the class through
// GetLocalShardNames first. ErrNamespaceUnknownLocally is the one exported
// error: a diverged namespace map, to report rather than skip.
func (db *DB) NamespaceStateForClass(className string) (api.NamespaceState, error) {
	state, err := db.namespaceState(className)
	if err != nil {
		// Before the check, so a lookup refusal stays distinct from an
		// unrecognised state.
		return "", err
	}
	if err := requireKnownNamespaceState(state); err != nil {
		return "", err
	}
	return state, nil
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

// namespaceState binds a class name to the shared state lookup, deriving the
// namespace from the qualified name. The state is returned unvalidated.
func (db *DB) namespaceState(className string) (api.NamespaceState, error) {
	return stateForShardDecision(db.namespacesExister, namespacing.NamespaceFromQualified(className))
}

// namespaceState reads this index's namespace state and logs at Error when it
// cannot. loadLocalShardIfActive returns nil on that error, so without this
// line nothing records the refusal.
func (i *Index) namespaceState() (api.NamespaceState, error) {
	state, err := stateForShardDecision(i.namespacesExister, i.namespace)
	if err != nil {
		i.logger.WithFields(logrus.Fields{
			"class": i.Config.ClassName.String(), "namespace": i.namespace,
		}).Errorf("refusing shard materialization: %v", err)
	}
	return state, err
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
