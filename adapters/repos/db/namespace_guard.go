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
	"sort"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/namespaces"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
	"github.com/weaviate/weaviate/usecases/sharding"
)

var (
	// errNamespaceRowMissing is returned when a class resolves to a namespace
	// that the namespace map does not hold. Every shard decision refuses on it.
	errNamespaceRowMissing = errors.New("namespace row missing for a locally-known class")

	// errNamespaceLookupMissing is returned when a namespaced class has no
	// namespace lookup to consult, which only a lost wiring line can produce.
	errNamespaceLookupMissing = errors.New("no namespace lookup for a namespaced class")

	// errShardNamespaceClosed is returned when the reopen path is asked for a
	// shard whose namespace no longer keeps any open.
	errShardNamespaceClosed = errors.New("namespace keeps no shards open")
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
	// callerReplication is a replica movement loading its target shard.
	// Suspending or resuming must not fail a movement already under way.
	callerReplication
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
		return "", refuseShardDecision(logger, namespace, class, errNamespaceLookupMissing)
	}
	ns, ok := e.GetNamespace(namespace)
	if !ok {
		return "", refuseShardDecision(logger, namespace, class, errNamespaceRowMissing)
	}
	return ns.State, nil
}

// desiredOpen reports whether a shard should be open. The status filter is the
// one initAndStoreShards applies at startup, so a shard reported open is one
// startup also registers. An empty status counts as HOT.
func desiredOpen(state api.NamespaceState, status string) bool {
	if !namespaces.ShardsShouldBeOpen(state) {
		return false
	}
	return schema.ActivityStatus(status) == models.TenantActivityStatusHOT
}

// DesiredOpenLocalShards returns the HOT shards this node should hold open,
// among those the sharding state lists it as a replica of. A single-tenant shard
// carries no status, which counts as HOT. A class in no namespace is decided as
// active.
//
// Being left out is not permission to unload. A replica movement holds its target
// shard on this node before it adds that shard to the sharding state, so the shard
// is missing here while still being wanted. A caller comparing this against the
// shards it holds must intersect with the listed replicas rather than unload
// everything this set omits.
func (db *DB) DesiredOpenLocalShards(className string) ([]string, error) {
	namespace := namespacing.NamespaceFromQualified(className)
	state, err := stateForShardDecision(db.namespacesExister, namespace, className, db.logger)
	if err != nil {
		return nil, err
	}
	if !namespaces.ShardsShouldBeOpen(state) {
		// Nothing is desired open, so the shards need not be enumerated.
		return nil, nil
	}

	var desired []string
	readErr := db.schemaReader.Read(className, true, func(_ *models.Class, shardingState *sharding.State) error {
		if shardingState == nil {
			// Returning no shards would read as "keep none open", which a sweep
			// would act on by unloading the class.
			return fmt.Errorf("no sharding state for class %q", className)
		}
		for name, physical := range shardingState.Physical {
			if shardingState.IsLocalShard(name) && desiredOpen(state, physical.Status) {
				desired = append(desired, name)
			}
		}
		return nil
	})
	if readErr != nil {
		return nil, readErr
	}
	// Map iteration order otherwise makes a sweep's logs and diffs unreproducible.
	sort.Strings(desired)
	return desired, nil
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
	case callerResume:
		if !namespaces.ShardsShouldBeOpen(state) {
			return errShardNamespaceClosed
		}
		return nil
	case callerReplication:
		return namespaces.AdmitReplicationTarget(state)
	}
	return errShardNamespaceClosed
}
