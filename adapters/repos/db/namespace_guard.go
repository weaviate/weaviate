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

// shardsShouldBeOpen reports whether this class's shards may be held open on
// this node. Any error answers false.
func shardsShouldBeOpen(e namespaces.Exister, namespace, class string, logger logrus.FieldLogger) bool {
	state, err := stateForShardDecision(e, namespace, class, logger)
	if err != nil {
		return false
	}
	return namespaces.ShardsShouldBeOpen(state)
}

// requireShardLoadable returns nil when a request may load one of the class's
// shards.
func requireShardLoadable(e namespaces.Exister, namespace, class string, logger logrus.FieldLogger) error {
	state, err := stateForShardDecision(e, namespace, class, logger)
	if err != nil {
		return err
	}
	return namespaces.RequireShardLoadable(state)
}

// admitReplicationTarget returns nil when a replica movement may load one of the
// class's shards.
func admitReplicationTarget(e namespaces.Exister, namespace, class string, logger logrus.FieldLogger) error {
	state, err := stateForShardDecision(e, namespace, class, logger)
	if err != nil {
		return err
	}
	return namespaces.AdmitReplicationTarget(state)
}

// desiredOpen reports whether a shard should be open, given its namespace's
// state and, for a multi-tenant class, its tenant's activity status. Only
// tenants carry an activity status, so a single-tenant shard is decided by the
// namespace alone, the way the single-tenant reload also applies no status
// filter. An empty tenant status counts as HOT.
func desiredOpen(state api.NamespaceState, partitioningEnabled bool, tenantStatus string) bool {
	if !namespaces.ShardsShouldBeOpen(state) {
		return false
	}
	if !partitioningEnabled {
		return true
	}
	return schema.ActivityStatus(tenantStatus) == models.TenantActivityStatusHOT
}

// DesiredOpenLocalShards returns the class's local shards that should be open on
// this node: the HOT tenants of a multi-tenant class, or every local shard of a
// single-tenant one. A class in no namespace is decided as active.
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
			if shardingState.IsLocalShard(name) &&
				desiredOpen(state, shardingState.PartitioningEnabled, physical.Status) {
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

func (i *Index) shardsShouldBeOpen() bool {
	return shardsShouldBeOpen(i.namespacesExister, i.namespace, i.Config.ClassName.String(), i.logger)
}

func (i *Index) requireShardLoadable() error {
	return requireShardLoadable(i.namespacesExister, i.namespace, i.Config.ClassName.String(), i.logger)
}

func (i *Index) admitReplicationTarget() error {
	return admitReplicationTarget(i.namespacesExister, i.namespace, i.Config.ClassName.String(), i.logger)
}

// requireNamespaceAllowsShardLoad returns nil when the namespace's state lets
// this caller load a shard. A caller with no case below is refused.
func (i *Index) requireNamespaceAllowsShardLoad(caller shardLoadCaller) error {
	switch caller {
	case callerUserRequest:
		return i.requireShardLoadable()
	case callerResume:
		if !i.shardsShouldBeOpen() {
			return errShardNamespaceClosed
		}
		return nil
	case callerReplication:
		return i.admitReplicationTarget()
	}
	return errShardNamespaceClosed
}
