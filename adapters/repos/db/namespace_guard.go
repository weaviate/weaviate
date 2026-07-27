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
	"errors"

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
)

// refuseShardDecision logs why a shard decision is being refused and returns
// the reason, so both refusals name the class and the namespace the same way.
func refuseShardDecision(logger logrus.FieldLogger, namespace, class string, reason error) error {
	logger.WithFields(logrus.Fields{"class": class, "namespace": namespace}).
		Errorf("refusing shard materialization: %v", reason)
	return reason
}

// stateForShardDecision returns the namespace state a shard decision should use.
//
// An unqualified class name yields active: nothing can suspend a class that
// belongs to no namespace, and that is every class on a cluster running with
// namespaces off. Such a cluster therefore never reaches the lookup, so it never
// takes the node-wide read lock behind it. A returned error is already logged and
// every decision refuses on it — including a namespaced class with no lookup,
// which must not read as a namespace that happens to be active.
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
// this node. A refused lookup answers false.
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

// desiredOpen reports whether a shard should be open, given its namespace's
// state and, for a multi-tenant class, its tenant's activity status. Only
// tenants carry an activity status, so a single-tenant shard is decided by the
// namespace alone — the same filter the single-tenant reload applies. An empty
// status on a tenant normalizes to HOT, the way boot reads it.
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
// this node. A class in no namespace yields all its local HOT shards, which is
// the answer for a cluster that uses no namespaces.
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
			return nil
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
	return desired, nil
}

func (i *Index) shardsShouldBeOpen() bool {
	return shardsShouldBeOpen(i.namespacesExister, i.namespace, i.Config.ClassName.String(), i.logger)
}

func (i *Index) requireShardLoadable() error {
	return requireShardLoadable(i.namespacesExister, i.namespace, i.Config.ClassName.String(), i.logger)
}
