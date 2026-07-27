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
	"github.com/weaviate/weaviate/usecases/namespaces"
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

// namespaceStateForGuard resolves the namespace state a shard decision needs.
// namespaced=false with err=nil means the class carries no namespace, and the
// caller answers as it always would for one; err!=nil is already logged and
// every decision refuses on it. An unqualified name is the only way to be
// un-namespaced, so a namespaced class with no lookup refuses rather than
// reading as a namespace that happens to be active.
func namespaceStateForGuard(e namespaces.Exister, namespace, class string, logger logrus.FieldLogger) (state api.NamespaceState, namespaced bool, err error) {
	if namespace == "" {
		return "", false, nil
	}
	if e == nil {
		return "", false, refuseShardDecision(logger, namespace, class, errNamespaceLookupMissing)
	}
	ns, ok := e.GetNamespace(namespace)
	if !ok {
		return "", false, refuseShardDecision(logger, namespace, class, errNamespaceRowMissing)
	}
	return ns.State, true, nil
}

// shardsShouldBeOpen reports whether this class's shards may be held open on
// this node. A lookup miss answers false.
func shardsShouldBeOpen(e namespaces.Exister, namespace, class string, logger logrus.FieldLogger) bool {
	state, namespaced, err := namespaceStateForGuard(e, namespace, class, logger)
	if err != nil {
		return false
	}
	if !namespaced {
		return true
	}
	return namespaces.ShardsShouldBeOpen(state)
}

// requireShardLoadable returns nil when a request may load one of the class's
// shards. A lookup miss returns that error.
func requireShardLoadable(e namespaces.Exister, namespace, class string, logger logrus.FieldLogger) error {
	state, namespaced, err := namespaceStateForGuard(e, namespace, class, logger)
	if err != nil {
		return err
	}
	if !namespaced {
		return nil
	}
	return namespaces.RequireShardLoadable(state)
}

func (i *Index) shardsShouldBeOpen() bool {
	return shardsShouldBeOpen(i.namespacesExister, i.namespace, i.Config.ClassName.String(), i.logger)
}

func (i *Index) requireShardLoadable() error {
	return requireShardLoadable(i.namespacesExister, i.namespace, i.Config.ClassName.String(), i.logger)
}
