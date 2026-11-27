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

package executor

import (
	"fmt"

	"github.com/weaviate/weaviate/cluster/router/types"
)

type Operation func(types.Replica) error

func validateExecutors(localOperation Operation, remoteOperation Operation) error {
	if localOperation == nil {
		return fmt.Errorf("local executor cannot be nil")
	}
	if remoteOperation == nil {
		return fmt.Errorf("remote executor cannot be nil")
	}
	return nil
}

func ExecuteForEachShard(plan types.ReadRoutingPlan, localOperation Operation, remoteOperation Operation) error {
	if err := validateExecutors(localOperation, remoteOperation); err != nil {
		return err
	}

	return forEachShard(plan, func(replica types.Replica, local bool) error {
		if local {
			if err := localOperation(replica); err != nil {
				return fmt.Errorf("failed to locally execute read plan on replica %s: %w", replica.NodeName, err)
			}
		} else {
			if err := remoteOperation(replica); err != nil {
				return fmt.Errorf("failed to remotely execute read plan on replica %s at addr %s: %w", replica.NodeName, replica.HostAddr, err)
			}
		}
		return nil
	})
}

// CountLocalShards returns how many local operations ExecuteForEachShard runs
// for plan.
func CountLocalShards(plan types.ReadRoutingPlan) int {
	n := 0
	_ = forEachShard(plan, func(_ types.Replica, local bool) error {
		if local {
			n++
		}
		return nil
	})
	return n
}

// forEachShard calls fn with the first replica of each shard in plan, and
// whether that replica is on this node, stopping at the first error.
func forEachShard(plan types.ReadRoutingPlan, fn func(replica types.Replica, local bool) error) error {
	shardSet := make(map[string]struct{})
	for _, replica := range plan.ReplicaSet.Replicas {
		if _, ok := shardSet[replica.ShardName]; ok {
			continue
		}
		shardSet[replica.ShardName] = struct{}{}

		if err := fn(replica, plan.LocalHostname == replica.NodeName); err != nil {
			return err
		}
	}
	return nil
}

func ExecuteForEachReplicaOfShard(plan types.ReadRoutingPlan, shardName string, localOperation Operation, remoteOperation Operation) error {
	if err := validateExecutors(localOperation, remoteOperation); err != nil {
		return err
	}

	for _, replica := range plan.ReplicaSet.Replicas {
		if replica.ShardName != shardName {
			continue
		}

		if plan.LocalHostname == replica.NodeName {
			if err := localOperation(replica); err != nil {
				return fmt.Errorf("failed to locally execute read plan on replica %s: %w", replica.NodeName, err)
			}
		} else {
			if err := remoteOperation(replica); err != nil {
				return fmt.Errorf("failed to remotely execute read plan on replica %s at addr %s: %w", replica.NodeName, replica.HostAddr, err)
			}
		}
	}
	return nil
}
