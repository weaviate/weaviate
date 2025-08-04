//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package router

import (
	"fmt"

	"github.com/weaviate/weaviate/cluster/router/types"
)

func (r *singleTenantRouter) ExecuteForEachShard(plan types.ReadRoutingPlan, options types.ReadRoutingPlanExecutorOptions, localExecutor types.ReadExecutor, remoteExecutor types.ReadExecutor) error {
	return executeForEachShard(r.nodeSelector.LocalName(), plan, options, localExecutor, remoteExecutor)
}

func (r *multiTenantRouter) ExecuteForEachShard(plan types.ReadRoutingPlan, options types.ReadRoutingPlanExecutorOptions, localExecutor types.ReadExecutor, remoteExecutor types.ReadExecutor) error {
	return executeForEachShard(r.nodeSelector.LocalName(), plan, options, localExecutor, remoteExecutor)
}

func executeForEachShard(localName string, plan types.ReadRoutingPlan, options types.ReadRoutingPlanExecutorOptions, localExecutor types.ReadExecutor, remoteExecutor types.ReadExecutor) error {
	if localExecutor == nil {
		return fmt.Errorf("local executor cannot be nil")
	}
	if remoteExecutor == nil {
		return fmt.Errorf("remote executor cannot be nil")
	}

	shardSet := make(map[string]struct{})
	for _, replica := range plan.ReplicaSet.Replicas {
		if _, ok := shardSet[replica.ShardName]; ok {
			continue
		}
		shardSet[replica.ShardName] = struct{}{}

		if localName == replica.NodeName {
			if err := localExecutor(replica); err != nil && options.StopOnError {
				return fmt.Errorf("failed to locally execute read plan on replica %s: %w", replica.NodeName, err)
			}
		} else {
			if err := remoteExecutor(replica); err != nil && options.StopOnError {
				return fmt.Errorf("failed to remotely execute read plan on replica %s at addr %s: %w", replica.NodeName, replica.HostAddr, err)
			}
		}
	}
	return nil
}

func (r *singleTenantRouter) ExecuteForEachReplicaOfShard(plan types.ReadRoutingPlan, options types.ReadRoutingPlanExecutorOptions, shardName string, localExecutor types.ReadExecutor, remoteExecutor types.ReadExecutor) error {
	return executeForEachReplicaOfShard(r.nodeSelector.LocalName(), plan, options, shardName, localExecutor, remoteExecutor)
}

func (r *multiTenantRouter) ExecuteForEachReplicaOfShard(plan types.ReadRoutingPlan, options types.ReadRoutingPlanExecutorOptions, shardName string, localExecutor types.ReadExecutor, remoteExecutor types.ReadExecutor) error {
	return executeForEachReplicaOfShard(r.nodeSelector.LocalName(), plan, options, shardName, localExecutor, remoteExecutor)
}

func executeForEachReplicaOfShard(localName string, plan types.ReadRoutingPlan, options types.ReadRoutingPlanExecutorOptions, shardName string, localExecutor types.ReadExecutor, remoteExecutor types.ReadExecutor) error {
	if localExecutor == nil {
		return fmt.Errorf("local executor cannot be nil")
	}
	if remoteExecutor == nil {
		return fmt.Errorf("remote executor cannot be nil")
	}

	for _, replica := range plan.ReplicaSet.Replicas {
		if replica.ShardName != shardName {
			continue
		}

		if localName == replica.NodeName {
			if err := localExecutor(replica); err != nil && options.StopOnError {
				return fmt.Errorf("failed to locally execute read plan on replica %s: %w", replica.NodeName, err)
			}
		} else {
			if err := remoteExecutor(replica); err != nil && options.StopOnError {
				return fmt.Errorf("failed to remotely execute read plan on replica %s at addr %s: %w", replica.NodeName, replica.HostAddr, err)
			}
		}
	}
	return nil
}
