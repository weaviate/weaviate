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

	shardSet := make(map[string]struct{})
	for _, replica := range plan.ReplicaSet.Replicas {
		if _, ok := shardSet[replica.ShardName]; ok {
			continue
		}
		shardSet[replica.ShardName] = struct{}{}

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
