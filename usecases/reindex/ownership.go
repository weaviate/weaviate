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

package reindex

import (
	"context"
	"fmt"
	"sort"

	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// ShardOwnershipReader is defined here (consumer side) so tests can
// substitute fakes without standing up a real DB.
type ShardOwnershipReader interface {
	ShardReplicaOwnership(ctx context.Context, className string) (map[string][]string, error)
	ShardReplicaOwnershipForMT(ctx context.Context, className string, tenantNames []string) (map[string][]string, error)
}

// BuildUnitMaps creates one unit per (node, shard) replica — each
// replica processes its own local copy of the data. Unit IDs are
// deterministic and stable for the same shard placement so retries
// reuse the same identifiers.
func BuildUnitMaps(shardOwnership map[string][]string) (unitIDs []string, unitToShard, unitToNode map[string]string) {
	unitToShard = make(map[string]string)
	unitToNode = make(map[string]string)
	for nodeName, shards := range shardOwnership {
		for _, shardName := range shards {
			unitID := fmt.Sprintf("%s__%s", shardName, nodeName)
			unitIDs = append(unitIDs, unitID)
			unitToShard[unitID] = shardName
			unitToNode[unitID] = nodeName
		}
	}
	sort.Strings(unitIDs)
	return unitIDs, unitToShard, unitToNode
}

// BuildUnitSpecs creates [distributedtask.UnitSpec] entries with
// GroupID = shardName (= tenant name in MT). This enables per-tenant
// barrier semantics via OnGroupCompleted.
func BuildUnitSpecs(shardOwnership map[string][]string) []distributedtask.UnitSpec {
	var specs []distributedtask.UnitSpec
	for nodeName, shards := range shardOwnership {
		for _, shardName := range shards {
			unitID := fmt.Sprintf("%s__%s", shardName, nodeName)
			specs = append(specs, distributedtask.UnitSpec{
				ID:      unitID,
				GroupID: shardName,
			})
		}
	}
	sort.Slice(specs, func(i, j int) bool { return specs[i].ID < specs[j].ID })
	return specs
}

// ValidateTenants checks that all specified tenants exist in the
// collection's sharding state and are in a status that has local data
// (HOT, ACTIVE, COLD, INACTIVE). Returns an error for nonexistent or
// OFFLOADED / FROZEN tenants.
func ValidateTenants(ctx context.Context, db ShardOwnershipReader, collection string, tenants []string) error {
	allOwnership, err := db.ShardReplicaOwnershipForMT(ctx, collection, nil)
	if err != nil {
		return fmt.Errorf("reading shard state: %w", err)
	}
	knownTenants := make(map[string]struct{})
	for _, shards := range allOwnership {
		for _, shard := range shards {
			knownTenants[shard] = struct{}{}
		}
	}

	activeOwnership, err := db.ShardReplicaOwnershipForMT(ctx, collection, tenants)
	if err != nil {
		return fmt.Errorf("reading shard state: %w", err)
	}
	activeTenants := make(map[string]struct{})
	for _, shards := range activeOwnership {
		for _, shard := range shards {
			activeTenants[shard] = struct{}{}
		}
	}

	for _, tenant := range tenants {
		if _, ok := knownTenants[tenant]; !ok {
			return fmt.Errorf("tenant %q does not exist", tenant)
		}
		if _, ok := activeTenants[tenant]; !ok {
			return fmt.Errorf("tenant %q is not in an active status (OFFLOADED/FROZEN tenants cannot be reindexed)", tenant)
		}
	}
	return nil
}
