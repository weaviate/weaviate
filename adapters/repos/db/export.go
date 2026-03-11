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
	"fmt"
	"sort"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/multitenancy"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// ExportShardLike is the minimal shard interface needed for export operations.
// Matches usecases/export.ShardLike.
type ExportShardLike = interface {
	Store() *lsmkv.Store
	Name() string
}

// ShardOwnership returns a map of node name to shard names for a given class.
// Shards are distributed across their replica nodes using a least-loaded
// strategy so that export work is balanced across the cluster.
func (db *DB) ShardOwnership(ctx context.Context, className string) (map[string][]string, error) {
	shardNodes := make(map[string][]string)

	err := db.schemaReader.Read(className, true, func(_ *models.Class, state *sharding.State) error {
		if state == nil {
			return fmt.Errorf("unable to retrieve sharding state for class %s", className)
		}

		for shardName, shard := range state.Physical {
			if len(shard.BelongsToNodes) == 0 {
				return fmt.Errorf("shard %s of class %s has no assigned nodes", shardName, className)
			}

			// Filter out empty node names to avoid assigning shards to an invalid node.
			validNodes := make([]string, 0, len(shard.BelongsToNodes))
			for _, node := range shard.BelongsToNodes {
				if node != "" {
					validNodes = append(validNodes, node)
				}
			}
			if len(validNodes) == 0 {
				return fmt.Errorf("shard %s of class %s has only empty assigned nodes", shardName, className)
			}

			shardNodes[shardName] = validNodes
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to read sharding state for class %s: %w", className, err)
	}

	return assignShardsToNodes(shardNodes), nil
}

// assignShardsToNodes distributes shards across their replica nodes using a
// least-loaded strategy. For each shard (processed in sorted order for
// determinism), it picks the replica node with the fewest already-assigned
// shards. Ties are broken by lexicographic node name order.
func assignShardsToNodes(shards map[string][]string) map[string][]string {
	result := make(map[string][]string)
	if len(shards) == 0 {
		return result
	}

	// Process shards in sorted order for determinism.
	shardNames := make([]string, 0, len(shards))
	for name := range shards {
		shardNames = append(shardNames, name)
	}
	sort.Strings(shardNames)

	for _, shardName := range shardNames {
		nodes := shards[shardName]

		// Pick the node with the least load; break ties lexicographically.
		best := nodes[0]
		bestLoad := len(result[best])
		for _, node := range nodes[1:] {
			nl := len(result[node])
			if nl < bestLoad || (nl == bestLoad && node < best) {
				best = node
				bestLoad = nl
			}
		}

		result[best] = append(result[best], shardName)
	}

	return result
}

// ExportShardNames returns all shard names for a class and whether the class is MT.
// Activity status is not checked here — callers must handle COLD tenants in
// AcquireShardForExport, which is resilient to tenants going COLD between
// listing and acquiring.
func (db *DB) ExportShardNames(className string) ([]string, bool, error) {
	idx := db.GetIndex(schema.ClassName(className))
	if idx == nil {
		return nil, false, fmt.Errorf("index not found for class %s", className)
	}

	class := idx.getClass()
	if class == nil {
		return nil, false, fmt.Errorf("class not found for index %s", className)
	}

	isMT := multitenancy.IsMultiTenant(class.MultiTenancyConfig)

	allShards, err := idx.schemaReader.Shards(class.Class)
	if err != nil {
		return nil, false, fmt.Errorf("get shards for class %s: %w", className, err)
	}

	return allShards, isMT, nil
}

// AcquireShardForExport returns the shard handle and a release function.
// For MT classes it checks the tenant's activity status:
//   - COLD + autoActivation enabled: activates the tenant; release deactivates it.
//   - COLD + autoActivation disabled: returns a nil shard and a nil error so the
//     caller can skip this tenant gracefully.
//   - HOT/ACTIVE: returns the shard as-is; release is a no-op.
//
// For non-MT classes it simply loads the shard.
func (db *DB) AcquireShardForExport(ctx context.Context, className, shardName string) (ExportShardLike, func(), string, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, "", err
	}

	idx := db.GetIndex(schema.ClassName(className))
	if idx == nil {
		return nil, nil, "", fmt.Errorf("index not found for class %s", className)
	}

	class := idx.getClass()
	if class == nil {
		return nil, nil, "", fmt.Errorf("class not found for index %s", className)
	}

	isMT := multitenancy.IsMultiTenant(class.MultiTenancyConfig)
	autoActivationEnabled := schema.AutoTenantActivationEnabled(class)

	// For MT classes, check the tenant's activity status up front.
	// Only HOT and COLD tenants are exportable. All other statuses
	// (OFFLOADED, OFFLOADING, ONLOADING, FROZEN, FREEZING, UNFREEZING, etc.)
	// are skipped — the caller receives a nil shard with no error.
	//
	// For COLD tenants:
	//   - auto-activation enabled: activate, export, then deactivate after.
	//   - auto-activation disabled: skip.
	deactivateAfter := false
	if isMT {
		statuses, err := idx.tenantsManager.TenantsStatus(class.Class, shardName)
		if err != nil {
			return nil, nil, "", fmt.Errorf("get tenant status for %s/%s: %w", className, shardName, err)
		}
		status := statuses[shardName]
		switch status {
		case models.TenantActivityStatusHOT, models.TenantActivityStatusACTIVE:
			// Exportable as-is.
		case models.TenantActivityStatusCOLD, models.TenantActivityStatusINACTIVE:
			if !autoActivationEnabled {
				return nil, nil, fmt.Sprintf("tenant is %s and auto-activation is disabled", status), nil
			}
			deactivateAfter = true
		default:
			// Any other status (OFFLOADED, FROZEN, transitional states, etc.)
			// — skip this tenant.
			return nil, nil, fmt.Sprintf("tenant status is %s", status), nil
		}
	}

	shard, shardRelease, err := idx.acquireShardWithLock(ctx, shardName, class)
	if err != nil {
		return nil, nil, "", fmt.Errorf("acquire shard %s for class %s: %w", shardName, className, err)
	}

	release := shardRelease
	if deactivateAfter {
		release = func() {
			shardRelease()
			deactivateCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := idx.tenantsManager.DeactivateTenants(deactivateCtx, class.Class, shardName); err != nil {
				idx.logger.WithField("action", "export").
					WithField("class", className).
					WithField("tenant", shardName).
					Warnf("failed to deactivate tenant after export: %v", err)
			}
		}
	}

	return shard, release, "", nil
}

// acquireShardWithLock loads (or initializes) a shard and returns it with
// shardCreateLocks.RLock held. The returned release function releases both
// preventShutdown and the RLock. Holding RLock blocks the migrator from
// deactivating the shard (it acquires shardCreateLocks.Lock).
func (i *Index) acquireShardWithLock(ctx context.Context, shardName string, class *models.Class) (ShardLike, func(), error) {
	i.closeLock.RLock()
	defer i.closeLock.RUnlock()
	if i.closed {
		return nil, nil, errAlreadyShutdown
	}

	i.shardCreateLocks.RLock(shardName)
	shard := i.shards.Load(shardName)

	if shard != nil {
		// Hot path: shard already loaded. RLock is held continuously from
		// load through preventShutdown — no gap for the migrator.
		shardRelease, err := shard.preventShutdown()
		if err != nil {
			i.shardCreateLocks.RUnlock(shardName)
			return nil, nil, err
		}
		release := func() {
			shardRelease()
			i.shardCreateLocks.RUnlock(shardName)
		}
		return shard, release, nil
	}

	// Cold path: shard not loaded — upgrade to exclusive Lock for init.
	// Go's RWMutex does not support upgrade, so release RLock first.
	i.shardCreateLocks.RUnlock(shardName)

	i.shardCreateLocks.Lock(shardName)
	// Double-check: another goroutine may have initialized it.
	shard = i.shards.Load(shardName)
	if shard == nil {
		var err error
		shard, err = i.initShard(ctx, shardName, class, i.metrics.baseMetrics, true, false)
		if err != nil {
			i.shardCreateLocks.Unlock(shardName)
			return nil, nil, err
		}
		i.shards.Store(shardName, shard)
	}

	// Call preventShutdown while still holding exclusive Lock, so no
	// deactivation can start between init and the ref being acquired.
	shardRelease, err := shard.preventShutdown()
	if err != nil {
		i.shardCreateLocks.Unlock(shardName)
		return nil, nil, err
	}

	// Downgrade to RLock: release exclusive Lock, then acquire RLock.
	// The migrator could slip in during this gap, but preventShutdown
	// is already held so it cannot shut the shard down.
	i.shardCreateLocks.Unlock(shardName)
	i.shardCreateLocks.RLock(shardName)

	release := func() {
		shardRelease()
		i.shardCreateLocks.RUnlock(shardName)
	}
	return shard, release, nil
}

// IsMultiTenant returns true if the class has multi-tenancy enabled.
func (db *DB) IsMultiTenant(_ context.Context, className string) bool {
	idx := db.GetIndex(schema.ClassName(className))
	if idx == nil {
		return false
	}
	class := idx.getClass()
	return class != nil && class.MultiTenancyConfig != nil && class.MultiTenancyConfig.Enabled
}

// ListClasses returns all class names (already exists on DB, this is a convenience alias comment).
// DB.ListClasses is defined in index.go.
