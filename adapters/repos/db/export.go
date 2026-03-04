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
// Only the primary owner (BelongsToNodes[0]) is included for each shard,
// ensuring replicated shards are never exported twice.
func (db *DB) ShardOwnership(ctx context.Context, className string) (map[string][]string, error) {
	result := make(map[string][]string)

	err := db.schemaReader.Read(className, true, func(_ *models.Class, state *sharding.State) error {
		if state == nil {
			return fmt.Errorf("unable to retrieve sharding state for class %s", className)
		}

		for shardName, shard := range state.Physical {
			if len(shard.BelongsToNodes) == 0 {
				return fmt.Errorf("shard %s of class %s has no assigned nodes", shardName, className)
			}
			primaryNode := shard.BelongsToNodes[0]
			result[primaryNode] = append(result[primaryNode], shardName)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to read sharding state for class %s: %w", className, err)
	}

	return result, nil
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
func (db *DB) AcquireShardForExport(ctx context.Context, className, shardName string) (ExportShardLike, func(), error) {
	idx := db.GetIndex(schema.ClassName(className))
	if idx == nil {
		return nil, nil, fmt.Errorf("index not found for class %s", className)
	}

	class := idx.getClass()
	if class == nil {
		return nil, nil, fmt.Errorf("class not found for index %s", className)
	}

	isMT := multitenancy.IsMultiTenant(class.MultiTenancyConfig)
	autoActivationEnabled := schema.AutoTenantActivationEnabled(class)

	// Following the objectTTL pattern: only check tenant status when
	// auto-activation is enabled, to know whether to deactivate after export.
	// getOptInitLocalShard will implicitly load/activate the shard.
	// When auto-activation is disabled, COLD tenants are skipped.
	deactivateAfter := false
	if isMT {
		if autoActivationEnabled {
			statuses, err := idx.tenantsManager.TenantsStatus(class.Class, shardName)
			if err != nil {
				return nil, nil, fmt.Errorf("get tenant status for %s/%s: %w", className, shardName, err)
			}
			deactivateAfter = statuses[shardName] == models.TenantActivityStatusCOLD
		}
	}

	shard, shardRelease, err := idx.getOptInitLocalShard(ctx, shardName, autoActivationEnabled)
	if err != nil {
		return nil, nil, fmt.Errorf("get shard %s for class %s: %w", shardName, className, err)
	}

	release := shardRelease
	if deactivateAfter {
		release = func() {
			shardRelease()
			deactivateCtx := context.Background()
			if err := idx.tenantsManager.DeactivateTenants(deactivateCtx, class.Class, shardName); err != nil {
				idx.logger.WithField("action", "export").
					WithField("class", className).
					WithField("tenant", shardName).
					Warnf("failed to deactivate tenant after export: %v", err)
			}
		}
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
