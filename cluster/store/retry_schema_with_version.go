//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package store

import (
	"context"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func (rs retrySchema) ClassInfoWithVersion(ctx context.Context, class string, version uint64) (ci ClassInfo, err error) {
	if version > 0 {
		return rs.versionedSchema.ClassInfo(ctx, class, version)
	}
	rs.retry(func(s *schema) error {
		ci = s.ClassInfo(class)
		if !ci.Exists {
			return errClassExists
		}
		return nil
	})
	return ci, nil
}

func (rs retrySchema) MultiTenancyWithVersion(ctx context.Context, class string, version uint64) (models.MultiTenancyConfig, error) {
	if version > 0 {
		return rs.versionedSchema.MultiTenancy(ctx, class, version)
	}
	mc, _ := rs.metaClass(class).MultiTenancyConfig()
	return mc, nil
}

// ReadOnlyClass returns a shallow copy of a class.
// The copy is read-only and should not be modified.
func (rs retrySchema) ReadOnlyClassWithVersion(ctx context.Context, class string, version uint64) (cls *models.Class, err error) {
	if version > 0 {
		return rs.versionedSchema.ReadOnlyClass(ctx, class, version)
	}
	rs.retry(func(s *schema) error {
		if cls, _ = s.ReadOnlyClass(class); cls == nil {
			return errClassNotFound
		}
		return nil
	})
	return cls, nil
}

// ShardOwner returns the node owner of the specified shard
func (rs retrySchema) ShardOwnerWithVersion(ctx context.Context, class, shard string, version uint64) (owner string, err error) {
	if version > 0 {
		return rs.versionedSchema.ShardOwner(ctx, class, shard, version)
	}
	err = rs.retry(func(s *schema) error {
		owner, _, err = s.ShardOwner(class, shard)
		return err
	})
	return
}

// ShardFromUUID returns shard name of the provided uuid
func (rs retrySchema) ShardFromUUIDWithVersion(ctx context.Context, class string, uuid []byte, version uint64) (shard string, err error) {
	if version > 0 {
		return rs.versionedSchema.ShardFromUUID(ctx, class, uuid, version)
	}
	rs.retry(func(s *schema) error {
		if shard, _ = s.ShardFromUUID(class, uuid); shard == "" {
			return errClassNotFound
		}
		return nil
	})
	return
}

// ShardReplicas returns the replica nodes of a shard
func (rs retrySchema) ShardReplicasWithVersion(ctx context.Context, class, shard string, version uint64) (nodes []string, err error) {
	if version > 0 {
		return rs.versionedSchema.ShardReplicas(ctx, class, shard, version)
	}
	rs.retry(func(s *schema) error {
		nodes, _, err = s.ShardReplicas(class, shard)
		return err
	})
	return
}

// TenantsShardsWithVersion returns shard name for the provided tenant and its activity status
func (rs retrySchema) TenantsShardsWithVersion(ctx context.Context, version uint64, class string, tenants ...string) (tenantShards map[string]string, err error) {
	if version > 0 {
		return rs.versionedSchema.TenantsShards(ctx, version, class, tenants...)
	}
	rs.retry(func(s *schema) error {
		if tenantShards = s.TenantsShards(class, tenants...); len(tenantShards) == 0 {
			return errShardNotFound
		}
		return nil
	})

	return
}

func (rs retrySchema) CopyShardingStateWithVersion(ctx context.Context, class string, version uint64) (ss *sharding.State, err error) {
	if version > 0 {
		return rs.versionedSchema.CopyShardingState(ctx, class, version)
	}
	rs.retry(func(s *schema) error {
		if ss, _ = s.CopyShardingState(class); ss == nil {
			return errClassNotFound
		}
		return nil
	})
	return
}
