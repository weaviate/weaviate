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

package schema

import (
	"context"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func (rs SchemaReader) WaitForUpdate(ctx context.Context, version uint64) error {
	if version > 0 {
		return rs.versionedSchemaReader.WaitForUpdate(ctx, version)
	}
	return nil
}

func (rs SchemaReader) ClassInfoWithVersion(ctx context.Context, class string, version uint64) (ci ClassInfo, err error) {
	if version > 0 {
		return rs.versionedSchemaReader.ClassInfo(ctx, class, version)
	}
	rs.retry(func(s *schema) error {
		ci = s.ClassInfo(class)
		if !ci.Exists {
			return ErrClassNotFound
		}
		return nil
	})
	return ci, nil
}

func (rs SchemaReader) MultiTenancyWithVersion(ctx context.Context, class string, version uint64) (models.MultiTenancyConfig, error) {
	if version > 0 {
		return rs.versionedSchemaReader.MultiTenancy(ctx, class, version)
	}
	mc, _ := rs.metaClass(class).MultiTenancyConfig()
	return mc, nil
}

// ReadOnlyClass returns a shallow copy of a class.
// The copy is read-only and should not be modified.
func (rs SchemaReader) ReadOnlyClassWithVersion(ctx context.Context, class string, version uint64) (cls *models.Class, err error) {
	if version > 0 {
		return rs.versionedSchemaReader.ReadOnlyClass(ctx, class, version)
	}
	rs.retry(func(s *schema) error {
		if cls, _ = s.ReadOnlyClass(class); cls == nil {
			return ErrClassNotFound
		}
		return nil
	})
	return cls, nil
}

// ShardOwner returns the node owner of the specified shard
func (rs SchemaReader) ShardOwnerWithVersion(ctx context.Context, class, shard string, version uint64) (owner string, err error) {
	if version > 0 {
		return rs.versionedSchemaReader.ShardOwner(ctx, class, shard, version)
	}
	err = rs.retry(func(s *schema) error {
		owner, _, err = s.ShardOwner(class, shard)
		return err
	})
	return
}

// ShardFromUUID returns shard name of the provided uuid
func (rs SchemaReader) ShardFromUUIDWithVersion(ctx context.Context, class string, uuid []byte, version uint64) (shard string, err error) {
	if version > 0 {
		return rs.versionedSchemaReader.ShardFromUUID(ctx, class, uuid, version)
	}
	rs.retry(func(s *schema) error {
		if shard, _ = s.ShardFromUUID(class, uuid); shard == "" {
			return ErrClassNotFound
		}
		return nil
	})
	return
}

// ShardReplicas returns the replica nodes of a shard
func (rs SchemaReader) ShardReplicasWithVersion(ctx context.Context, class, shard string, version uint64) (nodes []string, err error) {
	if version > 0 {
		return rs.versionedSchemaReader.ShardReplicas(ctx, class, shard, version)
	}
	rs.retry(func(s *schema) error {
		nodes, _, err = s.ShardReplicas(class, shard)
		return err
	})
	return
}

// TenantsShardsWithVersion returns shard name for the provided tenant and its activity status
func (rs SchemaReader) TenantsShardsWithVersion(ctx context.Context, version uint64, class string, tenants ...string) (tenantShards map[string]string, err error) {
	if version > 0 {
		status, _, err := rs.versionedSchemaReader.TenantsShards(ctx, version, class, tenants...)
		return status, err
	}
	rs.retry(func(s *schema) error {
		if tenantShards, _ = s.TenantsShards(class, tenants...); len(tenantShards) == 0 {
			return ErrShardNotFound
		}
		return nil
	})

	return
}

func (rs SchemaReader) CopyShardingStateWithVersion(ctx context.Context, class string, version uint64) (ss *sharding.State, err error) {
	if version > 0 {
		return rs.versionedSchemaReader.CopyShardingState(ctx, class, version)
	}
	rs.retry(func(s *schema) error {
		if ss, _ = s.CopyShardingState(class); ss == nil {
			return ErrClassNotFound
		}
		return nil
	})
	return
}
