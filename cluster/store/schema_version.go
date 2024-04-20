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

// versionedSchema is utilized to query the schema based on a specific update version. Serving as a thin wrapper around
// the original schema, it segregates waiting logic from the actual operation.
// It waits until it finds an update at least as up-to-date as the specified version.
// Note that updates may take some time to propagate to the follower, hence this process might take time.

type versionedSchema struct { // TODO TEST
	schema        *schema
	WaitForUpdate func(ctx context.Context, version uint64) error
}

func (s versionedSchema) ClassInfo(ctx context.Context,
	class string,
	v uint64,
) (ClassInfo, error) {
	err := s.WaitForUpdate(ctx, v)
	return s.schema.ClassInfo(class), err
}

func (s versionedSchema) MultiTenancy(ctx context.Context,
	class string,
	v uint64,
) (models.MultiTenancyConfig, error) {
	// MT is immutable
	if info := s.schema.ClassInfo(class); info.Exists {
		return info.MultiTenancy, nil
	}
	err := s.WaitForUpdate(ctx, v)
	return s.schema.MultiTenancy(class), err
}

// Read performs a read operation `reader` on the specified class and sharding state
func (s versionedSchema) Read(ctx context.Context,
	class string, v uint64,
	reader func(*models.Class,
		*sharding.State) error,
) error {
	if err := s.WaitForUpdate(ctx, v); err != nil {
		return err
	}

	return s.schema.Read(class, reader)
}

// ReadOnlyClass returns a shallow copy of a class.
// The copy is read-only and should not be modified.
func (s versionedSchema) ReadOnlyClass(ctx context.Context,
	class string,
	v uint64,
) (*models.Class, error) {
	err := s.WaitForUpdate(ctx, v)
	cls, _ := s.schema.ReadOnlyClass(class)
	return cls, err
}

// ShardOwner returns the node owner of the specified shard
func (s versionedSchema) ShardOwner(ctx context.Context,
	class, shard string,
	v uint64,
) (string, error) {
	err := s.WaitForUpdate(ctx, v)
	owner, _, sErr := s.schema.ShardOwner(class, shard)
	if sErr != nil && err == nil {
		err = sErr
	}
	return owner, err
}

// ShardFromUUID returns shard name of the provided uuid
func (s versionedSchema) ShardFromUUID(ctx context.Context,
	class string, uuid []byte, v uint64,
) (string, error) {
	err := s.WaitForUpdate(ctx, v)
	shard, _ := s.schema.ShardFromUUID(class, uuid)
	return shard, err
}

// ShardReplicas returns the replica nodes of a shard
func (s versionedSchema) ShardReplicas(
	ctx context.Context, class, shard string,
	v uint64,
) ([]string, error) {
	err := s.WaitForUpdate(ctx, v)
	nodes, _, sErr := s.schema.ShardReplicas(class, shard)
	if sErr != nil && err == nil {
		err = sErr
	}
	return nodes, err
}

// TenantShard returns shard name for the provided tenant and its activity status
func (s versionedSchema) TenantsShards(ctx context.Context,
	v uint64, class string, tenants ...string,
) (map[string]string, error) {
	err := s.WaitForUpdate(ctx, v)
	return s.schema.TenantsShards(class, tenants...), err
}

func (s versionedSchema) CopyShardingState(ctx context.Context,
	class string, v uint64,
) (*sharding.State, error) {
	err := s.WaitForUpdate(ctx, v)
	ss, _ := s.schema.CopyShardingState(class)
	return ss, err
}
