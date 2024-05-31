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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// VersionedSchemaReader is utilized to query the schema based on a specific update version. Serving as a thin wrapper around
// the original schema, it segregates waiting logic from the actual operation.
// It waits until it finds an update at least as up-to-date as the specified version.
// Note that updates may take some time to propagate to the follower, hence this process might take time.
type VersionedSchemaReader struct { // TODO TEST
	schema        *schema
	WaitForUpdate func(ctx context.Context, version uint64) error
}

func (s VersionedSchemaReader) ClassInfo(ctx context.Context,
	class string,
	v uint64,
) (ClassInfo, error) {
	t := prometheus.NewTimer(monitoring.GetMetrics().SchemaWaitForVersion.WithLabelValues("ClassInfo"))
	defer t.ObserveDuration()

	err := s.WaitForUpdate(ctx, v)
	return s.schema.ClassInfo(class), err
}

func (s VersionedSchemaReader) MultiTenancy(ctx context.Context,
	class string,
	v uint64,
) (models.MultiTenancyConfig, error) {
	t := prometheus.NewTimer(monitoring.GetMetrics().SchemaWaitForVersion.WithLabelValues("MultiTenancy"))
	defer t.ObserveDuration()

	if info := s.schema.ClassInfo(class); info.Exists {
		return info.MultiTenancy, nil
	}
	err := s.WaitForUpdate(ctx, v)
	return s.schema.MultiTenancy(class), err
}

// Read performs a read operation `reader` on the specified class and sharding state
func (s VersionedSchemaReader) Read(ctx context.Context,
	class string, v uint64,
	reader func(*models.Class, *sharding.State) error,
) error {
	t := prometheus.NewTimer(monitoring.GetMetrics().SchemaWaitForVersion.WithLabelValues("Read"))
	defer t.ObserveDuration()

	if err := s.WaitForUpdate(ctx, v); err != nil {
		return err
	}

	return s.schema.Read(class, reader)
}

// ReadOnlyClass returns a shallow copy of a class.
// The copy is read-only and should not be modified.
func (s VersionedSchemaReader) ReadOnlyClass(ctx context.Context,
	class string,
	v uint64,
) (*models.Class, error) {
	t := prometheus.NewTimer(monitoring.GetMetrics().SchemaWaitForVersion.WithLabelValues("ReadOnlyClass"))
	defer t.ObserveDuration()

	err := s.WaitForUpdate(ctx, v)
	cls, _ := s.schema.ReadOnlyClass(class)
	return cls, err
}

// ShardOwner returns the node owner of the specified shard
func (s VersionedSchemaReader) ShardOwner(ctx context.Context,
	class, shard string,
	v uint64,
) (string, error) {
	t := prometheus.NewTimer(monitoring.GetMetrics().SchemaWaitForVersion.WithLabelValues("ShardOwner"))
	defer t.ObserveDuration()

	err := s.WaitForUpdate(ctx, v)
	owner, _, sErr := s.schema.ShardOwner(class, shard)
	if sErr != nil && err == nil {
		err = sErr
	}
	return owner, err
}

// ShardFromUUID returns shard name of the provided uuid
func (s VersionedSchemaReader) ShardFromUUID(ctx context.Context,
	class string, uuid []byte, v uint64,
) (string, error) {
	t := prometheus.NewTimer(monitoring.GetMetrics().SchemaWaitForVersion.WithLabelValues("ShardFromUUID"))
	defer t.ObserveDuration()

	err := s.WaitForUpdate(ctx, v)
	shard, _ := s.schema.ShardFromUUID(class, uuid)
	return shard, err
}

// ShardReplicas returns the replica nodes of a shard
func (s VersionedSchemaReader) ShardReplicas(
	ctx context.Context, class, shard string,
	v uint64,
) ([]string, error) {
	t := prometheus.NewTimer(monitoring.GetMetrics().SchemaWaitForVersion.WithLabelValues("ShardReplicas"))
	defer t.ObserveDuration()

	err := s.WaitForUpdate(ctx, v)
	nodes, _, sErr := s.schema.ShardReplicas(class, shard)
	if sErr != nil && err == nil {
		err = sErr
	}
	return nodes, err
}

// TenantShard returns shard name for the provided tenant and its activity status
func (s VersionedSchemaReader) TenantsShards(ctx context.Context,
	v uint64, class string, tenants ...string,
) (map[string]string, uint64, error) {
	t := prometheus.NewTimer(monitoring.GetMetrics().SchemaWaitForVersion.WithLabelValues("TenantsShards"))
	defer t.ObserveDuration()

	err := s.WaitForUpdate(ctx, v)
	status, version := s.schema.TenantsShards(class, tenants...)
	return status, version, err
}

func (s VersionedSchemaReader) CopyShardingState(ctx context.Context,
	class string, v uint64,
) (*sharding.State, error) {
	t := prometheus.NewTimer(monitoring.GetMetrics().SchemaWaitForVersion.WithLabelValues("CopyShardingState"))
	defer t.ObserveDuration()

	err := s.WaitForUpdate(ctx, v)
	ss, _ := s.schema.CopyShardingState(class)
	return ss, err
}

func (s VersionedSchemaReader) Len() int { return s.schema.len() }

// ClassEqual returns the name of an existing class with a similar name, and "" otherwise
// strings.EqualFold is used to compare classes
func (s VersionedSchemaReader) ClassEqual(name string) string {
	return s.schema.ClassEqual(name)
}
