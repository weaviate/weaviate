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

	"github.com/cenkalti/backoff/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/cluster/utils"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/versioned"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// SchemaReader is used for retrying schema queries. It is a thin wrapper around
// the original schema, separating retry logic from the actual operation.
// Retry may be needed due to eventual consistency issues where
// updates might take some time to arrive at the follower.
type SchemaReader struct {
	schema                *schema
	versionedSchemaReader VersionedSchemaReader
}

func NewSchemaReader(sc *schema, vsr VersionedSchemaReader) SchemaReader {
	return SchemaReader{
		schema:                sc,
		versionedSchemaReader: vsr,
	}
}

func NewSchemaReaderWithoutVersion(sc *schema) SchemaReader {
	return SchemaReader{
		schema: sc,
		versionedSchemaReader: VersionedSchemaReader{
			schema:        sc,
			WaitForUpdate: func(context.Context, uint64) error { return nil },
		},
	}
}

func (rs SchemaReader) States() map[string]types.ClassState {
	t := prometheus.NewTimer(monitoring.GetMetrics().SchemaReadsLocal.WithLabelValues("States"))
	defer t.ObserveDuration()

	return rs.schema.States()
}

func (rs SchemaReader) ClassInfo(class string) (ci ClassInfo) {
	t := prometheus.NewTimer(monitoring.GetMetrics().SchemaReadsLocal.WithLabelValues("ClassInfo"))
	defer t.ObserveDuration()

	res, _ := rs.ClassInfoWithVersion(context.TODO(), class, 0)
	return res
}

// ClassEqual returns the name of an existing class with a similar name, and "" otherwise
// strings.EqualFold is used to compare classes
func (rs SchemaReader) ClassEqual(name string) string {
	return rs.schema.ClassEqual(name)
}

func (rs SchemaReader) MultiTenancy(class string) models.MultiTenancyConfig {
	t := prometheus.NewTimer(monitoring.GetMetrics().SchemaReadsLocal.WithLabelValues("MultiTenancy"))
	defer t.ObserveDuration()
	res, _ := rs.MultiTenancyWithVersion(context.TODO(), class, 0)
	return res
}

// Read performs a read operation `reader` on the specified class and sharding state
func (rs SchemaReader) Read(class string, reader func(*models.Class, *sharding.State) error) error {
	t := prometheus.NewTimer(monitoring.GetMetrics().SchemaReadsLocal.WithLabelValues("Read"))
	defer t.ObserveDuration()

	return rs.retry(func(s *schema) error {
		return s.Read(class, reader)
	})
}

// ReadOnlyClass returns a shallow copy of a class.
// The copy is read-only and should not be modified.
func (rs SchemaReader) ReadOnlyClass(class string) (cls *models.Class) {
	t := prometheus.NewTimer(monitoring.GetMetrics().SchemaReadsLocal.WithLabelValues("ReadOnlyClass"))
	defer t.ObserveDuration()

	res, _ := rs.ReadOnlyClassWithVersion(context.TODO(), class, 0)
	return res
}

// ReadOnlyVersionedClass returns a shallow copy of a class along with its version.
// The copy is read-only and should not be modified.
func (rs SchemaReader) ReadOnlyVersionedClass(className string) versioned.Class {
	class, version := rs.schema.ReadOnlyClass(className)
	return versioned.Class{
		Class:   class,
		Version: version,
	}
}

func (rs SchemaReader) metaClass(class string) (meta *metaClass) {
	rs.retry(func(s *schema) error {
		if meta = s.metaClass(class); meta == nil {
			return ErrClassNotFound
		}
		return nil
	})
	return
}

// ReadOnlySchema returns a read only schema
// Changing the schema outside this package might lead to undefined behavior.
//
// it creates a shallow copy of existing classes
//
// This function assumes that class attributes are being overwritten.
// The properties attribute is the only one that might vary in size;
// therefore, we perform a shallow copy of the existing properties.
// This implementation assumes that individual properties are overwritten rather than partially updated
func (rs SchemaReader) ReadOnlySchema() models.Schema {
	t := prometheus.NewTimer(monitoring.GetMetrics().SchemaReadsLocal.WithLabelValues("ReadOnlySchema"))
	defer t.ObserveDuration()

	return rs.schema.ReadOnlySchema()
}

// ShardOwner returns the node owner of the specified shard
func (rs SchemaReader) ShardOwner(class, shard string) (owner string, err error) {
	t := prometheus.NewTimer(monitoring.GetMetrics().SchemaReadsLocal.WithLabelValues("ShardOwner"))
	defer t.ObserveDuration()

	res, err := rs.ShardOwnerWithVersion(context.TODO(), class, shard, 0)
	return res, err
}

// ShardFromUUID returns shard name of the provided uuid
func (rs SchemaReader) ShardFromUUID(class string, uuid []byte) (shard string) {
	t := prometheus.NewTimer(monitoring.GetMetrics().SchemaReadsLocal.WithLabelValues("ShardFromUUID"))
	defer t.ObserveDuration()

	res, _ := rs.ShardFromUUIDWithVersion(context.TODO(), class, uuid, 0)
	return res
}

// ShardReplicas returns the replica nodes of a shard
func (rs SchemaReader) ShardReplicas(class, shard string) (nodes []string, err error) {
	t := prometheus.NewTimer(monitoring.GetMetrics().SchemaReadsLocal.WithLabelValues("ShardReplicas"))
	defer t.ObserveDuration()

	res, err := rs.ShardReplicasWithVersion(context.TODO(), class, shard, 0)
	return res, err
}

// TenantsShards returns shard name for the provided tenant and its activity status
func (rs SchemaReader) TenantsShards(class string, tenants ...string) (map[string]string, error) {
	t := prometheus.NewTimer(monitoring.GetMetrics().SchemaReadsLocal.WithLabelValues("TenantsShards"))
	defer t.ObserveDuration()

	return rs.TenantsShardsWithVersion(context.TODO(), 0, class, tenants...)
}

func (rs SchemaReader) CopyShardingState(class string) (ss *sharding.State) {
	t := prometheus.NewTimer(monitoring.GetMetrics().SchemaReadsLocal.WithLabelValues("CopyShardingState"))
	defer t.ObserveDuration()

	res, _ := rs.CopyShardingStateWithVersion(context.TODO(), class, 0)
	return res
}

func (rs SchemaReader) GetShardsStatus(class, tenant string) (models.ShardStatusList, error) {
	t := prometheus.NewTimer(monitoring.GetMetrics().SchemaReadsLocal.WithLabelValues("GetShardsStatus"))
	defer t.ObserveDuration()

	return rs.schema.GetShardsStatus(class, tenant)
}

func (rs SchemaReader) Len() int { return rs.schema.len() }

func (rs SchemaReader) retry(f func(*schema) error) error {
	return backoff.Retry(func() error {
		return f(rs.schema)
	}, utils.NewBackoff())
}
