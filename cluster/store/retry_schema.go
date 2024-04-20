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

	"github.com/cenkalti/backoff/v4"
	"github.com/weaviate/weaviate/cluster/utils"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// retrySchema is used for retrying schema queries. It is a thin wrapper around
// the original schema, separating retry logic from the actual operation.
// Retry may be needed due to eventual consistency issues where
// updates might take some time to arrive at the follower.
type retrySchema struct {
	schema          *schema
	versionedSchema versionedSchema
}

func (rs retrySchema) ClassInfo(class string) (ci ClassInfo) {
	res, _ := rs.ClassInfoWithVersion(context.TODO(), class, 0)
	return res
}

// ClassEqual returns the name of an existing class with a similar name, and "" otherwise
// strings.EqualFold is used to compare classes
func (rs retrySchema) ClassEqual(name string) string {
	return rs.schema.ClassEqual(name)
}

func (rs retrySchema) MultiTenancy(class string) models.MultiTenancyConfig {
	res, _ := rs.MultiTenancyWithVersion(context.TODO(), class, 0)
	return res
}

// Read performs a read operation `reader` on the specified class and sharding state
func (rs retrySchema) Read(class string, reader func(*models.Class, *sharding.State) error) error {
	return rs.retry(func(s *schema) error {
		return s.Read(class, reader)
	})
}

// ReadOnlyClass returns a shallow copy of a class.
// The copy is read-only and should not be modified.
func (rs retrySchema) ReadOnlyClass(class string) (cls *models.Class) {
	res, _ := rs.ReadOnlyClassWithVersion(context.TODO(), class, 0)
	return res
}

func (rs retrySchema) metaClass(class string) (meta *metaClass) {
	rs.retry(func(s *schema) error {
		if meta = s.metaClass(class); meta == nil {
			return errClassNotFound
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
func (rs retrySchema) ReadOnlySchema() models.Schema {
	return rs.schema.ReadOnlySchema()
}

// ShardOwner returns the node owner of the specified shard
func (rs retrySchema) ShardOwner(class, shard string) (owner string, err error) {
	res, err := rs.ShardOwnerWithVersion(context.TODO(), class, shard, 0)
	return res, err
}

// ShardFromUUID returns shard name of the provided uuid
func (rs retrySchema) ShardFromUUID(class string, uuid []byte) (shard string) {
	res, _ := rs.ShardFromUUIDWithVersion(context.TODO(), class, uuid, 0)
	return res
}

// ShardReplicas returns the replica nodes of a shard
func (rs retrySchema) ShardReplicas(class, shard string) (nodes []string, err error) {
	res, err := rs.ShardReplicasWithVersion(context.TODO(), class, shard, 0)
	return res, err
}

// TenantsShards returns shard name for the provided tenant and its activity status
func (rs retrySchema) TenantsShards(class string, tenants ...string) (map[string]string, error) {
	return rs.TenantsShardsWithVersion(context.TODO(), 0, class, tenants...)
}

func (rs retrySchema) CopyShardingState(class string) (ss *sharding.State) {
	res, _ := rs.CopyShardingStateWithVersion(context.TODO(), class, 0)
	return res
}

func (rs retrySchema) GetShardsStatus(class string) (models.ShardStatusList, error) {
	return rs.schema.GetShardsStatus(class)
}

func (rs retrySchema) Len() int { return rs.schema.len() }

func (rs retrySchema) retry(f func(*schema) error) error {
	return backoff.Retry(func() error {
		return f(rs.schema)
	}, utils.NewBackoff())
}
