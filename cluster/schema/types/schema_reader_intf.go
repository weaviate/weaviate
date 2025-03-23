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

package types

import (
	"context"

	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/versioned"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type SchemaReader interface {
	// WaitForUpdate ensures that the local schema has caught up to version.
	WaitForUpdate(ctx context.Context, version uint64) error

	// These schema reads function reads the metadata immediately present in the local schema and can be eventually
	// consistent.
	// For details about each endpoint see [github.com/weaviate/weaviate/cluster/schema.SchemaReader].
	ClassEqual(name string) string
	MultiTenancy(class string) models.MultiTenancyConfig
	ClassInfo(class string) (ci schema.ClassInfo)
	ReadOnlyClass(name string) *models.Class
	ReadOnlyVersionedClass(name string) versioned.Class
	ReadOnlySchema() models.Schema
	CopyShardingState(class string) *sharding.State
	ShardReplicas(class, shard string) ([]string, error)
	ShardFromUUID(class string, uuid []byte) string
	ShardOwner(class, shard string) (string, error)
	Read(class string, reader func(*models.Class, *sharding.State) error) error
	GetShardsStatus(class, tenant string) (models.ShardStatusList, error)

	// These schema reads function (...WithVersion) return the metadata once the local schema has caught up to the
	// version parameter. If version is 0 is behaves exactly the same as eventual consistent reads.
	// For details about each endpoint see [github.com/weaviate/weaviate/cluster/schema.VersionedSchemaReader].
	ClassInfoWithVersion(ctx context.Context, class string, version uint64) (schema.ClassInfo, error)
	MultiTenancyWithVersion(ctx context.Context, class string, version uint64) (models.MultiTenancyConfig, error)
	ReadOnlyClassWithVersion(ctx context.Context, class string, version uint64) (*models.Class, error)
	ShardOwnerWithVersion(ctx context.Context, lass, shard string, version uint64) (string, error)
	ShardFromUUIDWithVersion(ctx context.Context, class string, uuid []byte, version uint64) (string, error)
	ShardReplicasWithVersion(ctx context.Context, class, shard string, version uint64) ([]string, error)
	TenantsShardsWithVersion(ctx context.Context, version uint64, class string, tenants ...string) (map[string]string, error)
	CopyShardingStateWithVersion(ctx context.Context, class string, version uint64) (*sharding.State, error)
}
