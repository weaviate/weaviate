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

// Package local defines schema reads answered from this node's copy of the schema.
// They never leave the node, so they keep working without a RAFT leader, but they
// are eventually consistent: a change the leader has committed may not be applied
// here yet. VersionedReader closes that gap for callers that know which schema
// version they need. For reads that must reflect the leader's view see package leader.
//
// Nothing this package imports may import it back. In particular it must not import
// cluster/schema, which implements SchemaReader.
package local

import (
	"context"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/versioned"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// SchemaReader reads the local copy of the schema. It is the union of the
// context-specific readers; depend on the narrowest one that covers the caller.
type SchemaReader interface {
	ClassReader
	AliasReader
	ShardReader
	VersionedReader
}

// ClassReader reads class metadata.
type ClassReader interface {
	// ReadOnlyClass returns a shallow copy of the class, or nil if it does not exist.
	// The copy must not be modified.
	ReadOnlyClass(name string) *models.Class
	ReadOnlyVersionedClass(name string) versioned.Class
	ReadOnlySchema() models.Schema
	ClassInfo(class string) ClassInfo
	// ClassEqual returns the name of the existing class that matches name
	// case-insensitively, or "" if there is none.
	ClassEqual(name string) string
	MultiTenancy(class string) models.MultiTenancyConfig
	// Read calls reader with the class and its sharding state under the schema's read
	// lock. With retryIfClassNotFound it retries for a while when the class is missing,
	// to ride out a class that was just created on the leader.
	Read(class string, retryIfClassNotFound bool, reader func(*models.Class, *sharding.State) error) error
	// ReadSchema calls reader for every class with its version.
	ReadSchema(reader func(models.Class, uint64)) error
}

// AliasReader reads collection aliases.
type AliasReader interface {
	// ResolveAlias returns the class the alias points to, or "" if it is not an alias.
	ResolveAlias(alias string) string
	GetAliasesForClass(class string) []*models.Alias
	// Aliases returns every alias mapped to its class.
	Aliases() map[string]string
}

// ShardReader reads shard placement.
type ShardReader interface {
	// ShardOwner returns the node that owns the shard.
	ShardOwner(class, shard string) (string, error)
	// ShardReplicas returns the nodes holding a replica of the shard.
	ShardReplicas(class, shard string) ([]string, error)
	// ShardFromUUID returns the shard an object with the given UUID belongs to, or ""
	// when it cannot be resolved (e.g. the class is unknown).
	ShardFromUUID(class string, uuid []byte) string
	// Shards returns the names of all physical shards of the class.
	Shards(class string) ([]string, error)
	// LocalShards returns the names of the physical shards of the class that have a
	// replica on this node.
	LocalShards(class string) ([]string, error)
	// LocalActiveShardsCount returns the number of the class's shards on this node that
	// are active.
	LocalActiveShardsCount(class string) (int, error)
}

// UpdateWaiter waits for the local schema to catch up to a schema version.
type UpdateWaiter interface {
	// WaitForUpdate blocks until the local schema has applied version, or ctx is done.
	WaitForUpdate(ctx context.Context, version uint64) error
}

// VersionedReader reads the local schema once it has caught up to a schema version,
// typically one returned by a leader write. A version of 0 does not wait.
type VersionedReader interface {
	UpdateWaiter
	ClassInfoWithVersion(ctx context.Context, class string, version uint64) (ClassInfo, error)
	MultiTenancyWithVersion(ctx context.Context, class string, version uint64) (models.MultiTenancyConfig, error)
	ReadOnlyClassWithVersion(ctx context.Context, class string, version uint64) (*models.Class, error)
	ShardOwnerWithVersion(ctx context.Context, class, shard string, version uint64) (string, error)
	ShardFromUUIDWithVersion(ctx context.Context, class string, uuid []byte, version uint64) (string, error)
	ShardReplicasWithVersion(ctx context.Context, class, shard string, version uint64) ([]string, error)
	TenantsShardsWithVersion(ctx context.Context, version uint64, class string, tenants ...string) (map[string]string, error)
}

// ClassInfo summarizes a class as the local schema knows it.
type ClassInfo struct {
	Exists            bool
	MultiTenancy      models.MultiTenancyConfig
	ReplicationFactor int
	Tenants           int
	Properties        int
	ClassVersion      uint64
	ShardVersion      uint64
}

// Version returns the later of the class and shard versions.
func (ci *ClassInfo) Version() uint64 {
	return max(ci.ClassVersion, ci.ShardVersion)
}
