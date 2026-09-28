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

// Package leader defines the schema operations that go through the RAFT leader: reads
// that must reflect the leader's committed state, and every schema write. Each call is
// a network round-trip to the leader and fails while no leader is elected, so callers
// that can live with an eventually consistent answer should read through package local.
//
// Nothing this package imports may import it back.
package leader

import (
	"context"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/versioned"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// Schema is every schema operation that goes through the RAFT leader.
type Schema interface {
	SchemaReader
	SchemaWriter
}

// SchemaReader reads the schema from the RAFT leader. The FromLeader suffix keeps the
// round-trip visible at every call site and apart from the local reads of the same
// name. A returned uint64 is the schema version the answer reflects.
type SchemaReader interface {
	ClassReader
	TenantReader
	ShardReader
	AliasReader
}

// ClassReader reads class metadata from the leader.
type ClassReader interface {
	ReadOnlyClassesFromLeader(classes ...string) (map[string]versioned.Class, error)
	SchemaFromLeader() (models.Schema, error)
	// CollectionsCountFromLeader counts collections. An empty namespace counts the
	// whole cluster; a non-empty one restricts the count to that namespace.
	CollectionsCountFromLeader(namespace string) (int, error)
	ClassVersionsFromLeader(classes ...string) (map[string]uint64, error)
}

// TenantReader reads tenants from the leader. It never activates a tenant.
type TenantReader interface {
	TenantsFromLeader(class string, tenants []string) ([]*models.Tenant, uint64, error)
	TenantsShardsFromLeader(class string, tenants ...string) (map[string]string, uint64, error)
}

// ShardReader reads shard placement from the leader.
type ShardReader interface {
	ShardOwnerFromLeader(class, shard string) (string, uint64, error)
	ShardingStateFromLeader(class string) (*sharding.State, uint64, error)
}

// AliasReader reads collection aliases from the leader.
type AliasReader interface {
	AliasFromLeader(ctx context.Context, alias string) (*models.Alias, error)
	AliasesFromLeader(ctx context.Context, alias string, class *models.Class) ([]*models.Alias, error)
}

// SchemaWriter changes the schema through the RAFT leader. Every method returns the
// schema version the change was committed at; pass it to local.VersionedReader before
// reading the change back from the local schema.
type SchemaWriter interface {
	ClassWriter
	TenantWriter
	ShardWriter
	AliasWriter
}

// ClassWriter changes classes and their properties.
type ClassWriter interface {
	AddClass(ctx context.Context, cls *models.Class, ss *sharding.State) (uint64, error)
	RestoreClass(ctx context.Context, cls *models.Class, ss *sharding.State) (uint64, error)
	UpdateClass(ctx context.Context, cls *models.Class, ss *sharding.State) (uint64, error)
	DeleteClass(ctx context.Context, name string) (uint64, error)
	AddProperty(ctx context.Context, class string, p ...*models.Property) (uint64, error)
	// UpdateProperty merges property into the named class. When fields is non-empty,
	// the RAFT FSM only merges the listed property fields (see api.PropertyField*
	// constants); fields not listed keep their existing values. An empty fields keeps
	// the legacy "replace every field" semantics.
	UpdateProperty(ctx context.Context, class string, property *models.Property, fields ...string) (uint64, error)
	// UpdatePropertyFromMigration is the variant of UpdateProperty used by the
	// distributed-task scheduler's reindex completion path. It sets
	// [cmd.UpdatePropertyRequest.FromInFlightMigration] so the schema FSM's cross-FSM
	// MutationGuard, which blocks property mutations while a reindex on the same
	// property is STARTED or FINALIZING, lets the migration's own schema flip through:
	// OnTaskCompleted fires while the task is still FINALIZING. Public REST / gRPC
	// handlers must not call it.
	UpdatePropertyFromMigration(ctx context.Context, class string, property *models.Property, fields ...string) (uint64, error)
}

// TenantWriter changes tenants.
type TenantWriter interface {
	AddTenants(ctx context.Context, class string, req *cmd.AddTenantsRequest) (uint64, error)
	UpdateTenants(ctx context.Context, class string, req *cmd.UpdateTenantsRequest) (uint64, error)
	DeleteTenants(ctx context.Context, class string, req *cmd.DeleteTenantsRequest) (uint64, error)
}

// ShardWriter changes shard state.
type ShardWriter interface {
	UpdateShardStatus(ctx context.Context, class, shard, status string) (uint64, error)
}

// AliasWriter changes collection aliases.
type AliasWriter interface {
	CreateAlias(ctx context.Context, alias string, class *models.Class) (uint64, error)
	ReplaceAlias(ctx context.Context, alias *models.Alias, newClass *models.Class) (uint64, error)
	DeleteAlias(ctx context.Context, alias string) (uint64, error)
}
