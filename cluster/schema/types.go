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

package schema

import (
	"context"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/entities/models"
)

type (
	// LoadLegacySchema returns the legacy schema
	LoadLegacySchema func() (map[string]types.ClassState, error)
)

// Indexer interface updates both the collection and its indices in the filesystem.
// This is distinct from updating metadata, which is handled through a different interface.
type Indexer interface {
	AddClass(api.AddClassRequest) error
	UpdateClass(api.UpdateClassRequest) error
	DeleteClass(className string, hasFrozen bool) error
	AddProperty(class string, req api.AddPropertyRequest) error
	UpdateProperty(class string, req api.UpdatePropertyRequest) error
	AddTenants(class string, req *api.AddTenantsRequest) error
	// UpdateTenants receives, per tenant entering FREEZING, the status it held
	// beforehand — an aborted freeze reports that status back to the schema. The
	// map is complete on entry: the schema update that fills it runs first.
	UpdateTenants(class string, req *api.UpdateTenantsRequest, preFreezeStatuses map[string]string) error
	DeleteTenants(class string, tenants []*models.Tenant) error
	UpdateTenantsProcess(class string, req *api.TenantProcessRequest) error
	UpdateShardStatus(*api.UpdateShardStatusRequest) error
	// AddReplicaToShard materializes a replica the schema just assigned to this
	// node, with no replica movement under way.
	AddReplicaToShard(class, shard, targetNode string) error
	// AddReplicaToShardForMovement materializes the target shard of a replica
	// movement, which the schema assigns while the movement is still running.
	AddReplicaToShardForMovement(class, shard, targetNode string) error
	DeleteReplicaFromShard(class, shard, targetNode string) error
	ReconcileAsyncReplicationForShard(class, shard string) error
	LoadShard(class, shard string)     // is a no-op
	ShutdownShard(class, shard string) // is a no-op
	GetShardsStatus(class, tenant string) (models.ShardStatusList, error)

	TriggerSchemaUpdateCallbacks()

	// ReloadLocalDB reloads the local database using the latest schema.
	ReloadLocalDB(ctx context.Context, all []api.UpdateClassRequest) error

	// RestoreClassDir restores classes on the filesystem directly from the temporary class backup stored on disk.
	RestoreClassDir(class string) error
	Open(context.Context) error
	Close(context.Context) error
}

// Parser parses concrete class fields after deserialization
type Parser interface {
	// ParseClassUpdate parses a class after unmarshaling by setting concrete types for the fields
	ParseClass(class *models.Class) error

	// ParseClass parses new updates by providing the current class data.
	ParseClassUpdate(class, update *models.Class) (*models.Class, error)
}
