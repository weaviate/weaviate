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

package api

import (
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/versioned"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type AddClassRequest struct {
	Class *models.Class
	State *sharding.State
}

type UpdateClassRequest struct {
	Class *models.Class
	State *sharding.State
}

type AddPropertyRequest struct {
	Properties []*models.Property
}

type DeleteClassRequest struct {
	Name string
}

type UpdateShardStatusRequest struct {
	Class, Shard, Status string
	SchemaVersion        uint64
}

type AddReplicaToShard struct {
	Class, Shard, TargetNode string
	SchemaVersion            uint64
}

type DeleteReplicaFromShard struct {
	Class, Shard, TargetNode string
	SchemaVersion            uint64
}

type QueryReadOnlyClassesRequest struct {
	Classes []string
}

type QueryReadOnlyClassResponse struct {
	Classes map[string]versioned.Class
}

type QueryTenantsRequest struct {
	Class   string
	Tenants []string // If empty, all tenants are returned
}

type TenantWithVersion struct {
	ShardVersion uint64
	Tenant       *models.Tenant
}

type QueryTenantsResponse struct {
	ShardVersion uint64
	Tenants      []*models.Tenant
}

type QuerySchemaResponse struct {
	Schema models.Schema
}

type QueryCollectionsCountResponse struct {
	Count int
}

type QueryShardOwnerRequest struct {
	Class, Shard string
}

type QueryShardOwnerResponse struct {
	ShardVersion uint64
	Owner        string
}

type QueryTenantsShardsRequest struct {
	Class   string
	Tenants []string
}

type QueryTenantsShardsResponse struct {
	TenantsActivityStatus map[string]string // map[tenant]status
	SchemaVersion         uint64
}

type QueryShardingStateRequest struct {
	Class string
}

type QueryShardingStateResponse struct {
	State   *sharding.State
	Version uint64
}

type QueryClassVersionsRequest struct {
	Classes []string
}

type QueryClassVersionsResponse struct {
	// Classes is a map of class name to the class version
	Classes map[string]uint64
}
