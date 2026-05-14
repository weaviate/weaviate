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

// Field names for UpdatePropertyRequest.FieldsToUpdate.
//
// When the FieldsToUpdate slice on an UpdatePropertyRequest is non-empty,
// the FSM merges ONLY the listed property fields onto the in-memory class.
// Fields not listed are taken from the existing class state, so concurrent
// updaters that touch different fields cannot clobber each other.
//
// An empty / nil FieldsToUpdate slice preserves the historical "merge
// everything" semantics.
//
// Wire-format compatibility (rolling upgrades + WAL replay):
//
//   - Old leaders/clients that emit commands without FieldsToUpdate
//     unmarshal to nil here → full merge. This is the legacy path; WAL
//     entries written before this field existed still replay correctly.
//   - Old followers receiving a new masked command silently drop the
//     unknown JSON field and fall back to full merge. This is safe
//     because callers that pass a mask still populate the request's
//     Property with the full set of fields (read-modify-write off the
//     fresh class state), so an old follower behaves exactly as it does
//     today: a "replace all" merge with a small TOCTOU window. The race
//     is only fully closed once every node honors the mask, but the
//     upgrade window never regresses behavior.
//   - New followers honor the mask and skip the merge of unmasked fields
//     entirely. Once the rolling upgrade completes, the race is closed
//     cluster-wide.
//
// The constants are intentionally short tags rather than struct field
// names so they're stable even if the Go struct is refactored.
const (
	PropertyFieldIndexFilterable   = "indexFilterable"
	PropertyFieldIndexSearchable   = "indexSearchable"
	PropertyFieldIndexRangeFilters = "indexRangeFilters"
	PropertyFieldTokenization      = "tokenization"
	PropertyFieldNestedProperties  = "nestedProperties"
)

type UpdatePropertyRequest struct {
	Property *models.Property
	// FieldsToUpdate, when non-empty, restricts the FSM merge to ONLY the
	// listed field tags (see PropertyField* constants above). When empty or
	// nil, the FSM falls back to merging every supported field (legacy
	// "replace all" semantics). See the constants doc for the compatibility
	// contract.
	FieldsToUpdate []string `json:"FieldsToUpdate,omitempty"`
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

type QueryCollectionsCountRequest struct {
	// Namespace selects classes belonging to that namespace; empty counts all.
	Namespace string
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

type QueryResolveAliasRequest struct {
	Alias string
}

type QueryResolveAliasResponse struct {
	Class string
}

type QueryGetAliasesRequest struct {
	Alias, Class string
}

type QueryGetAliasesResponse struct {
	Aliases map[string]string
}
