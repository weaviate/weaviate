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
	"encoding/json"
	"fmt"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

func (sm *SchemaManager) QueryReadOnlyClasses(req *cmd.QueryRequest) ([]byte, error) {
	// Validate that the subcommand is the correct type
	subCommand := cmd.QueryReadOnlyClassesRequest{}
	if err := json.Unmarshal(req.SubCommand, &subCommand); err != nil {
		return []byte{}, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	// Read the meta class to get both the class and sharding information
	vclasses := sm.schema.ReadOnlyClasses(subCommand.Classes...)
	if len(vclasses) == 0 {
		return []byte{}, nil
	}

	// Build the response, marshal and return
	response := cmd.QueryReadOnlyClassResponse{
		Classes: vclasses,
	}
	payload, err := json.Marshal(&response)
	if err != nil {
		return []byte{}, fmt.Errorf("could not marshal query response: %w", err)
	}
	return payload, nil
}

func (sm *SchemaManager) QuerySchema() ([]byte, error) {
	// Build the response, marshal and return
	response := cmd.QuerySchemaResponse{Schema: sm.schema.ReadOnlySchema()}
	payload, err := json.Marshal(&response)
	if err != nil {
		return []byte{}, fmt.Errorf("could not marshal query response: %w", err)
	}
	return payload, nil
}

func (sm *SchemaManager) QueryCollectionsCount() ([]byte, error) {
	// Build the response, marshal and return
	response := cmd.QueryCollectionsCountResponse{Count: sm.schema.CollectionsCount()}
	payload, err := json.Marshal(&response)
	if err != nil {
		return []byte{}, fmt.Errorf("could not marshal query response: %w", err)
	}
	return payload, nil
}

func (sm *SchemaManager) QueryTenants(req *cmd.QueryRequest) ([]byte, error) {
	// Validate that the subcommand is the correct type
	subCommand := cmd.QueryTenantsRequest{}
	if err := json.Unmarshal(req.SubCommand, &subCommand); err != nil {
		return []byte{}, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	// Read the tenants
	tenants, err := sm.schema.getTenants(subCommand.Class, subCommand.Tenants)
	if err != nil {
		return []byte{}, fmt.Errorf("could not get tenants: %w", err)
	}

	// Build the response, marshal and return
	response := cmd.QueryTenantsResponse{Tenants: tenants}
	payload, err := json.Marshal(&response)
	if err != nil {
		return []byte{}, fmt.Errorf("could not marshal query response: %w", err)
	}
	return payload, nil
}

func (sm *SchemaManager) QueryShardOwner(req *cmd.QueryRequest) ([]byte, error) {
	// Validate that the subcommand is the correct type
	subCommand := cmd.QueryShardOwnerRequest{}
	if err := json.Unmarshal(req.SubCommand, &subCommand); err != nil {
		return []byte{}, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	// Read the meta class to get both the class and sharding information
	owner, version, err := sm.schema.ShardOwner(subCommand.Class, subCommand.Shard)
	if err != nil {
		return []byte{}, err
	}

	// Build the response, marshal and return
	response := cmd.QueryShardOwnerResponse{ShardVersion: version, Owner: owner}
	payload, err := json.Marshal(&response)
	if err != nil {
		return []byte{}, fmt.Errorf("could not marshal query response: %w", err)
	}
	return payload, nil
}

func (sm *SchemaManager) QueryTenantsShards(req *cmd.QueryRequest) ([]byte, error) {
	// Validate that the subcommand is the correct type
	subCommand := cmd.QueryTenantsShardsRequest{}
	if err := json.Unmarshal(req.SubCommand, &subCommand); err != nil {
		return []byte{}, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	// Read the meta class to get both the class and sharding information
	tenants, version := sm.schema.TenantsShards(subCommand.Class, subCommand.Tenants...)
	// Build the response, marshal and return
	response := cmd.QueryTenantsShardsResponse{TenantsActivityStatus: tenants, SchemaVersion: version}
	payload, err := json.Marshal(&response)
	if err != nil {
		return []byte{}, fmt.Errorf("could not marshal query response: %w", err)
	}
	return payload, nil
}

func (sm *SchemaManager) QueryShardingState(req *cmd.QueryRequest) ([]byte, error) {
	// Validate that the subcommand is the correct type
	subCommand := cmd.QueryShardingStateRequest{}
	if err := json.Unmarshal(req.SubCommand, &subCommand); err != nil {
		return []byte{}, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	state, version := sm.schema.CopyShardingState(subCommand.Class)
	// Build the response, marshal and return
	response := cmd.QueryShardingStateResponse{State: state, Version: version}
	payload, err := json.Marshal(&response)
	if err != nil {
		return []byte{}, fmt.Errorf("could not marshal query response: %w", err)
	}
	return payload, nil
}

// QueryClassVersions returns the versions of the requested classes
func (sm *SchemaManager) QueryClassVersions(req *cmd.QueryRequest) ([]byte, error) {
	// Validate that the subcommand is the correct type
	subCommand := cmd.QueryClassVersionsRequest{}
	if err := json.Unmarshal(req.SubCommand, &subCommand); err != nil {
		return []byte{}, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	// Read the meta class to get the class version
	vclasses := sm.schema.ReadOnlyClasses(subCommand.Classes...)
	if len(vclasses) == 0 {
		return []byte{}, nil
	}

	// Build the response, marshal and return
	classVersions := make(map[string]uint64, len(vclasses))
	for _, vclass := range vclasses {
		classVersions[vclass.Class.Class] = vclass.Version
	}
	response := cmd.QueryClassVersionsResponse{
		Classes: classVersions,
	}
	payload, err := json.Marshal(&response)
	if err != nil {
		return []byte{}, fmt.Errorf("could not marshal query response: %w", err)
	}
	return payload, nil
}
