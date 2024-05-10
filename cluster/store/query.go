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
	"encoding/json"
	"fmt"

	"github.com/sirupsen/logrus"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

func (st *Store) Query(req *cmd.QueryRequest) (*cmd.QueryResponse, error) {
	st.log.WithField("type", req.Type).Debug("server.query")

	var payload []byte
	var err error
	switch req.Type {
	case cmd.QueryRequest_TYPE_GET_CLASSES:
		payload, err = st.QueryReadOnlyClasses(req)
		if err != nil {
			return &cmd.QueryResponse{}, fmt.Errorf("could not get read only class: %w", err)
		}
	case cmd.QueryRequest_TYPE_GET_SCHEMA:
		payload, err = st.QuerySchema()
		if err != nil {
			return &cmd.QueryResponse{}, fmt.Errorf("could not get schema: %w", err)
		}
	case cmd.QueryRequest_TYPE_GET_TENANTS:
		payload, err = st.QueryTenants(req)
		if err != nil {
			return &cmd.QueryResponse{}, fmt.Errorf("could not get tenants: %w", err)
		}
	case cmd.QueryRequest_TYPE_GET_SHARD_OWNER:
		payload, err = st.QueryShardOwner(req)
		if err != nil {
			return &cmd.QueryResponse{}, fmt.Errorf("could not get shard owner: %w", err)
		}
	case cmd.QueryRequest_TYPE_GET_TENANTS_SHARDS:
		payload, err = st.QueryTenantsShards(req)
		if err != nil {
			return &cmd.QueryResponse{}, fmt.Errorf("could not get tenant shard: %w", err)
		}
	case cmd.QueryRequest_TYPE_GET_SHARDING_STATE:
		payload, err = st.QueryShardingState(req)
		if err != nil {
			return &cmd.QueryResponse{}, fmt.Errorf("could not get sharding state: %w", err)
		}

	default:
		// This could occur when a new command has been introduced in a later app version
		// At this point, we need to panic so that the app undergo an upgrade during restart
		const msg = "consider upgrading to newer version"
		st.log.WithFields(logrus.Fields{
			"type": req.Type,
			"more": msg,
		}).Error("unknown command")
		return &cmd.QueryResponse{}, fmt.Errorf("unknown command type %s: %s", req.Type, msg)
	}
	return &cmd.QueryResponse{Payload: payload}, nil
}

func (st *Store) QueryReadOnlyClasses(req *cmd.QueryRequest) ([]byte, error) {
	// Validate that the subcommand is the correct type
	subCommand := cmd.QueryReadOnlyClassesRequest{}
	if err := json.Unmarshal(req.SubCommand, &subCommand); err != nil {
		return []byte{}, fmt.Errorf("%w: %w", errBadRequest, err)
	}

	// Read the meta class to get both the class and sharding information
	vclasses := st.db.Schema.ReadOnlyClasses(subCommand.Classes...)
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

func (st *Store) QuerySchema() ([]byte, error) {
	// Build the response, marshal and return
	response := cmd.QuerySchemaResponse{Schema: st.db.Schema.ReadOnlySchema()}
	payload, err := json.Marshal(&response)
	if err != nil {
		return []byte{}, fmt.Errorf("could not marshal query response: %w", err)
	}
	return payload, nil
}

func (st *Store) QueryTenants(req *cmd.QueryRequest) ([]byte, error) {
	// Validate that the subcommand is the correct type
	subCommand := cmd.QueryTenantsRequest{}
	if err := json.Unmarshal(req.SubCommand, &subCommand); err != nil {
		return []byte{}, fmt.Errorf("%w: %w", errBadRequest, err)
	}

	// Read the tenants
	tenants, err := st.db.Schema.getTenants(subCommand.Class, subCommand.Tenants)
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

func (st *Store) QueryShardOwner(req *cmd.QueryRequest) ([]byte, error) {
	// Validate that the subcommand is the correct type
	subCommand := cmd.QueryShardOwnerRequest{}
	if err := json.Unmarshal(req.SubCommand, &subCommand); err != nil {
		return []byte{}, fmt.Errorf("%w: %w", errBadRequest, err)
	}

	// Read the meta class to get both the class and sharding information
	owner, version, err := st.db.Schema.ShardOwner(subCommand.Class, subCommand.Shard)
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

func (st *Store) QueryTenantsShards(req *cmd.QueryRequest) ([]byte, error) {
	// Validate that the subcommand is the correct type
	subCommand := cmd.QueryTenantsShardsRequest{}
	if err := json.Unmarshal(req.SubCommand, &subCommand); err != nil {
		return []byte{}, fmt.Errorf("%w: %w", errBadRequest, err)
	}

	// Read the meta class to get both the class and sharding information
	tenants, version := st.db.Schema.TenantsShards(subCommand.Class, subCommand.Tenants...)
	// Build the response, marshal and return
	response := cmd.QueryTenantsShardsResponse{TenantsActivityStatus: tenants, SchemaVersion: version}
	payload, err := json.Marshal(&response)
	if err != nil {
		return []byte{}, fmt.Errorf("could not marshal query response: %w", err)
	}
	return payload, nil
}

func (st *Store) QueryShardingState(req *cmd.QueryRequest) ([]byte, error) {
	// Validate that the subcommand is the correct type
	subCommand := cmd.QueryShardingStateRequest{}
	if err := json.Unmarshal(req.SubCommand, &subCommand); err != nil {
		return []byte{}, fmt.Errorf("%w: %w", errBadRequest, err)
	}

	state, version := st.db.Schema.CopyShardingState(subCommand.Class)
	// Build the response, marshal and return
	response := cmd.QueryShardingStateResponse{State: state, Version: version}
	payload, err := json.Marshal(&response)
	if err != nil {
		return []byte{}, fmt.Errorf("could not marshal query response: %w", err)
	}
	return payload, nil
}
