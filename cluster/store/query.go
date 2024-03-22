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

	cmd "github.com/weaviate/weaviate/cluster/proto/cluster"
)

func (st *Store) Query(req *cmd.QueryRequest) (*cmd.QueryResponse, error) {
	st.log.Debug("server.query", "type", req.Type)

	var payload []byte
	var err error
	switch req.Type {
	case cmd.QueryRequest_TYPE_GET_READONLY_CLASS:
		payload, err = st.QueryReadOnlyClass(req)
		if err != nil {
			return &cmd.QueryResponse{}, fmt.Errorf("could not get read only class: %w", err)
		}
	case cmd.QueryRequest_TYPE_GET_SCHEMA:
		payload, err = st.QueryGetSchema()
		if err != nil {
			return &cmd.QueryResponse{}, fmt.Errorf("could not get schema: %w", err)
		}
	default:
		// This could occur when a new command has been introduced in a later app version
		// At this point, we need to panic so that the app undergo an upgrade during restart
		const msg = "consider upgrading to newer version"
		st.log.Error("unknown command", "type", req.Type, "more", msg)
		return &cmd.QueryResponse{}, fmt.Errorf("unknown command type %s: %s", req.Type, msg)
	}
	return &cmd.QueryResponse{Payload: payload}, nil
}

func (st *Store) QueryReadOnlyClass(req *cmd.QueryRequest) ([]byte, error) {
	// Validate that the subcommand is the correct type
	subCommand := cmd.QueryReadOnlyClassRequest{}
	if err := json.Unmarshal(req.SubCommand, &subCommand); err != nil {
		return []byte{}, fmt.Errorf("%w: %w", errBadRequest, err)
	}

	// Read the meta class to get both the class and sharding information
	metaClass := st.db.Schema.metaClass(subCommand.Class)
	if metaClass == nil {
		return []byte{}, nil
	}

	// Build the response, marshal and return
	response := cmd.QueryReadOnlyClassResponse{Class: &metaClass.Class, State: &metaClass.Sharding}
	payload, err := json.Marshal(&response)
	if err != nil {
		return []byte{}, fmt.Errorf("could not marshal query response: %w", err)
	}
	return payload, nil
}

func (st *Store) QueryGetSchema() ([]byte, error) {
	// Build the response, marshal and return
	response := cmd.QueryGetSchemaResponse{Schema: st.db.Schema.ReadOnlySchema()}
	payload, err := json.Marshal(&response)
	if err != nil {
		return []byte{}, fmt.Errorf("could not marshal query response: %w", err)
	}
	return payload, nil
}
