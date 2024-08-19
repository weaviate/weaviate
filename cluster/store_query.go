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

package cluster

import (
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
		payload, err = st.schemaManager.QueryReadOnlyClasses(req)
		if err != nil {
			return &cmd.QueryResponse{}, fmt.Errorf("could not get read only class: %w", err)
		}
	case cmd.QueryRequest_TYPE_GET_SCHEMA:
		payload, err = st.schemaManager.QuerySchema()
		if err != nil {
			return &cmd.QueryResponse{}, fmt.Errorf("could not get schema: %w", err)
		}
	case cmd.QueryRequest_TYPE_GET_TENANTS:
		payload, err = st.schemaManager.QueryTenants(req)
		if err != nil {
			return &cmd.QueryResponse{}, fmt.Errorf("could not get tenants: %w", err)
		}
	case cmd.QueryRequest_TYPE_GET_SHARD_OWNER:
		payload, err = st.schemaManager.QueryShardOwner(req)
		if err != nil {
			return &cmd.QueryResponse{}, fmt.Errorf("could not get shard owner: %w", err)
		}
	case cmd.QueryRequest_TYPE_GET_TENANTS_SHARDS:
		payload, err = st.schemaManager.QueryTenantsShards(req)
		if err != nil {
			return &cmd.QueryResponse{}, fmt.Errorf("could not get tenant shard: %w", err)
		}
	case cmd.QueryRequest_TYPE_GET_SHARDING_STATE:
		payload, err = st.schemaManager.QueryShardingState(req)
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
