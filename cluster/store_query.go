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
	"context"
	"fmt"

	"github.com/sirupsen/logrus"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

func (st *Store) Query(req *cmd.QueryRequest) (*cmd.QueryResponse, error) {
	st.log.WithFields(logrus.Fields{"type": req.Type, "type_name": req.Type.String()}).Debug("server.query")

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
	case cmd.QueryRequest_TYPE_GET_COLLECTIONS_COUNT:
		payload, err = st.schemaManager.QueryCollectionsCount()
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
	case cmd.QueryRequest_TYPE_HAS_PERMISSION:
		payload, err = st.authZManager.HasPermission(req)
		if err != nil {
			return &cmd.QueryResponse{}, fmt.Errorf("could not get RBAC permissions: %w", err)
		}
	case cmd.QueryRequest_TYPE_GET_ROLES:
		payload, err = st.authZManager.GetRoles(req)
		if err != nil {
			return &cmd.QueryResponse{}, fmt.Errorf("could not get RBAC permissions: %w", err)
		}
	case cmd.QueryRequest_TYPE_GET_ROLES_FOR_USER:
		payload, err = st.authZManager.GetRolesForUser(req)
		if err != nil {
			return &cmd.QueryResponse{}, fmt.Errorf("could not get RBAC permissions: %w", err)
		}
	case cmd.QueryRequest_TYPE_GET_USERS_FOR_ROLE:
		payload, err = st.authZManager.GetUsersForRole(req)
		if err != nil {
			return &cmd.QueryResponse{}, fmt.Errorf("could not get RBAC permissions: %w", err)
		}
	case cmd.QueryRequest_TYPE_GET_CLASS_VERSIONS:
		payload, err = st.schemaManager.QueryClassVersions(req)
		if err != nil {
			return &cmd.QueryResponse{}, fmt.Errorf("could not get class versions: %w", err)
		}
	case cmd.QueryRequest_TYPE_GET_USERS:
		payload, err = st.dynUserManager.GetUsers(req)
		if err != nil {
			return &cmd.QueryResponse{}, fmt.Errorf("could not get dynamic user: %w", err)
		}
	case cmd.QueryRequest_TYPE_USER_IDENTIFIER_EXISTS:
		payload, err = st.dynUserManager.GetUsers(req)
		if err != nil {
			return &cmd.QueryResponse{}, fmt.Errorf("could not check user identifier: %w", err)
		}
	case cmd.QueryRequest_TYPE_GET_REPLICATION_DETAILS:
		payload, err = st.replicationManager.GetReplicationDetailsByReplicationId(req)
		if err != nil {
			return &cmd.QueryResponse{}, fmt.Errorf("could not get replication operation details: %w", err)
		}
	case cmd.QueryRequest_TYPE_GET_REPLICATION_DETAILS_BY_COLLECTION:
		payload, err = st.replicationManager.GetReplicationDetailsByCollection(req)
		if err != nil {
			return &cmd.QueryResponse{}, fmt.Errorf("could not get replication operation details by collection: %w", err)
		}
	case cmd.QueryRequest_TYPE_GET_REPLICATION_DETAILS_BY_COLLECTION_AND_SHARD:
		payload, err = st.replicationManager.GetReplicationDetailsByCollectionAndShard(req)
		if err != nil {
			return &cmd.QueryResponse{}, fmt.Errorf("could not get replication operation details by collection and shards: %w", err)
		}
	case cmd.QueryRequest_TYPE_GET_REPLICATION_DETAILS_BY_TARGET_NODE:
		payload, err = st.replicationManager.GetReplicationDetailsByTargetNode(req)
		if err != nil {
			return &cmd.QueryResponse{}, fmt.Errorf("could not get replication operation details by target node: %w", err)
		}
	case cmd.QueryRequest_TYPE_GET_SHARDING_STATE_BY_COLLECTION:
		payload, err = st.replicationManager.QueryShardingStateByCollection(req)
		if err != nil {
			return &cmd.QueryResponse{}, fmt.Errorf("could not get sharding state by collection: %w", err)
		}
	case cmd.QueryRequest_TYPE_GET_SHARDING_STATE_BY_COLLECTION_AND_SHARD:
		payload, err = st.replicationManager.QueryShardingStateByCollectionAndShard(req)
		if err != nil {
			return &cmd.QueryResponse{}, fmt.Errorf("could not get sharding state by collection and shard: %w", err)
		}
	case cmd.QueryRequest_TYPE_GET_ALL_REPLICATION_DETAILS:
		payload, err = st.replicationManager.GetAllReplicationDetails(req)
		if err != nil {
			return &cmd.QueryResponse{}, fmt.Errorf("could not get all replication operation details: %w", err)
		}
	case cmd.QueryRequest_TYPE_DISTRIBUTED_TASK_LIST:
		payload, err = st.distributedTasksManager.ListDistributedTasksPayload(context.Background())
		if err != nil {
			return &cmd.QueryResponse{}, fmt.Errorf("could not get distributed task list: %w", err)
		}
	case cmd.QueryRequest_TYPE_GET_REPLICATION_OPERATION_STATE:
		payload, err = st.replicationManager.GetReplicationOperationState(req)
		if err != nil {
			return &cmd.QueryResponse{}, fmt.Errorf("could not get replication operation state: %w", err)
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
