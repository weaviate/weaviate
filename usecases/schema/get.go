//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package schema

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// GetSchema retrieves a locally cached copy of the schema
func (m *Manager) GetSchema(principal *models.Principal) (schema.Schema, error) {
	err := m.Authorizer.Authorize(principal, "list", "schema/*")
	if err != nil {
		return schema.Schema{}, err
	}

	return schema.Schema{
		Objects: m.state.ObjectSchema,
	}, nil
}

// GetSchemaSkipAuth can never be used as a response to a user request as it
// could leak the schema to an unauthorized user, is intended to be used for
// non-user triggered processes, such as regular updates / maintenance / etc
func (m *Manager) GetSchemaSkipAuth() schema.Schema {
	return schema.Schema{
		Objects: m.state.ObjectSchema,
	}
}

func (m *Manager) getSchema() schema.Schema {
	return schema.Schema{
		Objects: m.state.ObjectSchema,
	}
}

func (m *Manager) IndexedInverted(className, propertyName string) bool {
	class := m.getClassByName(className)
	if class == nil {
		return false
	}

	for _, prop := range class.Properties {
		if prop.Name == propertyName {
			if prop.IndexInverted == nil {
				return true
			}

			return *prop.IndexInverted
		}
	}

	return false
}

func (m *Manager) GetClass(ctx context.Context, principal *models.Principal,
	name string,
) (*models.Class, error) {
	err := m.Authorizer.Authorize(principal, "list", "schema/*")
	if err != nil {
		return nil, err
	}
	return m.getClassByName(name), nil
}

func (m *Manager) getClassByName(name string) *models.Class {
	s := schema.Schema{
		Objects: m.state.ObjectSchema,
	}

	return s.FindClassByName(schema.ClassName(name))
}

func (m *Manager) ShardingState(className string) *sharding.State {
	m.shardingStateLock.RLock()
	copiedState := m.state.ShardingState[className].DeepCopy()
	m.shardingStateLock.RUnlock()
	return &copiedState
}

// ResolveParentNodes gets all replicas for a specific class shard and resolves their names
//
// it returns map[node_name] node_address where node_address = "" if can't resolve node_name
func (m *Manager) ResolveParentNodes(class, shardName string) (map[string]string, error) {
	shard, ok := m.ShardingState(class).Physical[shardName]
	if !ok {
		return nil, fmt.Errorf("sharding state not found")
	}

	if len(shard.BelongsToNodes) == 0 {
		return nil, nil
	}

	name2Addr := make(map[string]string, len(shard.BelongsToNodes))
	for _, node := range shard.BelongsToNodes {
		host, _ := m.clusterState.NodeHostname(node)
		name2Addr[node] = host
	}
	return name2Addr, nil
}

func (m *Manager) Nodes() []string {
	return m.clusterState.AllNames()
}

func (m *Manager) NodeName() string {
	return m.clusterState.LocalName()
}

func (m *Manager) ClusterHealthScore() int {
	return m.clusterState.ClusterHealthScore()
}

func (m *Manager) GetShardsStatus(ctx context.Context, principal *models.Principal,
	className string,
) (models.ShardStatusList, error) {
	err := m.Authorizer.Authorize(principal, "list", fmt.Sprintf("schema/%s/shards", className))
	if err != nil {
		return nil, err
	}

	shardsStatus, err := m.migrator.GetShardsStatus(ctx, className)
	if err != nil {
		return nil, err
	}

	resp := models.ShardStatusList{}

	for name, status := range shardsStatus {
		resp = append(resp, &models.ShardStatusGetResponse{
			Name:   name,
			Status: status,
		})
	}

	return resp, nil
}
