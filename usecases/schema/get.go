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
	"context"
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// GetSchema retrieves a locally cached copy of the schema
func (m *Manager) GetSchema(principal *models.Principal) (schema.Schema, error) {
	err := m.Authorizer.Authorize(principal, "list", "schema/*")
	if err != nil {
		return schema.Schema{}, err
	}

	return m.getSchema(), nil
}

// GetSchemaSkipAuth can never be used as a response to a user request as it
// could leak the schema to an unauthorized user, is intended to be used for
// non-user triggered processes, such as regular updates / maintenance / etc
func (m *Manager) GetSchemaSkipAuth() schema.Schema { return m.getSchema() }

func (m *Manager) getSchema() schema.Schema {
	return schema.Schema{
		Objects: m.schemaCache.readOnlySchema(),
	}
}

func (m *Manager) IndexedInverted(className, propertyName string) bool {
	class := m.getClassByName(className)
	if class == nil {
		return false
	}
	prop, _ := schema.GetPropertyByName(class, propertyName)
	if prop == nil {
		return false
	}
	return inverted.HasInvertedIndex(prop)
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
	c, _ := m.schemaCache.readOnlyClass(name)
	return c
}

// ResolveParentNodes gets all replicas for a specific class shard and resolves their names
//
// it returns map[node_name] node_address where node_address = "" if can't resolve node_name
func (m *Manager) ResolveParentNodes(class, shardName string) (map[string]string, error) {
	nodes, err := m.ShardReplicas(class, shardName)
	if err != nil {
		return nil, fmt.Errorf("get replicas from schema: %w", err)
	}

	if len(nodes) == 0 {
		return nil, nil
	}

	name2Addr := make(map[string]string, len(nodes))
	for _, node := range nodes {
		if node != "" {
			host, _ := m.clusterState.NodeHostname(node)
			name2Addr[node] = host
		}
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
	className, tenant string,
) (models.ShardStatusList, error) {
	err := m.Authorizer.Authorize(principal, "list", fmt.Sprintf("schema/%s/shards", className))
	if err != nil {
		return nil, err
	}

	shardsStatus, err := m.migrator.GetShardsStatus(ctx, className, tenant)
	if err != nil {
		return nil, err
	}
	shardsQueueSize, err := m.migrator.GetShardsQueueSize(ctx, className, tenant)
	if err != nil {
		return nil, err
	}

	resp := models.ShardStatusList{}

	for name, status := range shardsStatus {
		resp = append(resp, &models.ShardStatusGetResponse{
			Name:            name,
			Status:          status,
			VectorQueueSize: shardsQueueSize[name],
		})
	}

	return resp, nil
}
