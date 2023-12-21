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

	"github.com/weaviate/weaviate/entities/models"
)

/// TODO-RAFT START
/// ClusterStatus: Does this function still make sense, considering that the old logic may not be applicable in the case of Raft
/// TODO-RAFT END

func (m *Manager) ClusterStatus(ctx context.Context) (*models.SchemaClusterStatus, error) {
	m.RLock()
	defer m.RUnlock()

	out := &models.SchemaClusterStatus{
		Hostname:         m.clusterState.LocalName(),
		IgnoreSchemaSync: m.clusterState.SchemaSyncIgnored(),
	}

	nodes := m.clusterState.AllNames()
	out.NodeCount = int64(len(nodes))
	if len(nodes) < 2 {
		out.Healthy = true
		return out, nil
	}

	out.Healthy = true
	return out, nil
}
