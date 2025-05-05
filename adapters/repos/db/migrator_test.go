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

package db

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestUpdateIndexTenants(t *testing.T) {
	tests := []struct {
		name           string
		originalStatus string
		incomingStatus string
		expectedStatus storagestate.Status
		getClass       bool
	}{
		{
			name:           "when tenant is marked as COLD in incoming state while being HOT in original index",
			originalStatus: models.TenantActivityStatusHOT,
			incomingStatus: models.TenantActivityStatusCOLD,
			expectedStatus: storagestate.StatusShutdown,
		},
		{
			name:           "when tenant is marked as HOT in incoming state while being COLD in original index",
			originalStatus: models.TenantActivityStatusCOLD,
			incomingStatus: models.TenantActivityStatusHOT,
			expectedStatus: storagestate.StatusReady,
			getClass:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockSchemaGetter := schemaUC.NewMockSchemaGetter(t)
			mockSchemaGetter.On("NodeName").Return("node1")

			class := &models.Class{
				Class:               "TestClass",
				InvertedIndexConfig: &models.InvertedIndexConfig{},
			}
			if tt.getClass {
				mockSchemaGetter.On("ReadOnlyClass", "TestClass").Return(class)
			}

			// Create original index state
			originalSS := &sharding.State{
				Physical: map[string]sharding.Physical{
					"shard1": {
						Name:           "shard1",
						BelongsToNodes: []string{"node1"},
						Status:         tt.originalStatus,
					},
				},
				PartitioningEnabled: true,
			}

			index, err := NewIndex(context.Background(), IndexConfig{
				ClassName:         schema.ClassName("TestClass"),
				RootPath:          t.TempDir(),
				ReplicationFactor: NewAtomicInt64(1),
				ShardLoadLimiter:  NewShardLoadLimiter(monitoring.NoopRegisterer, 1),
			}, originalSS, inverted.ConfigFromModel(class.InvertedIndexConfig),
				hnsw.NewDefaultUserConfig(), nil, mockSchemaGetter, nil, logrus.New(), nil, nil, nil, nil, class, nil, nil, nil)
			require.NoError(t, err)

			shard, err := NewShard(context.Background(), nil, "shard1", index, class, nil, nil)
			require.NoError(t, err)

			index.shards.Store("shard1", shard)

			migrator := &Migrator{
				db: &DB{
					schemaGetter: mockSchemaGetter,
				},
				nodeId: "node1",
			}

			// Create incoming state
			incomingSS := &sharding.State{
				Physical: map[string]sharding.Physical{
					"shard1": {
						Name:           "shard1",
						BelongsToNodes: []string{"node1"},
						Status:         tt.incomingStatus,
					},
				},
				PartitioningEnabled: true,
			}

			err = migrator.updateIndexTenants(context.Background(), index, incomingSS)
			require.NoError(t, err)

			mockSchemaGetter.AssertExpectations(t)

			// Verify the shard status
			require.Equal(t, tt.expectedStatus, shard.GetStatus())
		})
	}
}
