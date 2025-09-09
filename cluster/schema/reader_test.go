package schema

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestSchemaReader_WithShardingStateCheck(t *testing.T) {
	tests := []struct {
		name          string
		setupSchema   func(*schema) string
		readerCalled  bool
		expectedError string
	}{
		{
			name: "valid non-partitioned state",
			setupSchema: func(s *schema) string {
				class := &models.Class{Class: "ValidClass"}
				shardState := &sharding.State{
					PartitioningEnabled: false,
					Physical: map[string]sharding.Physical{
						"shard1": {Name: "shard1", BelongsToNodes: []string{"node1"}},
					},
					Virtual: []sharding.Virtual{
						{Name: "virtual1", AssignedToPhysical: "shard1"},
					},
				}
				err := s.addClass(class, shardState, 1)
				require.NoError(t, err)
				return "ValidClass"
			},
			readerCalled: true,
		},
		{
			name: "valid partitioned state",
			setupSchema: func(s *schema) string {
				class := &models.Class{Class: "PartitionedClass"}
				shardState := &sharding.State{
					PartitioningEnabled: true,
					Physical: map[string]sharding.Physical{
						"tenant1": {Name: "tenant1", BelongsToNodes: []string{"node1"}},
					},
				}
				err := s.addClass(class, shardState, 1)
				require.NoError(t, err)
				return "PartitionedClass"
			},
			readerCalled: true,
		},
		{
			name: "partitioned with nil physical",
			setupSchema: func(s *schema) string {
				class := &models.Class{Class: "PartitionedNilPhysical"}
				shardState := &sharding.State{
					PartitioningEnabled: true,
					Physical:            nil,
				}
				err := s.addClass(class, shardState, 1)
				require.NoError(t, err)
				return "PartitionedNilPhysical"
			},
			readerCalled:  false,
			expectedError: "invalid sharding state: physical map is nil (partitioned)",
		},
		{
			name: "non-partitioned with nil physical",
			setupSchema: func(s *schema) string {
				class := &models.Class{Class: "NilPhysical"}
				shardState := &sharding.State{
					PartitioningEnabled: false,
					Physical:            nil,
					Virtual:             []sharding.Virtual{},
				}
				err := s.addClass(class, shardState, 1)
				require.NoError(t, err)
				return "NilPhysical"
			},
			readerCalled:  false,
			expectedError: "invalid sharding state: physical shards unavailable",
		},
		{
			name: "non-partitioned with empty physical",
			setupSchema: func(s *schema) string {
				class := &models.Class{Class: "EmptyPhysical"}
				shardState := &sharding.State{
					PartitioningEnabled: false,
					Physical:            map[string]sharding.Physical{},
					Virtual:             []sharding.Virtual{},
				}
				err := s.addClass(class, shardState, 1)
				require.NoError(t, err)
				return "EmptyPhysical"
			},
			readerCalled:  false,
			expectedError: "invalid sharding state: physical shards unavailable",
		},
		{
			name: "non-partitioned with nil virtual",
			setupSchema: func(s *schema) string {
				class := &models.Class{Class: "NilVirtual"}
				shardState := &sharding.State{
					PartitioningEnabled: false,
					Physical: map[string]sharding.Physical{
						"shard1": {Name: "shard1", BelongsToNodes: []string{"node1"}},
					},
					Virtual: nil,
				}
				err := s.addClass(class, shardState, 1)
				require.NoError(t, err)
				return "NilVirtual"
			},
			readerCalled:  false,
			expectedError: "invalid sharding state: virtual shards unavailable",
		},
		{
			name: "non-partitioned with empty virtual",
			setupSchema: func(s *schema) string {
				class := &models.Class{Class: "EmptyVirtual"}
				shardState := &sharding.State{
					PartitioningEnabled: false,
					Physical: map[string]sharding.Physical{
						"shard1": {Name: "shard1", BelongsToNodes: []string{"node1"}},
					},
					Virtual: []sharding.Virtual{},
				}
				err := s.addClass(class, shardState, 1)
				require.NoError(t, err)
				return "EmptyVirtual"
			},
			readerCalled:  false,
			expectedError: "invalid sharding state: virtual shards unavailable",
		},
		{
			name: "class not found",
			setupSchema: func(s *schema) string {
				return "NonExistentClass"
			},
			readerCalled:  false,
			expectedError: "class not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// GIVEN
			s := NewSchema("test-node", nil, prometheus.NewPedanticRegistry())
			className := tt.setupSchema(s)

			reader := SchemaReader{schema: s}

			readerCalled := false
			readerCallback := func(*models.Class, *sharding.State) error {
				readerCalled = true
				return nil
			}

			// WHEN
			err := reader.Read(className, readerCallback)

			// THEN
			if tt.expectedError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tt.readerCalled, readerCalled)
		})
	}
}
