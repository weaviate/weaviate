//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package schema

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestSchemaReader_WithShardingStateCheck(t *testing.T) {
	// what a full retry round of SchemaReader.Read costs: 3 retries, 50ms apart
	const retryWindow = 150 * time.Millisecond

	tests := []struct {
		name                 string
		setupSchema          func(*schema) string
		retryIfClassNotFound bool
		readerCalled         bool
		expectedError        string
		retried              bool
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
			retryIfClassNotFound: true,
			readerCalled:         true,
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
			retryIfClassNotFound: true,
			readerCalled:         true,
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
			retryIfClassNotFound: true,
			readerCalled:         true,
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
			retryIfClassNotFound: true,
			readerCalled:         false,
			expectedError:        "invalid sharding state: physical shards unavailable",
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
			retryIfClassNotFound: true,
			readerCalled:         false,
			expectedError:        "invalid sharding state: physical shards unavailable",
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
			retryIfClassNotFound: true,
			readerCalled:         false,
			expectedError:        "invalid sharding state: virtual shards unavailable",
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
			retryIfClassNotFound: true,
			readerCalled:         false,
			expectedError:        "invalid sharding state: virtual shards unavailable",
		},
		{
			name: "class not found with retry",
			setupSchema: func(s *schema) string {
				return "NonExistentClass"
			},
			retryIfClassNotFound: true,
			readerCalled:         false,
			expectedError:        "class not found",
			retried:              true,
		},
		{
			name: "class not found without retry",
			setupSchema: func(s *schema) string {
				return "NonExistentClass"
			},
			retryIfClassNotFound: false,
			readerCalled:         false,
			expectedError:        "class not found",
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
			start := time.Now()
			err := reader.Read(className, tt.retryIfClassNotFound, readerCallback)
			elapsed := time.Since(start)

			// THEN
			if tt.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedError)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tt.readerCalled, readerCalled)

			if tt.retried {
				require.GreaterOrEqual(t, elapsed, retryWindow)
			} else {
				require.Less(t, elapsed, retryWindow)
			}
		})
	}
}
