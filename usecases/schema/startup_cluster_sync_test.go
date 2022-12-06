//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package schema

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/cluster"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/sharding"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStartupSync(t *testing.T) {
	t.Run("new node joining, other nodes have schema", func(t *testing.T) {
		clusterState := &fakeClusterState{
			hosts: []string{"node1", "node2"},
		}

		txJSON, _ := json.Marshal(ReadSchemaPayload{
			Schema: &State{
				ObjectSchema: &models.Schema{
					Classes: []*models.Class{
						{
							Class:           "Bongourno",
							VectorIndexType: "hnsw",
						},
					},
				},
			},
		})

		txClient := &fakeTxClient{
			openInjectPayload: json.RawMessage(txJSON),
		}

		sm, err := newManagerWithClusterAndTx(t, clusterState, txClient, nil)
		require.Nil(t, err)

		localSchema := sm.GetSchemaSkipAuth()
		assert.Equal(t, "Bongourno", localSchema.FindClassByName("Bongourno").Class)
	})

	t.Run("new node joining, other nodes have no schema", func(t *testing.T) {
		clusterState := &fakeClusterState{
			hosts: []string{"node1", "node2"},
		}

		txJSON, _ := json.Marshal(ReadSchemaPayload{
			Schema: &State{
				ObjectSchema: &models.Schema{
					Classes: []*models.Class{},
				},
			},
		})

		txClient := &fakeTxClient{
			openInjectPayload: json.RawMessage(txJSON),
		}

		sm, err := newManagerWithClusterAndTx(t, clusterState, txClient, nil)
		require.Nil(t, err)

		localSchema := sm.GetSchemaSkipAuth()
		assert.Len(t, localSchema.Objects.Classes, 0)
	})

	t.Run("new node joining, conflict in schema between nodes", func(t *testing.T) {
		clusterState := &fakeClusterState{
			hosts: []string{"node1", "node2"},
		}

		txJSON, _ := json.Marshal(ReadSchemaPayload{
			Schema: &State{
				ObjectSchema: &models.Schema{
					Classes: []*models.Class{
						{
							Class:           "Bongourno",
							VectorIndexType: "hnsw",
						},
					},
				},
			},
		})

		txClient := &fakeTxClient{
			openInjectPayload: json.RawMessage(txJSON),
		}

		_, err := newManagerWithClusterAndTx(t, clusterState, txClient, &State{
			ObjectSchema: &models.Schema{
				Classes: []*models.Class{
					{
						Class:           "Hola",
						VectorIndexType: "hnsw",
					},
				},
			},
		})
		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "corrupt")
	})

	t.Run("new node joining, agreement between all", func(t *testing.T) {
		clusterState := &fakeClusterState{
			hosts: []string{"node1", "node2"},
		}

		txJSON, _ := json.Marshal(ReadSchemaPayload{
			Schema: &State{
				ObjectSchema: &models.Schema{
					Classes: []*models.Class{
						{
							Class:           "GutenTag",
							VectorIndexType: "hnsw",
						},
					},
				},
			},
		})

		txClient := &fakeTxClient{
			openInjectPayload: json.RawMessage(txJSON),
		}

		sm, err := newManagerWithClusterAndTx(t, clusterState, txClient, &State{
			ObjectSchema: &models.Schema{
				Classes: []*models.Class{
					{
						Class:           "GutenTag",
						VectorIndexType: "hnsw",
					},
				},
			},
		})
		require.Nil(t, err)

		localSchema := sm.GetSchemaSkipAuth()
		assert.Equal(t, "GutenTag", localSchema.FindClassByName("GutenTag").Class)
	})

	t.Run("new node joining, other nodes include an outdated version", func(t *testing.T) {
		clusterState := &fakeClusterState{
			hosts: []string{"node1", "node2"},
		}

		txClient := &fakeTxClient{
			openErr: fmt.Errorf("unrecognized schema transaction type"),
		}

		sm, err := newManagerWithClusterAndTx(t, clusterState, txClient, nil)
		require.Nil(t, err) // no error, sync was skipped

		schema := sm.GetSchemaSkipAuth()
		assert.Len(t, schema.Objects.Classes, 0, "schema is still empty")
	})

	t.Run("node with data (re-)joining, but other nodes are too old", func(t *testing.T) {
		// we expect that sync would be skipped beacause the other nodes can't take
		// part in the sync
		clusterState := &fakeClusterState{
			hosts: []string{"node1", "node2"},
		}

		txClient := &fakeTxClient{
			openErr: fmt.Errorf("unrecognized schema transaction type"),
		}

		sm, err := newManagerWithClusterAndTx(t, clusterState, txClient, &State{
			ObjectSchema: &models.Schema{
				Classes: []*models.Class{
					{
						Class:           "Hola",
						VectorIndexType: "hnsw",
					},
				},
			},
		})
		require.Nil(t, err) // startup sync was skipped, no error
		schema := sm.GetSchemaSkipAuth()
		require.Len(t, schema.Objects.Classes, 1, "schema is still the local schema")
		assert.Equal(t, "Hola", schema.Objects.Classes[0].Class)
	})

	t.Run("new node joining, schema identical, but other nodes have already been migrated", func(t *testing.T) {
		// Migration refers to the the change that happens when a node first starts
		// up with v1.17. It reads the `belongsToNode` from the sharding config and
		// writes the content into the new `belongsToNodes[]` array type.
		//
		// The timing of the migration vs the sync matters: The remote notes have
		// already completed startup, therefore they have been migrated. If the
		// local schema is not migrated yet, it could fail the checks even though
		// it is logically identical.
		clusterState := &fakeClusterState{
			hosts: []string{"node1", "node2"},
		}

		txJSON, _ := json.Marshal(ReadSchemaPayload{
			Schema: &State{
				ShardingState: map[string]*sharding.State{
					"GutenTag": {
						IndexID: "GutenTag",
						Physical: map[string]sharding.Physical{
							"a-shard-of-beauty": {
								Name:           "a-shard-of-beauty",
								BelongsToNodes: []string{"node-0"}, // Note the usage of the new field (!)
							},
						},
					},
				},
				ObjectSchema: &models.Schema{
					Classes: []*models.Class{
						{
							Class:           "GutenTag",
							VectorIndexType: "hnsw",
						},
					},
				},
			},
		})

		txClient := &fakeTxClient{
			openInjectPayload: json.RawMessage(txJSON),
		}

		sm, err := newManagerWithClusterAndTx(t, clusterState, txClient, &State{
			ShardingState: map[string]*sharding.State{
				"GutenTag": {
					IndexID: "GutenTag",
					Physical: map[string]sharding.Physical{
						"a-shard-of-beauty": {
							Name:                                 "a-shard-of-beauty",
							LegacyBelongsToNodeForBackwardCompat: "node-0", // Note the usage of the old field (!)
						},
					},
				},
			},
			ObjectSchema: &models.Schema{
				Classes: []*models.Class{
					{
						Class:           "GutenTag",
						VectorIndexType: "hnsw",
					},
				},
			},
		})
		require.Nil(t, err)

		localSchema := sm.GetSchemaSkipAuth()
		assert.Equal(t, "GutenTag", localSchema.FindClassByName("GutenTag").Class)
	})
}

func TestStartupSyncUnhappyPaths(t *testing.T) {
	type test struct {
		name          string
		nodes         []string
		errContains   string
		txPayload     interface{}
		txOpenErr     error
		initialSchema *State
	}

	tests := []test{
		{
			name:        "corrupt cluster state: no nodes",
			nodes:       []string{},
			errContains: "cluster has size=0",
		},
		{
			name:        "corrupt cluster state: name mismatch",
			nodes:       []string{"the-wrong-one"},
			errContains: "only node in the cluster does not match local",
		},
		{
			name:        "open tx fails on empty node",
			nodes:       []string{"node1", "node2"},
			txOpenErr:   cluster.ErrConcurrentTransaction,
			errContains: "concurrent transaction",
		},
		{
			name: "open tx fails on populated node",
			initialSchema: &State{ObjectSchema: &models.Schema{
				Classes: []*models.Class{{Class: "Foo", VectorIndexType: "hnsw"}},
			}},
			nodes:       []string{"node1", "node2"},
			txOpenErr:   cluster.ErrConcurrentTransaction,
			errContains: "concurrent transaction",
		},
		{
			name:        "wrong tx payload",
			nodes:       []string{"node1", "node2"},
			txPayload:   "foo",
			errContains: "unmarshal tx",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clusterState := &fakeClusterState{
				hosts: test.nodes,
			}

			if test.txPayload == nil {
				test.txPayload = ReadSchemaPayload{
					Schema: &State{
						ObjectSchema: &models.Schema{
							Classes: []*models.Class{
								{
									Class:           "Bongourno",
									VectorIndexType: "hnsw",
								},
							},
						},
					},
				}
			}

			txJSON, _ := json.Marshal(test.txPayload)

			txClient := &fakeTxClient{
				openInjectPayload: json.RawMessage(txJSON),
				openErr:           test.txOpenErr,
			}

			_, err := newManagerWithClusterAndTx(t, clusterState, txClient, test.initialSchema)
			require.NotNil(t, err)
			assert.Contains(t, err.Error(), test.errContains)
		})
	}
}

func newManagerWithClusterAndTx(t *testing.T, clusterState clusterState,
	txClient cluster.Client, initialSchema *State,
) (*Manager, error) {
	logger, _ := test.NewNullLogger()
	repo := newFakeRepo()
	repo.schema = initialSchema
	sm, err := NewManager(&NilMigrator{}, repo, logger, &fakeAuthorizer{},
		config.Config{DefaultVectorizerModule: config.VectorizerModuleNone},
		dummyParseVectorConfig, // only option for now
		&fakeVectorizerValidator{}, dummyValidateInvertedConfig,
		&fakeModuleConfig{}, clusterState, txClient, &fakeScaleOutManager{},
	)

	return sm, err
}
