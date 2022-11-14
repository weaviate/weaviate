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
	"context"
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/cluster"
	"github.com/semi-technologies/weaviate/usecases/sharding"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIncommingTxCommit(t *testing.T) {
	type test struct {
		name                string
		tx                  *cluster.Transaction
		assertSchema        func(t *testing.T, sm *Manager)
		expectedErrContains string
	}

	tests := []test{
		{
			name: "successful add class",
			tx: &cluster.Transaction{
				Type: AddClass,
				Payload: AddClassPayload{
					Class: &models.Class{
						Class:           "SecondClass",
						VectorIndexType: "hnsw",
					},
					State: &sharding.State{},
				},
			},
			assertSchema: func(t *testing.T, sm *Manager) {
				class, err := sm.GetClass(context.Background(), nil, "SecondClass")
				require.Nil(t, err)
				assert.Equal(t, "SecondClass", class.Class)
			},
		},
		{
			name: "add class with incorrect payload",
			tx: &cluster.Transaction{
				Type:    AddClass,
				Payload: "wrong-payload",
			},
			expectedErrContains: "expected commit payload to be",
		},
		{
			name: "add class with vector parse error",
			tx: &cluster.Transaction{
				Type: AddClass,
				Payload: AddClassPayload{
					Class: &models.Class{
						Class:           "SecondClass",
						VectorIndexType: "some-weird-pq-based-index",
					},
					State: &sharding.State{},
				},
			},
			expectedErrContains: "unsupported vector index type",
		},
		{
			name: "add class with sharding parse error",
			tx: &cluster.Transaction{
				Type: AddClass,
				Payload: AddClassPayload{
					Class: &models.Class{
						Class:           "SecondClass",
						VectorIndexType: "hnsw",
						ShardingConfig:  "this-cant-be-a-string",
					},
					State: &sharding.State{},
				},
			},
			expectedErrContains: "parse sharding config",
		},
		{
			name: "successful add property",
			tx: &cluster.Transaction{
				Type: AddProperty,
				Payload: AddPropertyPayload{
					ClassName: "FirstClass",
					Property: &models.Property{
						DataType: []string{"string"},
						Name:     "new_prop",
					},
				},
			},
			assertSchema: func(t *testing.T, sm *Manager) {
				class, err := sm.GetClass(context.Background(), nil, "FirstClass")
				require.Nil(t, err)
				assert.Equal(t, "new_prop", class.Properties[0].Name)
			},
		},
		{
			name: "add property with incorrect payload",
			tx: &cluster.Transaction{
				Type:    AddProperty,
				Payload: "wrong-payload",
			},
			expectedErrContains: "expected commit payload to be",
		},
		{
			name: "successful delete class",
			tx: &cluster.Transaction{
				Type: DeleteClass,
				Payload: DeleteClassPayload{
					ClassName: "FirstClass",
				},
			},
			assertSchema: func(t *testing.T, sm *Manager) {
				class, err := sm.GetClass(context.Background(), nil, "FirstClass")
				require.Nil(t, err)
				assert.Nil(t, class)
			},
		},
		{
			name: "delete class with incorrect payload",
			tx: &cluster.Transaction{
				Type:    DeleteClass,
				Payload: "wrong-payload",
			},
			expectedErrContains: "expected commit payload to be",
		},
		{
			name: "successful update class",
			tx: &cluster.Transaction{
				Type: UpdateClass,
				Payload: UpdateClassPayload{
					ClassName: "FirstClass",
					Class: &models.Class{
						Class:           "FirstClass",
						VectorIndexType: "hnsw",
						Properties: []*models.Property{
							{
								Name:     "added_through_update",
								DataType: []string{"int"},
							},
						},
					},
					State: &sharding.State{},
				},
			},
			assertSchema: func(t *testing.T, sm *Manager) {
				class, err := sm.GetClass(context.Background(), nil, "FirstClass")
				require.Nil(t, err)
				assert.Equal(t, "added_through_update", class.Properties[0].Name)
			},
		},
		{
			name: "update class with incorrect payload",
			tx: &cluster.Transaction{
				Type:    UpdateClass,
				Payload: "wrong-payload",
			},
			expectedErrContains: "expected commit payload to be",
		},
		{
			name: "update class with invalid vector index",
			tx: &cluster.Transaction{
				Type: UpdateClass,
				Payload: UpdateClassPayload{
					ClassName: "FirstClass",
					Class: &models.Class{
						Class:           "FirstClass",
						VectorIndexType: "nope",
					},
					State: &sharding.State{},
				},
			},
			expectedErrContains: "parse vector index",
		},
		{
			name: "update class with invalid sharding config",
			tx: &cluster.Transaction{
				Type: UpdateClass,
				Payload: UpdateClassPayload{
					ClassName: "FirstClass",
					Class: &models.Class{
						Class:           "FirstClass",
						VectorIndexType: "hnsw",
						ShardingConfig:  "this-cant-be-a-string",
					},
					State: &sharding.State{},
				},
			},
			expectedErrContains: "parse sharding config",
		},
		{
			name: "invalid commit type",
			tx: &cluster.Transaction{
				Type: "i-dont-exist",
			},
			expectedErrContains: "unrecognized commit type",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			schemaBefore := &State{
				ObjectSchema: &models.Schema{
					Classes: []*models.Class{
						{
							Class:           "FirstClass",
							VectorIndexType: "hnsw",
						},
					},
				},
			}
			sm, err := newManagerWithClusterAndTx(t,
				&fakeClusterState{hosts: []string{"node1"}}, &fakeTxClient{},
				schemaBefore)
			require.Nil(t, err)

			err = sm.handleCommit(context.Background(), test.tx)
			if test.expectedErrContains == "" {
				require.Nil(t, err)
				test.assertSchema(t, sm)
			} else {
				require.NotNil(t, err)
				assert.Contains(t, err.Error(), test.expectedErrContains)
			}
		})
	}
}

func TestTxResponse(t *testing.T) {
	type test struct {
		name     string
		tx       *cluster.Transaction
		assertTx func(t *testing.T, tx *cluster.Transaction)
	}

	tests := []test{
		{
			name: "ignore write transactions",
			tx: &cluster.Transaction{
				Type: AddClass,
				Payload: AddClassPayload{
					Class: &models.Class{
						Class:           "SecondClass",
						VectorIndexType: "hnsw",
					},
					State: &sharding.State{},
				},
			},
			assertTx: func(t *testing.T, tx *cluster.Transaction) {
				_, ok := tx.Payload.(AddClassPayload)
				assert.True(t, ok, "write tx was not changed")
			},
		},
		{
			name: "respond with schema on ReadSchema",
			tx: &cluster.Transaction{
				Type:    ReadSchema,
				Payload: nil,
			},
			assertTx: func(t *testing.T, tx *cluster.Transaction) {
				pl, ok := tx.Payload.(ReadSchemaPayload)
				require.True(t, ok)
				require.Len(t, pl.Schema.ObjectSchema.Classes, 1)
				assert.Equal(t, "FirstClass", pl.Schema.ObjectSchema.Classes[0].Class)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			schemaBefore := &State{
				ObjectSchema: &models.Schema{
					Classes: []*models.Class{
						{
							Class:           "FirstClass",
							VectorIndexType: "hnsw",
						},
					},
				},
			}
			sm, err := newManagerWithClusterAndTx(t,
				&fakeClusterState{hosts: []string{"node1"}}, &fakeTxClient{},
				schemaBefore)
			require.Nil(t, err)

			err = sm.handleTxResponse(context.Background(), test.tx)
			require.Nil(t, err)
			test.assertTx(t, test.tx)
		})
	}
}
