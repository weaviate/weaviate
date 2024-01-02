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
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// This test makes sure that even when a commit fails, the coordinator still
// honors the commit. This was introduced as part of
// https://github.com/weaviate/weaviate/issues/2616, where a schema
// inconsistency was guaranteed as soon as any node died in it's commit phase
// because the coordinator would behave differently than other (alive) which it
// told to commit.
func TestFailedCommits(t *testing.T) {
	type test struct {
		name string
		// prepare runs before any commit errors occur to build an initial state
		prepare func(*testing.T, *Manager)
		// action runs with commit errors
		action    func(*testing.T, *Manager)
		expSchema []*models.Class
	}

	ctx := context.Background()
	vTrue := true
	vFalse := false

	tests := []test{
		{
			name: "Add a class",
			action: func(t *testing.T, sm *Manager) {
				sm.AddClass(ctx, nil, &models.Class{
					Class:           "MyClass",
					VectorIndexType: "hnsw",
				})
			},
			expSchema: []*models.Class{
				classWithDefaultsWithProps(t, "MyClass", nil),
			},
		},
		{
			name: "Delete a class",
			prepare: func(t *testing.T, sm *Manager) {
				sm.AddClass(ctx, nil, &models.Class{
					Class:           "MyClass",
					VectorIndexType: "hnsw",
				})
				sm.AddClass(ctx, nil, &models.Class{
					Class:           "OtherClass",
					VectorIndexType: "hnsw",
				})
			},
			action: func(t *testing.T, sm *Manager) {
				assert.Nil(t, sm.DeleteClass(ctx, nil, "MyClass"))
			},
			expSchema: []*models.Class{
				classWithDefaultsWithProps(t, "OtherClass", nil),
			},
		},
		{
			name: "Extend a class with a property",
			prepare: func(t *testing.T, sm *Manager) {
				sm.AddClass(ctx, nil, &models.Class{
					Class:           "MyClass",
					VectorIndexType: "hnsw",
				})
			},
			action: func(t *testing.T, sm *Manager) {
				err := sm.AddClassProperty(ctx, nil, "MyClass", &models.Property{
					Name:     "prop_1",
					DataType: schema.DataTypeInt.PropString(),
				})
				assert.Nil(t, err)
			},
			expSchema: []*models.Class{
				classWithDefaultsWithProps(t, "MyClass", []*models.Property{
					{
						Name:            "prop_1",
						DataType:        schema.DataTypeInt.PropString(),
						IndexFilterable: &vTrue,
						IndexSearchable: &vFalse,
					},
				}),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clusterState := &fakeClusterState{
				hosts: []string{"node1", "node2"},
			}

			// required for the startup sync
			txJSON, _ := json.Marshal(ReadSchemaPayload{
				Schema: &State{
					ObjectSchema: &models.Schema{
						Classes: []*models.Class{},
					},
				},
			})

			txClient := &fakeTxClient{
				openInjectPayload: json.RawMessage(txJSON), // required for the startup sync
			}

			initialSchema := &State{
				ObjectSchema:  &models.Schema{},
				ShardingState: map[string]*sharding.State{},
			}

			sm, err := newManagerWithClusterAndTx(t, clusterState, txClient, initialSchema)
			require.Nil(t, err)

			sm.StartServing(context.Background())

			if test.prepare != nil {
				test.prepare(t, sm)
			}

			txClient.commitErr = fmt.Errorf("Oh I, I just died in your arms tonight")
			test.action(t, sm)

			assert.ElementsMatch(t, test.expSchema, sm.GetSchemaSkipAuth().Objects.Classes)
		})
	}
}

func classWithDefaultsWithProps(t *testing.T, name string,
	props []*models.Property,
) *models.Class {
	class := &models.Class{Class: name, VectorIndexType: "hnsw"}
	class.Vectorizer = "none"

	sc, err := sharding.ParseConfig(map[string]interface{}{}, 1)
	require.Nil(t, err)

	class.ShardingConfig = sc

	class.VectorIndexConfig = fakeVectorConfig{}
	class.ReplicationConfig = &models.ReplicationConfig{Factor: 1}
	class.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: false}

	(&fakeModuleConfig{}).SetClassDefaults(class)
	setInvertedConfigDefaults(class)

	class.Properties = props

	return class
}
