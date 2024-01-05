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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestForceDelete(t *testing.T) {
	type test struct {
		name           string
		existingSchema []*models.Class
		classToDelete  string
		expErr         bool
		expErrMsg      string
		expSchema      []*models.Class
	}

	tests := []test{
		{
			name: "class exists",
			existingSchema: []*models.Class{
				{Class: "MyClass", VectorIndexType: "hnsw"},
				{Class: "OtherClass", VectorIndexType: "hnsw"},
			},
			classToDelete: "MyClass",
			expSchema: []*models.Class{
				classWithDefaultsSet(t, "OtherClass"),
			},
			expErr: false,
		},
		{
			name: "class does not exist",
			existingSchema: []*models.Class{
				{Class: "OtherClass", VectorIndexType: "hnsw"},
			},
			classToDelete: "MyClass",
			expSchema: []*models.Class{
				classWithDefaultsSet(t, "OtherClass"),
			},
			expErr: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clusterState := &fakeClusterState{
				hosts: []string{"node1"},
			}

			txClient := &fakeTxClient{}

			initialSchema := &State{
				ObjectSchema: &models.Schema{
					Classes: test.existingSchema,
				},
			}

			sm, err := newManagerWithClusterAndTx(t, clusterState, txClient, initialSchema)
			require.Nil(t, err)
			err = sm.DeleteClass(context.Background(), nil, test.classToDelete)

			if test.expErr {
				require.NotNil(t, err)
				assert.Contains(t, err.Error(), test.expErrMsg)
			} else {
				require.Nil(t, err)
			}

			assert.ElementsMatch(t, test.expSchema, sm.GetSchemaSkipAuth().Objects.Classes)

			if len(sm.schemaCache.ShardingState) != len(test.expSchema) {
				t.Errorf("sharding state entries != schema: %d vs %d",
					len(sm.schemaCache.ShardingState), len(test.expSchema))
			}
		})
	}
}

func classWithDefaultsSet(t *testing.T, name string) *models.Class {
	class := &models.Class{Class: name, VectorIndexType: "hnsw"}

	sc, err := sharding.ParseConfig(map[string]interface{}{}, 1)
	require.Nil(t, err)

	class.ShardingConfig = sc

	class.VectorIndexConfig = fakeVectorConfig{}
	class.ReplicationConfig = &models.ReplicationConfig{Factor: 1}

	return class
}
