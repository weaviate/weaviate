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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/test_utils"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestIncommingTxCommit(t *testing.T) {
	type test struct {
		name                string
		before              func(t *testing.T, SM *Manager)
		tx                  *cluster.Transaction
		assertSchema        func(t *testing.T, sm *Manager)
		expectedErrContains string
	}

	vFalse := false
	vTrue := true
	propertyName := "object_prop"
	objectProperty := &models.Property{
		Name:            propertyName,
		DataType:        schema.DataTypeObject.PropString(),
		IndexFilterable: &vTrue,
		IndexSearchable: &vFalse,
		Tokenization:    "",
		NestedProperties: []*models.NestedProperty{
			{
				Name:            "nested_int",
				DataType:        schema.DataTypeInt.PropString(),
				IndexFilterable: &vTrue,
				IndexSearchable: &vFalse,
				Tokenization:    "",
			},
			{
				Name:            "nested_text",
				DataType:        schema.DataTypeText.PropString(),
				IndexFilterable: &vTrue,
				IndexSearchable: &vTrue,
				Tokenization:    models.PropertyTokenizationWord,
			},
			{
				Name:            "nested_objects",
				DataType:        schema.DataTypeObjectArray.PropString(),
				IndexFilterable: &vTrue,
				IndexSearchable: &vFalse,
				Tokenization:    "",
				NestedProperties: []*models.NestedProperty{
					{
						Name:            "nested_bool_lvl2",
						DataType:        schema.DataTypeBoolean.PropString(),
						IndexFilterable: &vTrue,
						IndexSearchable: &vFalse,
						Tokenization:    "",
					},
					{
						Name:            "nested_numbers_lvl2",
						DataType:        schema.DataTypeNumberArray.PropString(),
						IndexFilterable: &vTrue,
						IndexSearchable: &vFalse,
						Tokenization:    "",
					},
				},
			},
		},
	}
	updatedObjectProperty := &models.Property{
		Name:            propertyName,
		DataType:        schema.DataTypeObject.PropString(),
		IndexFilterable: &vFalse, // different setting than existing class/prop
		IndexSearchable: &vFalse,
		Tokenization:    "",
		NestedProperties: []*models.NestedProperty{
			{
				Name:            "nested_number",
				DataType:        schema.DataTypeNumber.PropString(),
				IndexFilterable: &vTrue,
				IndexSearchable: &vFalse,
				Tokenization:    "",
			},
			{
				Name:            "nested_text",
				DataType:        schema.DataTypeText.PropString(),
				IndexFilterable: &vTrue,
				IndexSearchable: &vTrue,
				Tokenization:    models.PropertyTokenizationField, // different setting than existing class/prop
			},
			{
				Name:            "nested_objects",
				DataType:        schema.DataTypeObjectArray.PropString(),
				IndexFilterable: &vTrue,
				IndexSearchable: &vFalse,
				Tokenization:    "",
				NestedProperties: []*models.NestedProperty{
					{
						Name:            "nested_date_lvl2",
						DataType:        schema.DataTypeDate.PropString(),
						IndexFilterable: &vTrue,
						IndexSearchable: &vFalse,
						Tokenization:    "",
					},
					{
						Name:            "nested_numbers_lvl2",
						DataType:        schema.DataTypeNumberArray.PropString(),
						IndexFilterable: &vFalse, // different setting than existing class/prop
						IndexSearchable: &vFalse,
						Tokenization:    "",
					},
				},
			},
		},
	}
	expectedObjectProperty := &models.Property{
		Name:            propertyName,
		DataType:        schema.DataTypeObject.PropString(),
		IndexFilterable: &vTrue,
		IndexSearchable: &vFalse,
		Tokenization:    "",
		NestedProperties: []*models.NestedProperty{
			{
				Name:            "nested_int",
				DataType:        schema.DataTypeInt.PropString(),
				IndexFilterable: &vTrue,
				IndexSearchable: &vFalse,
				Tokenization:    "",
			},
			{
				Name:            "nested_number",
				DataType:        schema.DataTypeNumber.PropString(),
				IndexFilterable: &vTrue,
				IndexSearchable: &vFalse,
				Tokenization:    "",
			},
			{
				Name:            "nested_text",
				DataType:        schema.DataTypeText.PropString(),
				IndexFilterable: &vTrue,
				IndexSearchable: &vTrue,
				Tokenization:    models.PropertyTokenizationWord, // from existing class/prop
			},
			{
				Name:            "nested_objects",
				DataType:        schema.DataTypeObjectArray.PropString(),
				IndexFilterable: &vTrue,
				IndexSearchable: &vFalse,
				Tokenization:    "",
				NestedProperties: []*models.NestedProperty{
					{
						Name:            "nested_bool_lvl2",
						DataType:        schema.DataTypeBoolean.PropString(),
						IndexFilterable: &vTrue,
						IndexSearchable: &vFalse,
						Tokenization:    "",
					},
					{
						Name:            "nested_date_lvl2",
						DataType:        schema.DataTypeDate.PropString(),
						IndexFilterable: &vTrue,
						IndexSearchable: &vFalse,
						Tokenization:    "",
					},
					{
						Name:            "nested_numbers_lvl2",
						DataType:        schema.DataTypeNumberArray.PropString(),
						IndexFilterable: &vTrue, // from existing class/prop
						IndexSearchable: &vFalse,
						Tokenization:    "",
					},
				},
			},
		},
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
						DataType:     schema.DataTypeText.PropString(),
						Tokenization: models.PropertyTokenizationWhitespace,
						Name:         "new_prop",
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

		{
			name: "successfully add tenants",
			tx: &cluster.Transaction{
				Type: addTenants,
				Payload: AddTenantsPayload{
					Class:   "FirstClass",
					Tenants: []TenantCreate{{Name: "P1"}, {Name: "P2"}},
				},
			},
			assertSchema: func(t *testing.T, sm *Manager) {
				st := sm.CopyShardingState("FirstClass")
				require.NotNil(t, st)
				require.Contains(t, st.Physical, "P1")
				require.Contains(t, st.Physical, "P2")
			},
		},
		{
			name: "add partition to an unknown class",
			tx: &cluster.Transaction{
				Type: addTenants,
				Payload: AddTenantsPayload{
					Class:   "UnknownClass",
					Tenants: []TenantCreate{{Name: "P1"}, {Name: "P2"}},
				},
			},
			expectedErrContains: "UnknownClass",
		},
		{
			name: "add tenants with incorrect payload",
			tx: &cluster.Transaction{
				Type:    addTenants,
				Payload: AddPropertyPayload{},
			},
			expectedErrContains: "expected commit payload to be",
		},

		{
			name: "successfully update tenants",
			before: func(t *testing.T, sm *Manager) {
				err := sm.handleCommit(context.Background(), &cluster.Transaction{
					Type: addTenants,
					Payload: AddTenantsPayload{
						Class: "FirstClass",
						Tenants: []TenantCreate{
							{Name: "P1"},
							{Name: "P2", Status: models.TenantActivityStatusHOT},
						},
					},
				})
				require.Nil(t, err)
			},
			tx: &cluster.Transaction{
				Type: updateTenants,
				Payload: UpdateTenantsPayload{
					Class: "FirstClass",
					Tenants: []TenantUpdate{
						{Name: "P1", Status: models.TenantActivityStatusCOLD},
						{Name: "P2", Status: models.TenantActivityStatusCOLD},
					},
				},
			},
			assertSchema: func(t *testing.T, sm *Manager) {
				st := sm.CopyShardingState("FirstClass")
				require.NotNil(t, st)
				require.Contains(t, st.Physical, "P1")
				require.Contains(t, st.Physical, "P2")
				assert.Equal(t, st.Physical["P1"].Status, models.TenantActivityStatusCOLD)
				assert.Equal(t, st.Physical["P2"].Status, models.TenantActivityStatusCOLD)
			},
		},
		{
			name: "update tenants of unknown class",
			tx: &cluster.Transaction{
				Type: updateTenants,
				Payload: UpdateTenantsPayload{
					Class: "UnknownClass",
					Tenants: []TenantUpdate{
						{Name: "P1", Status: models.TenantActivityStatusCOLD},
						{Name: "P2", Status: models.TenantActivityStatusCOLD},
					},
				},
			},
			expectedErrContains: "UnknownClass",
		},
		{
			name: "update tenants with incorrect payload",
			tx: &cluster.Transaction{
				Type:    updateTenants,
				Payload: AddPropertyPayload{},
			},
			expectedErrContains: "expected commit payload to be",
		},

		{
			name: "merge object property of unknown class",
			tx: &cluster.Transaction{
				Type: mergeObjectProperty,
				Payload: MergeObjectPropertyPayload{
					ClassName: "UnknownClass",
					Property:  updatedObjectProperty,
				},
			},
			expectedErrContains: "class not found",
		},
		{
			name: "merge object property of unknown property",
			tx: &cluster.Transaction{
				Type: mergeObjectProperty,
				Payload: MergeObjectPropertyPayload{
					ClassName: "FirstClass",
					Property:  updatedObjectProperty,
				},
			},
			expectedErrContains: "property not found",
		},
		{
			name: "merge object property",
			before: func(t *testing.T, sm *Manager) {
				err := sm.handleCommit(context.Background(), &cluster.Transaction{
					Type: AddProperty,
					Payload: AddPropertyPayload{
						ClassName: "FirstClass",
						Property:  objectProperty,
					},
				})
				require.Nil(t, err)
			},
			tx: &cluster.Transaction{
				Type: mergeObjectProperty,
				Payload: MergeObjectPropertyPayload{
					ClassName: "FirstClass",
					Property:  updatedObjectProperty,
				},
			},
			assertSchema: func(t *testing.T, sm *Manager) {
				updatedClass := sm.getClassByName("FirstClass")

				require.NotNil(t, updatedClass)
				require.Len(t, updatedClass.Properties, 1)

				mergedProperty := updatedClass.Properties[0]
				require.NotNil(t, mergedProperty)
				assert.Equal(t, expectedObjectProperty.DataType, mergedProperty.DataType)
				assert.Equal(t, expectedObjectProperty.IndexFilterable, mergedProperty.IndexFilterable)
				assert.Equal(t, expectedObjectProperty.IndexSearchable, mergedProperty.IndexSearchable)
				assert.Equal(t, expectedObjectProperty.Tokenization, mergedProperty.Tokenization)

				test_utils.AssertNestedPropsMatch(t, expectedObjectProperty.NestedProperties, mergedProperty.NestedProperties)
			},
		},
		{
			name: "merge object property with invalid payload",
			tx: &cluster.Transaction{
				Type: mergeObjectProperty,
				Payload: AddPropertyPayload{
					ClassName: "FirstClass",
					Property:  updatedObjectProperty,
				},
			},
			expectedErrContains: "expected commit payload to be",
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

			if test.before != nil {
				test.before(t, sm)
			}

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
		assertTx func(t *testing.T, tx *cluster.Transaction, payload json.RawMessage)
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
			assertTx: func(t *testing.T, tx *cluster.Transaction, payload json.RawMessage) {
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
			assertTx: func(t *testing.T, tx *cluster.Transaction, payload json.RawMessage) {
				pl, err := unmarshalRawJson[ReadSchemaPayload](payload)
				require.Nil(t, err)
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

			data, err := sm.handleTxResponse(context.Background(), test.tx)
			require.Nil(t, err)
			if test.tx.Type == ReadSchema {
				var txRes txResponsePayload
				err = json.Unmarshal(data, &txRes)
				require.Nil(t, err)
				test.assertTx(t, test.tx, txRes.Payload)

			}
		})
	}
}

type txResponsePayload struct {
	Type    cluster.TransactionType `json:"type"`
	ID      string                  `json:"id"`
	Payload json.RawMessage         `json:"payload"`
}
