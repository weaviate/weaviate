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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/test_utils"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestShardReplicas(t *testing.T) {
	cache := schemaCache{
		State: State{
			ShardingState: map[string]*sharding.State{},
		},
	}

	// class not found
	_, err := cache.ShardReplicas("C", "S")
	assert.ErrorContains(t, err, "class not found")

	// shard not found
	ss := &sharding.State{Physical: make(map[string]sharding.Physical)}
	cache.State.ShardingState["C"] = ss
	_, err = cache.ShardReplicas("C", "S")
	assert.ErrorContains(t, err, "shard not found")

	// two replicas found
	nodes := []string{"A", "B"}
	ss.Physical["S"] = sharding.Physical{BelongsToNodes: nodes}
	res, err := cache.ShardReplicas("C", "S")
	assert.Nil(t, err)
	assert.Equal(t, nodes, res)
}

func TestUpdateClass(t *testing.T) {
	class := "C"
	cache := schemaCache{
		State: State{
			ShardingState: map[string]*sharding.State{},
			ObjectSchema: &models.Schema{
				Classes: []*models.Class{},
			},
		},
	}

	if err := cache.updateClass(&models.Class{}, nil); !errors.Is(err, errClassNotFound) {
		t.Fatalf("update_class: want %v got %v", errClassNotFound, err)
	}
	if _, err := cache.addProperty("?", nil); !errors.Is(err, errClassNotFound) {
		t.Fatalf("add_property: want %v got %v", errClassNotFound, err)
	}
	c := models.Class{
		Class:      class,
		Properties: []*models.Property{},
	}
	uc := models.Class{
		Class: class,
		Properties: []*models.Property{
			{Description: "P1"},
		},
	}
	// add class
	ss := sharding.State{}
	cache.addClass(&c, &ss)

	if c, _ := cache.readOnlyClass(class); c == nil {
		t.Fatalf("class not found")
	} else if c == cache.unsafeFindClass(class) {
		t.Fatalf("read_only_class doesn't return a shallow copy")
	}

	// update class
	cache.updateClass(&uc, &sharding.State{IndexID: class})
	p := models.Property{Description: "P2"}

	x, err := cache.addProperty(class, &p)
	if err != nil {
		t.Fatalf("class not found")
	}
	if n := len(x.Properties); n != 2 {
		t.Fatalf("number of properties want: %v got: 2", n)
	}
}

func TestCache_MergeObjectProperty(t *testing.T) {
	vFalse := false
	vTrue := true

	className := "objectClass"
	propertyName := "objectProperty"

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

	cache := schemaCache{
		State: State{
			ShardingState: map[string]*sharding.State{},
			ObjectSchema: &models.Schema{
				Classes: []*models.Class{
					{
						Class:      className,
						Properties: []*models.Property{objectProperty},
					},
				},
			},
		},
	}

	updatedClass, err := cache.mergeObjectProperty(className, updatedObjectProperty)
	require.NoError(t, err)
	require.NotNil(t, updatedClass)
	require.Len(t, updatedClass.Properties, 1)

	mergedProperty := updatedClass.Properties[0]
	require.NotNil(t, mergedProperty)
	assert.Equal(t, expectedObjectProperty.DataType, mergedProperty.DataType)
	assert.Equal(t, expectedObjectProperty.IndexFilterable, mergedProperty.IndexFilterable)
	assert.Equal(t, expectedObjectProperty.IndexSearchable, mergedProperty.IndexSearchable)
	assert.Equal(t, expectedObjectProperty.Tokenization, mergedProperty.Tokenization)

	test_utils.AssertNestedPropsMatch(t, expectedObjectProperty.NestedProperties, mergedProperty.NestedProperties)
}
