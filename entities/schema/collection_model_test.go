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

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	sharding "github.com/weaviate/weaviate/usecases/sharding/config"
)

var (
	vTrue             = true
	vFalse            = false
	emptyModuleConfig map[string]interface{}
)

func TestCollectionFromAndToModel(t *testing.T) {
	tests := []struct {
		name        string
		inputModel  models.Class
		outputModel models.Class
	}{
		{
			name:       "empty",
			inputModel: models.Class{},
			outputModel: models.Class{
				InvertedIndexConfig: &models.InvertedIndexConfig{
					Bm25:      &models.BM25Config{B: 0, K1: 0},
					Stopwords: &models.StopwordConfig{Additions: nil, Preset: "", Removals: nil},
				},
				MultiTenancyConfig: &models.MultiTenancyConfig{},
				ModuleConfig:       emptyModuleConfig,
				Properties:         make([]*models.Property, 0),
				ReplicationConfig:  &models.ReplicationConfig{},
				ShardingConfig:     sharding.Config{},
				VectorIndexType:    "",
			},
		},
		{
			name:       "unknown",
			inputModel: models.Class{VectorIndexType: "unknown"},
		},

		{
			name: "all elements",
			inputModel: models.Class{
				Class:               "class",
				Description:         "description",
				InvertedIndexConfig: &models.InvertedIndexConfig{},
				ModuleConfig:        map[string]string{},
				MultiTenancyConfig:  &models.MultiTenancyConfig{},
				Properties: []*models.Property{
					{
						Name:              "objectProperty",
						DataType:          DataTypeObject.PropString(),
						IndexFilterable:   &vTrue,
						IndexSearchable:   &vFalse,
						IndexRangeFilters: &vFalse,
						Tokenization:      "",
						NestedProperties: []*models.NestedProperty{
							{
								Name:     "text",
								DataType: DataTypeText.PropString(),
							},
							{
								Name:     "texts",
								DataType: DataTypeTextArray.PropString(),
							},
							{
								Name:     "number",
								DataType: DataTypeNumber.PropString(),
							},
							{
								Name:     "numbers",
								DataType: DataTypeNumberArray.PropString(),
							},
							{
								Name:     "int",
								DataType: DataTypeInt.PropString(),
							},
							{
								Name:     "ints",
								DataType: DataTypeIntArray.PropString(),
							},
							{
								Name:     "date",
								DataType: DataTypeDate.PropString(),
							},
							{
								Name:     "dates",
								DataType: DataTypeDateArray.PropString(),
							},
							{
								Name:     "bool",
								DataType: DataTypeBoolean.PropString(),
							},
							{
								Name:     "bools",
								DataType: DataTypeBooleanArray.PropString(),
							},
							{
								Name:     "uuid",
								DataType: DataTypeUUID.PropString(),
							},
							{
								Name:     "uuids",
								DataType: DataTypeUUIDArray.PropString(),
							},
							{
								Name:              "nested_int",
								DataType:          DataTypeInt.PropString(),
								IndexFilterable:   &vTrue,
								IndexSearchable:   &vFalse,
								IndexRangeFilters: &vFalse,
								Tokenization:      "",
							},
							{
								Name:              "nested_number",
								DataType:          DataTypeNumber.PropString(),
								IndexFilterable:   &vTrue,
								IndexSearchable:   &vFalse,
								IndexRangeFilters: &vFalse,
								Tokenization:      "",
							},
							{
								Name:              "nested_text",
								DataType:          DataTypeText.PropString(),
								IndexFilterable:   &vTrue,
								IndexSearchable:   &vTrue,
								IndexRangeFilters: &vFalse,
								Tokenization:      models.PropertyTokenizationWord,
							},
							{
								Name:              "nested_objects",
								DataType:          DataTypeObject.PropString(),
								IndexFilterable:   &vTrue,
								IndexSearchable:   &vFalse,
								IndexRangeFilters: &vFalse,
								Tokenization:      "",
								NestedProperties: []*models.NestedProperty{
									{
										Name:              "nested_bool_lvl2",
										DataType:          DataTypeBoolean.PropString(),
										IndexFilterable:   &vTrue,
										IndexSearchable:   &vFalse,
										IndexRangeFilters: &vFalse,
										Tokenization:      "",
									},
									{
										Name:              "nested_numbers_lvl2",
										DataType:          DataTypeNumberArray.PropString(),
										IndexFilterable:   &vTrue,
										IndexSearchable:   &vFalse,
										IndexRangeFilters: &vFalse,
										Tokenization:      "",
									},
								},
							},
							{
								Name:              "nested_array_objects",
								DataType:          DataTypeObjectArray.PropString(),
								IndexFilterable:   &vTrue,
								IndexSearchable:   &vFalse,
								IndexRangeFilters: &vFalse,
								Tokenization:      "",
								NestedProperties: []*models.NestedProperty{
									{
										Name:              "nested_bool_lvl2",
										DataType:          DataTypeBoolean.PropString(),
										IndexFilterable:   &vTrue,
										IndexSearchable:   &vFalse,
										IndexRangeFilters: &vFalse,
										Tokenization:      "",
									},
									{
										Name:              "nested_numbers_lvl2",
										DataType:          DataTypeNumberArray.PropString(),
										IndexFilterable:   &vTrue,
										IndexSearchable:   &vFalse,
										IndexRangeFilters: &vFalse,
										Tokenization:      "",
									},
								},
							},
						},
					},
				},
				ReplicationConfig: &models.ReplicationConfig{},
				ShardingConfig:    sharding.Config{},
				VectorIndexType:   "hnsw",
				VectorIndexConfig: hnsw.UserConfig{},
			},
			outputModel: models.Class{
				Class:       "class",
				Description: "description",
				InvertedIndexConfig: &models.InvertedIndexConfig{
					Bm25:      &models.BM25Config{B: 0, K1: 0},
					Stopwords: &models.StopwordConfig{Additions: nil, Preset: "", Removals: nil},
				},
				ModuleConfig:       emptyModuleConfig,
				MultiTenancyConfig: &models.MultiTenancyConfig{},
				Properties: []*models.Property{
					{
						Name:              "objectProperty",
						DataType:          DataTypeObject.PropString(),
						IndexFilterable:   &vTrue,
						IndexInverted:     &vTrue,
						IndexSearchable:   &vFalse,
						IndexRangeFilters: &vFalse,
						Tokenization:      "",
						ModuleConfig:      emptyModuleConfig,
						Processing:        &models.PropertyProcessing{},
						NestedProperties: []*models.NestedProperty{
							{
								Name:              "text",
								IndexFilterable:   &vTrue,
								IndexSearchable:   &vTrue,
								IndexRangeFilters: &vFalse,
								DataType:          DataTypeText.PropString(),
								Processing:        &models.PropertyProcessing{},
							},
							{
								Name:              "texts",
								IndexFilterable:   &vTrue,
								IndexSearchable:   &vTrue,
								IndexRangeFilters: &vFalse,
								DataType:          DataTypeTextArray.PropString(),
								Processing:        &models.PropertyProcessing{},
							},
							{
								Name:              "number",
								IndexFilterable:   &vTrue,
								IndexSearchable:   &vTrue,
								IndexRangeFilters: &vFalse,
								DataType:          DataTypeNumber.PropString(),
								Processing:        &models.PropertyProcessing{},
							},
							{
								Name:              "numbers",
								IndexFilterable:   &vTrue,
								IndexSearchable:   &vTrue,
								IndexRangeFilters: &vFalse,
								DataType:          DataTypeNumberArray.PropString(),
								Processing:        &models.PropertyProcessing{},
							},
							{
								Name:              "int",
								IndexFilterable:   &vTrue,
								IndexSearchable:   &vTrue,
								IndexRangeFilters: &vFalse,
								DataType:          DataTypeInt.PropString(),
								Processing:        &models.PropertyProcessing{},
							},
							{
								Name:              "ints",
								IndexFilterable:   &vTrue,
								IndexSearchable:   &vTrue,
								IndexRangeFilters: &vFalse,
								DataType:          DataTypeIntArray.PropString(),
								Processing:        &models.PropertyProcessing{},
							},
							{
								Name:              "date",
								IndexFilterable:   &vTrue,
								IndexSearchable:   &vTrue,
								IndexRangeFilters: &vFalse,
								DataType:          DataTypeDate.PropString(),
								Processing:        &models.PropertyProcessing{},
							},
							{
								Name:              "dates",
								IndexFilterable:   &vTrue,
								IndexSearchable:   &vTrue,
								IndexRangeFilters: &vFalse,
								DataType:          DataTypeDateArray.PropString(),
								Processing:        &models.PropertyProcessing{},
							},
							{
								Name:              "bool",
								IndexFilterable:   &vTrue,
								IndexSearchable:   &vTrue,
								IndexRangeFilters: &vFalse,
								DataType:          DataTypeBoolean.PropString(),
								Processing:        &models.PropertyProcessing{},
							},
							{
								Name:              "bools",
								IndexFilterable:   &vTrue,
								IndexSearchable:   &vTrue,
								IndexRangeFilters: &vFalse,
								DataType:          DataTypeBooleanArray.PropString(),
								Processing:        &models.PropertyProcessing{},
							},
							{
								Name:              "uuid",
								IndexFilterable:   &vTrue,
								IndexSearchable:   &vTrue,
								IndexRangeFilters: &vFalse,
								DataType:          DataTypeUUID.PropString(),
								Processing:        &models.PropertyProcessing{},
							},
							{
								Name:              "uuids",
								IndexFilterable:   &vTrue,
								IndexSearchable:   &vTrue,
								IndexRangeFilters: &vFalse,
								DataType:          DataTypeUUIDArray.PropString(),
								Processing:        &models.PropertyProcessing{},
							},
							{
								Name:              "nested_int",
								DataType:          DataTypeInt.PropString(),
								IndexFilterable:   &vTrue,
								IndexSearchable:   &vFalse,
								IndexRangeFilters: &vFalse,
								Tokenization:      "",
								Processing:        &models.PropertyProcessing{},
							},
							{
								Name:              "nested_number",
								DataType:          DataTypeNumber.PropString(),
								IndexFilterable:   &vTrue,
								IndexSearchable:   &vFalse,
								IndexRangeFilters: &vFalse,
								Tokenization:      "",
								Processing:        &models.PropertyProcessing{},
							},
							{
								Name:              "nested_text",
								DataType:          DataTypeText.PropString(),
								IndexFilterable:   &vTrue,
								IndexSearchable:   &vTrue,
								IndexRangeFilters: &vFalse,
								Tokenization:      models.PropertyTokenizationWord,
								Processing:        &models.PropertyProcessing{},
							},
							{
								Name:              "nested_objects",
								DataType:          DataTypeObject.PropString(),
								IndexFilterable:   &vTrue,
								IndexSearchable:   &vFalse,
								IndexRangeFilters: &vFalse,
								Tokenization:      "",
								Processing:        &models.PropertyProcessing{},
								NestedProperties: []*models.NestedProperty{
									{
										Name:              "nested_bool_lvl2",
										DataType:          DataTypeBoolean.PropString(),
										IndexFilterable:   &vTrue,
										IndexSearchable:   &vFalse,
										IndexRangeFilters: &vFalse,
										Tokenization:      "",
										Processing:        &models.PropertyProcessing{},
									},
									{
										Name:              "nested_numbers_lvl2",
										DataType:          DataTypeNumberArray.PropString(),
										IndexFilterable:   &vTrue,
										IndexSearchable:   &vFalse,
										IndexRangeFilters: &vFalse,
										Tokenization:      "",
										Processing:        &models.PropertyProcessing{},
									},
								},
							},
							{
								Name:              "nested_array_objects",
								DataType:          DataTypeObjectArray.PropString(),
								IndexFilterable:   &vTrue,
								IndexSearchable:   &vFalse,
								IndexRangeFilters: &vFalse,
								Tokenization:      "",
								Processing:        &models.PropertyProcessing{},
								NestedProperties: []*models.NestedProperty{
									{
										Name:              "nested_bool_lvl2",
										DataType:          DataTypeBoolean.PropString(),
										IndexFilterable:   &vTrue,
										IndexSearchable:   &vFalse,
										IndexRangeFilters: &vFalse,
										Tokenization:      "",
										Processing:        &models.PropertyProcessing{},
									},
									{
										Name:              "nested_numbers_lvl2",
										DataType:          DataTypeNumberArray.PropString(),
										IndexFilterable:   &vTrue,
										IndexSearchable:   &vFalse,
										IndexRangeFilters: &vFalse,
										Tokenization:      "",
										Processing:        &models.PropertyProcessing{},
									},
								},
							},
						},
					},
				},
				ReplicationConfig: &models.ReplicationConfig{},
				ShardingConfig:    sharding.Config{},
				VectorIndexType:   "hnsw",
				VectorIndexConfig: hnsw.UserConfig{},
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			c, err := CollectionFromClass(tc.inputModel)
			if vi := tc.inputModel.VectorIndexType; vi != "" && vi != "hnsw" && vi != "flat" {
				require.NotNil(t, err)
				return
			}
			require.Nil(t, err)
			m := CollectionToClass(c)

			require.Equal(t, tc.outputModel, m)
		})
	}
}
