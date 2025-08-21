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

package vectorizer

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
	"github.com/weaviate/weaviate/usecases/modules"
)

func TestObjectVectorizer_TextsWithTitleProperty(t *testing.T) {
	className := "TestClass"
	var nilAnyArray []any
	asTime := func(date string) time.Time {
		if asTime, err := time.Parse(time.RFC3339, date); err == nil {
			return asTime
		}
		// fallback to current time, this will surely fail tests
		return time.Now()
	}
	tests := []struct {
		name                    string
		object                  *models.Object
		vectorizableProperties  []string
		lowerCaseInput          bool
		titlePropertyName       string
		wantCorpi               string
		weantTitlePropertyValue string
	}{
		{
			name:      "empty properties",
			object:    &models.Object{Class: className},
			wantCorpi: "Test Class",
		},
		{
			name: "nil property",
			object: &models.Object{Class: className, Properties: map[string]any{
				"nil_prop": nilAnyArray,
			}},
			vectorizableProperties: []string{"nil_prop"},
			wantCorpi:              "Test Class",
		},
		{
			name: "explicit nil property",
			object: &models.Object{Class: className, Properties: map[string]any{
				"nil_prop": nil,
			}},
			vectorizableProperties: []string{"nil_prop"},
			wantCorpi:              "Test Class",
		},
		{
			name: "string property",
			object: &models.Object{Class: className, Properties: map[string]any{
				"string_prop": "value of string property with it's OWN Casing",
			}},
			vectorizableProperties: []string{"string_prop"},
			wantCorpi:              "value of string property with it's OWN Casing",
		},
		{
			name: "string array property",
			object: &models.Object{Class: className, Properties: map[string]any{
				"string_array": []string{"value", "FROM", "String", "Property"},
			}},
			vectorizableProperties: []string{"string_array"},
			wantCorpi:              "value FROM String Property",
		},
		{
			name: "string and string array property",
			object: &models.Object{Class: className, Properties: map[string]any{
				"a_string_prop": "value of string property with it's OWN Casing",
				"string_array":  []string{"value", "FROM", "String", "Property"},
			}},
			vectorizableProperties: []string{"a_string_prop", "string_array"},
			wantCorpi:              "value of string property with it's OWN Casing value FROM String Property",
		},
		{
			name: "int array property",
			object: &models.Object{Class: className, Properties: map[string]any{
				"int_array_prop": []int64{1, 2, 3},
			}},
			vectorizableProperties: []string{"int_array_prop"},
			wantCorpi:              "1 2 3",
		},
		{
			name: "int array property as []int{}",
			object: &models.Object{Class: className, Properties: map[string]any{
				"int_array_prop": []int{1, 2, 3},
			}},
			vectorizableProperties: []string{"int_array_prop"},
			wantCorpi:              "1 2 3",
		},
		{
			name: "int array property as []any{int64}",
			object: &models.Object{Class: className, Properties: map[string]any{
				"int_array_prop": []any{int64(1), int64(2), int64(3)},
			}},
			vectorizableProperties: []string{"int_array_prop"},
			wantCorpi:              "1 2 3",
		},
		{
			name: "number property",
			object: &models.Object{Class: className, Properties: map[string]any{
				"number_prop": float64(1.1),
			}},
			vectorizableProperties: []string{"number_prop"},
			wantCorpi:              "1.1",
		},
		{
			name: "number property as json.Number",
			object: &models.Object{Class: className, Properties: map[string]any{
				"number_prop": json.Number("1.1"),
			}},
			vectorizableProperties: []string{"number_prop"},
			wantCorpi:              "1.1",
		},
		{
			name: "number array property",
			object: &models.Object{Class: className, Properties: map[string]any{
				"number_array_prop": []float64{1.1, 2.00002, 3},
			}},
			vectorizableProperties: []string{"number_array_prop"},
			wantCorpi:              "1.1 2.00002 3",
		},
		{
			name: "number array property as []any{float64}",
			object: &models.Object{Class: className, Properties: map[string]any{
				"number_array_prop": []any{float64(1.1), float64(2.00002), float64(3)},
			}},
			vectorizableProperties: []string{"number_array_prop"},
			wantCorpi:              "1.1 2.00002 3",
		},
		{
			name: "object array property as []any{map[string]any}",
			object: &models.Object{Class: className, Properties: map[string]any{
				"object_array": []any{map[string]any{"name": "something"}, map[string]any{"name": "something else"}},
			}},
			vectorizableProperties: []string{"object_array"},
			wantCorpi:              `[{"name":"something"},{"name":"something else"}]`,
		},
		{
			name: "object array property as []map[string]any",
			object: &models.Object{Class: className, Properties: map[string]any{
				"object_array": []map[string]any{{"name": "something"}, {"name": "something else"}},
			}},
			vectorizableProperties: []string{"object_array"},
			wantCorpi:              `[{"name":"something"},{"name":"something else"}]`,
		},
		{
			name: "object property",
			object: &models.Object{Class: className, Properties: map[string]any{
				"simple_obj": map[string]any{"name": map[string]any{"name": "something else"}},
			}},
			vectorizableProperties: []string{"simple_obj"},
			wantCorpi:              `{"name":{"name":"something else"}}`,
		},
		{
			name: "boolean property",
			object: &models.Object{Class: className, Properties: map[string]any{
				"boolean_prop": true,
			}},
			vectorizableProperties: []string{"boolean_prop"},
			wantCorpi:              "true",
		},
		{
			name: "boolean array property",
			object: &models.Object{Class: className, Properties: map[string]any{
				"boolean_array_prop": []bool{false, true, true},
			}},
			vectorizableProperties: []string{"boolean_array_prop"},
			wantCorpi:              "false true true",
		},
		{
			name: "boolean array property as []any{bool}",
			object: &models.Object{Class: className, Properties: map[string]any{
				"boolean_array_prop": []any{false, true, true},
			}},
			vectorizableProperties: []string{"boolean_array_prop"},
			wantCorpi:              "false true true",
		},
		{
			name: "date property as time.Time",
			object: &models.Object{Class: className, Properties: map[string]any{
				"date_prop": asTime("2011-05-05T07:16:30+02:00"),
			}},
			vectorizableProperties: []string{"date_prop"},
			wantCorpi:              "2011-05-05T07:16:30+02:00",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			targetVector := "targetVector"
			class := &models.Class{
				Class: className,
				VectorConfig: map[string]models.VectorConfig{
					targetVector: {
						Vectorizer: map[string]any{
							"my-module": map[string]any{
								"vectorizeClassName": false,
								"properties":         tt.vectorizableProperties,
							},
						},
						VectorIndexType: "hnsw",
					},
				},
			}
			cfg := modules.NewClassBasedModuleConfig(class, "my-module", "tenant", targetVector, nil)
			icheck := basesettings.NewBaseClassSettings(cfg, tt.lowerCaseInput)
			v := &ObjectVectorizer{}
			corpi, titlePropertyValue := v.TextsWithTitleProperty(context.TODO(), tt.object, icheck, tt.titlePropertyName)
			assert.Equal(t, tt.wantCorpi, corpi)
			assert.Equal(t, tt.weantTitlePropertyValue, titlePropertyValue)
		})
	}
}
