//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package generate

import (
	"context"
	"fmt"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
)

func TestAdditionalAnswerProvider(t *testing.T) {
	t.Run("should answer", func(t *testing.T) {
		// given
		logger, _ := test.NewNullLogger()
		client := &fakeClient{}
		defaultProviderName := "openai"
		additionalGenerativeParameters := map[string]modulecapabilities.GenerativeProperty{
			defaultProviderName: {Client: client},
		}
		answerProvider := NewGeneric(additionalGenerativeParameters, defaultProviderName, logger)
		in := []search.Result{
			{
				ID: "some-uuid",
				Schema: map[string]interface{}{
					"content": "content",
				},
			},
		}
		s := "this is a task"
		fakeParams := &Params{
			Task: &s,
		}
		limit := 1
		argumentModuleParams := map[string]interface{}{}

		// when
		out, err := answerProvider.AdditionalPropertyFn(context.Background(), in, fakeParams, &limit, argumentModuleParams, nil)

		// then
		require.Nil(t, err)
		require.NotEmpty(t, out)
		assert.Equal(t, 1, len(in))
		answer, answerOK := in[0].AdditionalProperties["generate"]
		assert.True(t, answerOK)
		assert.NotNil(t, answer)
		answerAdditional, answerAdditionalOK := answer.(map[string]interface{})
		assert.True(t, answerAdditionalOK)
		groupedResult, ok := answerAdditional["groupedResult"].(*string)
		assert.True(t, ok)
		assert.Equal(t, "this is a task", *groupedResult)
	})
}

type fakeClient struct{}

func (c *fakeClient) GenerateAllResults(ctx context.Context, properties []*modulecapabilities.GenerateProperties, task string, settings interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	return c.getResults(task), nil
}

func (c *fakeClient) GenerateSingleResult(ctx context.Context, properties *modulecapabilities.GenerateProperties, prompt string, settings interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	return c.getResult(prompt), nil
}

func (c *fakeClient) getResults(task string) *modulecapabilities.GenerateResponse {
	return &modulecapabilities.GenerateResponse{
		Result: &task,
	}
}

func (c *fakeClient) getResult(task string) *modulecapabilities.GenerateResponse {
	return &modulecapabilities.GenerateResponse{
		Result: &task,
	}
}

func Test_getProperties(t *testing.T) {
	var provider GenerateProvider

	for _, tt := range []struct {
		missing  any
		dataType schema.DataType
	}{
		{nil, schema.DataTypeBlob},
		{[]string{}, schema.DataTypeTextArray},
		{nil, schema.DataTypeTextArray},
	} {
		t.Run(fmt.Sprintf("%s=%v", tt.dataType, tt.missing), func(t *testing.T) {
			result := search.Result{
				Schema: models.PropertySchema(map[string]any{
					"missing": tt.missing,
				}),
			}

			// Get provider to iterate over a result object with a nil property.
			require.NotPanics(t, func() {
				provider.getProperties(result, []string{"missing"},
					map[string]schema.DataType{"missing": tt.dataType})
			})
		})
	}
}

func Test_getProperties_parseValues(t *testing.T) {
	blobValue := "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAGAAAAA/CAYAAAAfQM0aAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAyRpVFh0WE1MOmNvbS5hZG9iZS54bXAAAAAAADw/eHBhY2tldCBiZWdpbj0i77u/IiBpZD0iVzVNME1wQ2VoaUh6cmVTek5UY3prYzlkIj8+IDx4OnhtcG1ldGEgeG1sbnM6eD0iYWRvYmU6bnM6bWV0YS8iIHg6eG1wdGs9IkFkb2JlIFhNUCBDb3JlIDUuMy1jMDExIDY2LjE0NTY2MSwgMjAxMi8wMi8wNi0xNDo1NjoyNyAgICAgICAgIj4gPHJkZjpSREYgeG1sbnM6cmRmPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5LzAyLzIyLXJkZi1zeW50YXgtbnMjIj4gPHJkZjpEZXNjcmlwdGlvbiByZGY6YWJvdXQ9IiIgeG1sbnM6eG1wPSJodHRwOi8vbnMuYWRvYmUuY29tL3hhcC8xLjAvIiB4bWxuczp4bXBNTT0iaHR0cDovL25zLmFkb2JlLmNvbS94YXAvMS4wL21tLyIgeG1sbnM6c3RSZWY9Imh0dHA6Ly9ucy5hZG9iZS5jb20veGFwLzEuMC9zVHlwZS9SZXNvdXJjZVJlZiMiIHhtcDpDcmVhdG9yVG9vbD0iQWRvYmUgUGhvdG9zaG9wIENTNiAoTWFjaW50b3NoKSIgeG1wTU06SW5zdGFuY2VJRD0ieG1wLmlpZDpCRjQ5NEM3RDI5QTkxMUUyOTc1NENCMzI4N0QwNDNCOSIgeG1wTU06RG9jdW1lbnRJRD0ieG1wLmRpZDpCRjQ5NEM3RTI5QTkxMUUyOTc1NENCMzI4N0QwNDNCOSI+IDx4bXBNTTpEZXJpdmVkRnJvbSBzdFJlZjppbnN0YW5jZUlEPSJ4bXAuaWlkOkJGNDk0QzdCMjlBOTExRTI5NzU0Q0IzMjg3RDA0M0I5IiBzdFJlZjpkb2N1bWVudElEPSJ4bXAuZGlkOkJGNDk0QzdDMjlBOTExRTI5NzU0Q0IzMjg3RDA0M0I5Ii8+IDwvcmRmOkRlc2NyaXB0aW9uPiA8L3JkZjpSREY+IDwveDp4bXBtZXRhPiA8P3hwYWNrZXQgZW5kPSJyIj8+WeGRxAAAB2hJREFUeNrUXFts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"
	latitude, longitude := float32(1.1), float32(2.2)
	for _, tt := range []struct {
		name                   string
		result                 search.Result
		propertyDataTypes      map[string]schema.DataType
		expectedTextProperties map[string]string
		expectedBlobProperties map[string]*string
	}{
		{
			name: "primitive types",
			result: search.Result{
				Schema: models.PropertySchema(map[string]any{
					"text":   "text",
					"string": "string",
					"date":   "date",
					"number": float64(0.01),
					"int":    int(100),
					"bool":   true,
					"uuid":   "bd512e32-3802-44b1-8d73-65fa8f6e9b59",
					"blob":   blobValue,
				}),
			},
			propertyDataTypes: map[string]schema.DataType{
				"text":   schema.DataTypeText,
				"string": schema.DataTypeString,
				"date":   schema.DataTypeDate,
				"number": schema.DataTypeNumber,
				"int":    schema.DataTypeInt,
				"bool":   schema.DataTypeBoolean,
				"uuid":   schema.DataTypeUUID,
				"blob":   schema.DataTypeBlob,
			},
			expectedTextProperties: map[string]string{
				"text":   "text",
				"string": "string",
				"date":   "date",
				"number": "0.01",
				"int":    "100",
				"bool":   "true",
				"uuid":   "bd512e32-3802-44b1-8d73-65fa8f6e9b59",
			},
			expectedBlobProperties: map[string]*string{
				"blob": &blobValue,
			},
		},
		{
			name: "array types",
			result: search.Result{
				Schema: models.PropertySchema(map[string]any{
					"text[]":   []string{"a", "b", "c"},
					"string[]": []string{"aa", "bb"},
					"date[]":   []string{"2025-07-12", "2025-07-18"},
					"number[]": []float64{22.01, 33.0},
					"int[]":    []any{1, 2, 3, 4},
					"bool[]":   []bool{true, false, true},
					"uuid[]":   []any{"bd512e32-3802-44b1-8d73-65fa8f6e9b58", "bd512e32-3802-44b1-8d73-65fa8f6e9b59"},
				}),
			},
			propertyDataTypes: map[string]schema.DataType{
				"text[]":   schema.DataTypeTextArray,
				"string[]": schema.DataTypeStringArray,
				"date[]":   schema.DataTypeDateArray,
				"number[]": schema.DataTypeNumberArray,
				"int[]":    schema.DataTypeIntArray,
				"bool[]":   schema.DataTypeBooleanArray,
				"uuid[]":   schema.DataTypeUUIDArray,
			},
			expectedTextProperties: map[string]string{
				"text[]":   `["a","b","c"]`,
				"string[]": `["aa","bb"]`,
				"date[]":   `["2025-07-12","2025-07-18"]`,
				"number[]": `[22.01,33]`,
				"int[]":    `[1,2,3,4]`,
				"bool[]":   `[true,false,true]`,
				"uuid[]":   `["bd512e32-3802-44b1-8d73-65fa8f6e9b58","bd512e32-3802-44b1-8d73-65fa8f6e9b59"]`,
			},
			expectedBlobProperties: nil,
		},
		{
			name: "object types",
			result: search.Result{
				Schema: models.PropertySchema(map[string]any{
					"object": map[string]any{
						"nestedProp": map[string]any{
							"a": int64(1),
							"b_nested": map[string]any{
								"b": int64(2),
								"c_nested": map[string]any{
									"c":      int64(3),
									"nested": map[string]any{},
								},
							},
						},
					},
					"object[]": []any{
						map[string]any{
							"nestedProp": map[string]any{
								"a": int64(1),
								"b_nested": map[string]any{
									"b": int64(2),
									"c_nested": map[string]any{
										"c":      int64(3),
										"nested": map[string]any{},
									},
								},
							},
						},
						map[string]any{
							"book": map[string]any{
								"author": "Frank Herbert",
								"title":  "Dune",
							},
						},
					},
				}),
			},
			propertyDataTypes: map[string]schema.DataType{
				"object":   schema.DataTypeObject,
				"object[]": schema.DataTypeObjectArray,
			},
			expectedTextProperties: map[string]string{
				"object":   `{"nestedProp":{"a":1,"b_nested":{"b":2,"c_nested":{"c":3,"nested":{}}}}}`,
				"object[]": `[{"nestedProp":{"a":1,"b_nested":{"b":2,"c_nested":{"c":3,"nested":{}}}}},{"book":{"author":"Frank Herbert","title":"Dune"}}]`,
			},
			expectedBlobProperties: nil,
		},
		{
			name: "phone number and geo and cref",
			result: search.Result{
				Schema: models.PropertySchema(map[string]any{
					"phoneNumber": &models.PhoneNumber{CountryCode: 49, Input: "500600700"},
					"phoneNumberMap": map[string]interface{}{
						"national": 0o1711234567,
					},
					"geo": &models.GeoCoordinates{Latitude: &latitude, Longitude: &longitude},
					"geoMap": map[string]interface{}{
						"latitude":  5.00005,
						"longitude": 4.00004,
					},
					"cref": []string{"AnotherClass"},
				}),
			},
			propertyDataTypes: map[string]schema.DataType{
				"phoneNumber":    schema.DataTypePhoneNumber,
				"phoneNumberMap": schema.DataTypePhoneNumber,
				"geo":            schema.DataTypeGeoCoordinates,
				"geoMap":         schema.DataTypeGeoCoordinates,
				"cref":           schema.DataTypeCRef,
			},
			expectedTextProperties: map[string]string{
				"phoneNumber":    `{"countryCode":49,"input":"500600700"}`,
				"phoneNumberMap": `{"national":254097783}`,
				"geo":            `{"latitude":1.1,"longitude":2.2}`,
				"geoMap":         `{"latitude":5.00005,"longitude":4.00004}`,
				"cref":           `["AnotherClass"]`,
			},
			expectedBlobProperties: nil,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			properties := []string{}
			for prop := range tt.expectedTextProperties {
				properties = append(properties, prop)
			}
			for prop := range tt.expectedBlobProperties {
				properties = append(properties, prop)
			}
			provider := &GenerateProvider{}
			res := provider.getProperties(tt.result, properties, tt.propertyDataTypes)
			require.NotNil(t, res)
			assert.Len(t, res.Text, len(tt.expectedTextProperties))
			for prop, val := range tt.expectedTextProperties {
				assert.Equal(t, val, res.Text[prop])
			}
			assert.Len(t, res.Blob, len(tt.expectedBlobProperties))
			for prop, val := range tt.expectedBlobProperties {
				assert.Equal(t, val, res.Blob[prop])
			}
		})
	}
}
