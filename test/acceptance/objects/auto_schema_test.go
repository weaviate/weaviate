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

package test

// Acceptance tests for objects.

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/client/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

const UUID = strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168241")

func TestAutoSchemaWithDifferentProperties(t *testing.T) {
	// Add two objects with different properties to the same class. With autoschema enabled both should be added and
	// the class should have properties form both classes at the end
	className := "RandomName234234"

	testCases := []struct {
		name  string
		names []string
	}{
		{name: "UpperCase", names: []string{"NonExistingProperty", "OtherNonExistingProperty"}},
		{name: "LowerCase", names: []string{"nonExistingProperty", "otherNonExistingProperty"}},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			obj1 := &models.Object{
				Class: className,
				Properties: map[string]interface{}{
					test.names[0]: "test",
				},
			}
			params := objects.NewObjectsCreateParams().WithBody(obj1)
			resp, err := helper.Client(t).Objects.ObjectsCreate(params, nil)
			helper.AssertRequestOk(t, resp, err, nil)

			obj2 := &models.Object{
				Class: className,
				Properties: map[string]interface{}{
					test.names[1]: "test",
				},
			}
			params2 := objects.NewObjectsCreateParams().WithBody(obj2)
			resp2, err2 := helper.Client(t).Objects.ObjectsCreate(params2, nil)
			helper.AssertRequestOk(t, resp2, err2, nil)

			SchemaParams := schema.NewSchemaDumpParams()
			resp3, err3 := helper.Client(t).Schema.SchemaDump(SchemaParams, nil)
			helper.AssertRequestOk(t, resp3, err3, nil)
			assert.Len(t, resp3.Payload.Classes, 1)
			class := resp3.Payload.Classes[0]
			assert.Len(t, class.Properties, 2)
			props := class.Properties
			assert.ElementsMatch(t, []string{props[0].Name, props[1].Name}, []string{"nonExistingProperty", "otherNonExistingProperty"})
			deleteObjectClass(t, className)
		})
	}
}

// run from setup_test.go
func autoSchemaObjects(t *testing.T) {
	autoSchemaObjectTestCases := []struct {
		// the name of the test
		name string
		// the example object, with non existent classes and properties.
		object func() *models.Object
	}{
		{
			name: "non existing class",
			object: func() *models.Object {
				return &models.Object{
					ID:    "8e2997f2-1972-4ee2-ad35-5fc704f2893e",
					Class: "NonExistingClass",
					Properties: map[string]interface{}{
						"testString":  "test",
						"testNumber":  json.Number("1"),
						"testDate":    "2002-10-02T15:00:00Z",
						"testBoolean": true,
						"testGeoCoordinates": map[string]interface{}{
							"latitude":  json.Number("1.01"),
							"longitude": json.Number("1.01"),
						},
						"testPhoneNumber": map[string]interface{}{
							"input":          "020 1234567",
							"defaultCountry": "nl",
						},
						"textArray":   []string{"a", "b", "c"},
						"intArray":    []int{1, 2, 3},
						"numberArray": []int{11.0, 22.0, 33.0},
					},
				}
			},
		},
		{
			name: "non existing property",
			object: func() *models.Object {
				return &models.Object{
					Class: "TestObject",
					Properties: map[string]interface{}{
						"nonExistingProperty": "test",
					},
				}
			},
		},
		{
			name: "non existing property update class",
			object: func() *models.Object {
				return &models.Object{
					ID:    "8e2997f2-1972-4ee2-ad35-5fc704f2893f",
					Class: "TestObject",
					Properties: map[string]interface{}{
						"nonExistingDateProperty":   "2002-10-02T15:00:00Z",
						"nonExistingNumberProperty": json.Number("1"),
					},
				}
			},
		},
	}

	t.Run("auto schema should create object with missing classes and properties", func(t *testing.T) {
		for _, example_ := range autoSchemaObjectTestCases {
			t.Run(example_.name, func(t *testing.T) {
				example := example_ // Needed; example is updated to point to a new test case.
				t.Parallel()

				params := objects.NewObjectsCreateParams().WithBody(example.object())
				resp, err := helper.Client(t).Objects.ObjectsCreate(params, nil)
				helper.AssertRequestOk(t, resp, err, nil)
			})
		}
	})

	autoSchemaCrossRefTestCases := []struct {
		// the name of the test
		name string
		// the example object, with non existent classes and properties.
		object func() *models.Object
	}{
		{
			name: "non existing cross ref property update class",
			object: func() *models.Object {
				return &models.Object{
					Class: "TestObject",
					Properties: map[string]interface{}{
						"hasNonExistingClass": []interface{}{
							map[string]interface{}{
								"beacon": "weaviate://localhost/8e2997f2-1972-4ee2-ad35-5fc704f2893e",
							},
						},
					},
				}
			},
		},
		{
			name: "non existing cross ref property update class",
			object: func() *models.Object {
				return &models.Object{
					Class: "TestObject",
					Properties: map[string]interface{}{
						"hasNonExistingClassAndTestObject": []interface{}{
							map[string]interface{}{
								"beacon": "weaviate://localhost/8e2997f2-1972-4ee2-ad35-5fc704f2893e",
							},
							map[string]interface{}{
								"beacon": "weaviate://localhost/8e2997f2-1972-4ee2-ad35-5fc704f2893f",
							},
						},
					},
				}
			},
		},
	}

	t.Run("auto schema should create object with missing cross ref properties", func(t *testing.T) {
		for _, example_ := range autoSchemaCrossRefTestCases {
			t.Run(example_.name, func(t *testing.T) {
				example := example_ // Needed; example is updated to point to a new test case.
				params := objects.NewObjectsCreateParams().WithBody(example.object())
				resp, err := helper.Client(t).Objects.ObjectsCreate(params, nil)
				helper.AssertRequestOk(t, resp, err, nil)
			})
		}
	})
}

func TestClassCapitalisationInBatchWithAutoSchemaAndExistingClass(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviateWithGRPC().
		WithWeaviateEnv("AUTOSCHEMA_ENABLED", "true").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	defer helper.ResetClient()
	helper.SetupClient(compose.GetWeaviate().URI())
	helper.SetupGRPCClient(t, compose.GetWeaviate().GrpcURI())

	helper.CreateClass(t, &models.Class{
		Class: "Test",
		Properties: []*models.Property{{
			DataType: []string{"text"},
			Name:     "name",
		}},
	})
	defer helper.DeleteClass(t, "Test")

	t.Run("batch insert object over grpc with lowercase class name", func(t *testing.T) {
		res, err := helper.ClientGRPC(t).BatchObjects(ctx, &pb.BatchObjectsRequest{
			Objects: []*pb.BatchObject{{
				Collection: "test",
				Properties: &pb.BatchObject_Properties{
					NonRefProperties: &structpb.Struct{
						Fields: map[string]*structpb.Value{
							"name": {
								Kind: &structpb.Value_StringValue{
									StringValue: "test",
								},
							},
						},
					},
				},
				Uuid: string(UUID),
			}},
		})
		require.Nil(t, err)
		require.Len(t, res.Errors, 0)
	})

	t.Run("batch insert object over rest with lowercase class name", func(t *testing.T) {
		res, err := helper.Client(t).Batch.BatchObjectsCreate(batch.NewBatchObjectsCreateParams().WithBody(batch.BatchObjectsCreateBody{
			Objects: []*models.Object{
				{
					Class: "test",
					Properties: map[string]interface{}{
						"name": "test",
					},
				},
			},
		}), nil)
		require.Nil(t, err)
		require.Nil(t, res.Payload[0].Result.Errors)
	})
}
