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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

var schemaTests = []struct {
	name string
	fn   func(*testing.T, *Handler, *fakeSchemaManager)
}{
	{name: "AddObjectClass", fn: testAddObjectClass},
	{name: "AddObjectClassWithExplicitVectorizer", fn: testAddObjectClassExplicitVectorizer},
	{name: "AddObjectClassWithImplicitVectorizer", fn: testAddObjectClassImplicitVectorizer},
	{name: "AddObjectClassWithWrongVectorizer", fn: testAddObjectClassWrongVectorizer},
	{name: "AddObjectClassWithWrongIndexType", fn: testAddObjectClassWrongIndexType},
	{name: "RemoveObjectClass", fn: testRemoveObjectClass},
	{name: "CantAddSameClassTwice", fn: testCantAddSameClassTwice},
	{name: "CantAddSameClassTwiceDifferentKind", fn: testCantAddSameClassTwiceDifferentKinds},
	{name: "AddPropertyDuringCreation", fn: testAddPropertyDuringCreation},
	{name: "AddPropertyWithTargetVectorConfig", fn: testAddPropertyWithTargetVectorConfig},
	{name: "AddInvalidPropertyDuringCreation", fn: testAddInvalidPropertyDuringCreation},
	{name: "AddInvalidPropertyWithEmptyDataTypeDuringCreation", fn: testAddInvalidPropertyWithEmptyDataTypeDuringCreation},
	{name: "DropProperty", fn: testDropProperty},
}

func testAddObjectClass(t *testing.T, handler *Handler, fakeSchemaManager *fakeSchemaManager) {
	t.Parallel()

	class := &models.Class{
		Class: "Car",
		Properties: []*models.Property{{
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
			Name:         "dummy",
		}},
		Vectorizer:        "model1",
		VectorIndexConfig: map[string]interface{}{},
		ReplicationConfig: &models.ReplicationConfig{Factor: 1},
	}
	fakeSchemaManager.On("AddClass", class, mock.Anything).Return(nil)
	fakeSchemaManager.On("QueryCollectionsCount").Return(0, nil)
	_, _, err := handler.AddClass(context.Background(), nil, class)
	assert.Nil(t, err)
}

func testAddObjectClassExplicitVectorizer(t *testing.T, handler *Handler, fakeSchemaManager *fakeSchemaManager) {
	t.Parallel()

	class := &models.Class{
		Vectorizer:      config.VectorizerModuleText2VecContextionary,
		VectorIndexType: "hnsw",
		Class:           "Car",
		Properties: []*models.Property{{
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
			Name:         "dummy",
		}},
		ReplicationConfig: &models.ReplicationConfig{Factor: 1},
	}
	fakeSchemaManager.On("AddClass", class, mock.Anything).Return(nil)
	fakeSchemaManager.On("QueryCollectionsCount").Return(0, nil)
	_, _, err := handler.AddClass(context.Background(), nil, class)
	assert.Nil(t, err)
}

func testAddObjectClassImplicitVectorizer(t *testing.T, handler *Handler, fakeSchemaManager *fakeSchemaManager) {
	t.Parallel()
	handler.config.DefaultVectorizerModule = config.VectorizerModuleText2VecContextionary
	class := &models.Class{
		Class: "Car",
		Properties: []*models.Property{{
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
			Name:         "dummy",
		}},
		ReplicationConfig: &models.ReplicationConfig{Factor: 1},
	}

	fakeSchemaManager.On("AddClass", mock.Anything, mock.Anything).Return(nil)
	fakeSchemaManager.On("QueryCollectionsCount").Return(0, nil)
	_, _, err := handler.AddClass(context.Background(), nil, class)
	assert.Nil(t, err)
}

func testAddObjectClassWrongVectorizer(t *testing.T, handler *Handler, fakeSchemaManager *fakeSchemaManager) {
	t.Parallel()

	class := &models.Class{
		Class:      "Car",
		Vectorizer: "vectorizer-5000000",
		Properties: []*models.Property{{
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
			Name:         "dummy",
		}},
		ReplicationConfig: &models.ReplicationConfig{Factor: 1},
	}

	_, _, err := handler.AddClass(context.Background(), nil, class)
	assert.Error(t, err)
}

func testAddObjectClassWrongIndexType(t *testing.T, handler *Handler, fakeSchemaManager *fakeSchemaManager) {
	t.Parallel()

	class := &models.Class{
		Class:           "Car",
		VectorIndexType: "vector-index-2-million",
		Properties: []*models.Property{{
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
			Name:         "dummy",
		}},
		ReplicationConfig: &models.ReplicationConfig{Factor: 1},
	}
	_, _, err := handler.AddClass(context.Background(), nil, class)
	require.NotNil(t, err)
	assert.Equal(t, "unrecognized or unsupported vectorIndexType \"vector-index-2-million\"", err.Error())
}

func testRemoveObjectClass(t *testing.T, handler *Handler, fakeSchemaManager *fakeSchemaManager) {
	t.Parallel()

	class := &models.Class{
		Class:      "Car",
		Vectorizer: "text2vec-contextionary",
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
		ReplicationConfig: &models.ReplicationConfig{Factor: 1},
	}

	fakeSchemaManager.On("AddClass", class, mock.Anything).Return(nil)
	fakeSchemaManager.On("QueryCollectionsCount").Return(0, nil)
	_, _, err := handler.AddClass(context.Background(), nil, class)
	require.Nil(t, err)

	// Now delete the class
	fakeSchemaManager.On("DeleteClass", "Car").Return(nil)
	err = handler.DeleteClass(context.Background(), nil, "Car")
	assert.Nil(t, err)
}

func testCantAddSameClassTwice(t *testing.T, handler *Handler, fakeSchemaManager *fakeSchemaManager) {
	t.Parallel()

	reset := fakeSchemaManager.On("ReadOnlySchema").Return(models.Schema{})

	class := &models.Class{
		Class:      "Car",
		Vectorizer: "text2vec-contextionary",
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
		ReplicationConfig: &models.ReplicationConfig{Factor: 1},
	}
	fakeSchemaManager.On("AddClass", class, mock.Anything).Return(nil)
	fakeSchemaManager.On("QueryCollectionsCount").Return(0, nil)
	_, _, err := handler.AddClass(context.Background(), nil, class)
	assert.Nil(t, err)

	// Reset schema to simulate the class has been added
	reset.Unset()
	class = &models.Class{
		Class:      "Car",
		Vectorizer: "text2vec-contextionary",
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
		ReplicationConfig: &models.ReplicationConfig{Factor: 1},
	}
	fakeSchemaManager.ExpectedCalls = fakeSchemaManager.ExpectedCalls[:0]
	fakeSchemaManager.On("QueryCollectionsCount").Return(0, nil)
	fakeSchemaManager.On("AddClass", class, mock.Anything).Return(ErrNotFound)

	// Add it again
	_, _, err = handler.AddClass(context.Background(), nil, class)
	assert.NotNil(t, err)
}

func testCantAddSameClassTwiceDifferentKinds(t *testing.T, handler *Handler, fakeSchemaManager *fakeSchemaManager) {
	t.Parallel()
	ctx := context.Background()
	class := &models.Class{
		Class:      "Car",
		Vectorizer: "text2vec-contextionary",
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
		ReplicationConfig: &models.ReplicationConfig{Factor: 1},
	}
	fakeSchemaManager.On("QueryCollectionsCount").Return(0, nil)
	fakeSchemaManager.On("AddClass", class, mock.Anything).Return(nil)
	_, _, err := handler.AddClass(ctx, nil, class)
	assert.Nil(t, err)

	class.ModuleConfig = map[string]interface{}{
		"my-module1": map[string]interface{}{
			"my-setting": "some-value",
		},
	}

	// Add it again, but with a different kind.
	fakeSchemaManager.On("AddClass", class, mock.Anything).Return(nil)
	_, _, err = handler.AddClass(context.Background(), nil, class)
	assert.NotNil(t, err)
}

func testAddPropertyDuringCreation(t *testing.T, handler *Handler, fakeSchemaManager *fakeSchemaManager) {
	t.Parallel()

	vFalse := false
	vTrue := true

	properties := []*models.Property{
		{
			Name:         "color",
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
		},
		{
			Name:            "colorRaw1",
			DataType:        schema.DataTypeText.PropString(),
			Tokenization:    models.PropertyTokenizationWhitespace,
			IndexFilterable: &vFalse,
			IndexSearchable: &vFalse,
		},
		{
			Name:            "colorRaw2",
			DataType:        schema.DataTypeText.PropString(),
			Tokenization:    models.PropertyTokenizationWhitespace,
			IndexFilterable: &vTrue,
			IndexSearchable: &vFalse,
		},
		{
			Name:            "colorRaw3",
			DataType:        schema.DataTypeText.PropString(),
			Tokenization:    models.PropertyTokenizationWhitespace,
			IndexFilterable: &vFalse,
			IndexSearchable: &vTrue,
		},
		{
			Name:            "colorRaw4",
			DataType:        schema.DataTypeText.PropString(),
			Tokenization:    models.PropertyTokenizationWhitespace,
			IndexFilterable: &vTrue,
			IndexSearchable: &vTrue,
		},
		{
			Name:         "content",
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
		},
		{
			Name:         "allDefault",
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
		},
	}

	class := &models.Class{
		Class:             "Car",
		Properties:        properties,
		ReplicationConfig: &models.ReplicationConfig{Factor: 1},
	}
	fakeSchemaManager.On("AddClass", class, mock.Anything).Return(nil)
	fakeSchemaManager.On("QueryCollectionsCount").Return(0, nil)
	_, _, err := handler.AddClass(context.Background(), nil, class)
	assert.Nil(t, err)
}

func testAddPropertyWithTargetVectorConfig(t *testing.T, handler *Handler, fakeSchemaManager *fakeSchemaManager) {
	t.Parallel()

	class := &models.Class{
		Class: "Car",
		Properties: []*models.Property{
			{
				Name:         "color",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
				ModuleConfig: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"vectorizePropertyName": true,
					},
				},
			},
		},
		VectorConfig: map[string]models.VectorConfig{
			"vec1": {
				Vectorizer:      map[string]interface{}{"text2vec-contextionary": map[string]interface{}{}},
				VectorIndexType: "flat",
			},
		},
		ReplicationConfig: &models.ReplicationConfig{Factor: 1},
	}
	fakeSchemaManager.On("QueryCollectionsCount").Return(0, nil)
	fakeSchemaManager.On("AddClass", class, mock.Anything).Return(nil)
	_, _, err := handler.AddClass(context.Background(), nil, class)
	require.NoError(t, err)
}

func testAddInvalidPropertyDuringCreation(t *testing.T, handler *Handler, fakeSchemaManager *fakeSchemaManager) {
	t.Parallel()

	properties := []*models.Property{
		{Name: "color", DataType: []string{"blurp"}},
	}

	_, _, err := handler.AddClass(context.Background(), nil, &models.Class{
		Class:             "Car",
		Properties:        properties,
		ReplicationConfig: &models.ReplicationConfig{Factor: 1},
	})
	assert.NotNil(t, err)
}

func testAddInvalidPropertyWithEmptyDataTypeDuringCreation(t *testing.T, handler *Handler, fakeSchemaManager *fakeSchemaManager) {
	t.Parallel()

	properties := []*models.Property{
		{Name: "color", DataType: []string{""}},
	}

	_, _, err := handler.AddClass(context.Background(), nil, &models.Class{
		Class:             "Car",
		Properties:        properties,
		ReplicationConfig: &models.ReplicationConfig{Factor: 1},
	})
	assert.NotNil(t, err)
}

func testDropProperty(t *testing.T, handler *Handler, fakeSchemaManager *fakeSchemaManager) {
	// TODO: https://github.com/weaviate/weaviate/issues/973
	// Remove skip

	t.Skip()

	t.Parallel()

	fakeSchemaManager.On("ReadOnlySchema").Return(models.Schema{})

	properties := []*models.Property{
		{Name: "color", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWhitespace},
	}
	class := &models.Class{
		Class:             "Car",
		Properties:        properties,
		ReplicationConfig: &models.ReplicationConfig{Factor: 1},
	}
	fakeSchemaManager.On("QueryCollectionsCount").Return(0, nil)
	fakeSchemaManager.On("AddClass", class, mock.Anything).Return(nil)
	_, _, err := handler.AddClass(context.Background(), nil, class)
	assert.Nil(t, err)

	// Now drop the property
	handler.DeleteClassProperty(context.Background(), nil, "Car", "color")
	// TODO: add the mock necessary to verify that the property is deleted
}

// This grant parent test setups up the temporary directory needed for the tests.
func TestSchema(t *testing.T) {
	t.Run("TestSchema", func(t *testing.T) {
		for _, testCase := range schemaTests {
			// Run each test independently with their own handler
			t.Run(testCase.name, func(t *testing.T) {
				handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})
				handler.schemaConfig.MaximumAllowedCollectionsCount = runtime.NewDynamicValue(-1)
				defer fakeSchemaManager.AssertExpectations(t)
				testCase.fn(t, handler, fakeSchemaManager)
			})
		}
	})
}
