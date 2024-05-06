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
)

var schemaTests = []struct {
	name string
	fn   func(*testing.T, *Handler, *fakeMetaHandler)
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
	{name: "AddInvalidPropertyDuringCreation", fn: testAddInvalidPropertyDuringCreation},
	{name: "AddInvalidPropertyWithEmptyDataTypeDuringCreation", fn: testAddInvalidPropertyWithEmptyDataTypeDuringCreation},
	{name: "DropProperty", fn: testDropProperty},
}

func testAddObjectClass(t *testing.T, handler *Handler, fakeMetaHandler *fakeMetaHandler) {
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
	}
	fakeMetaHandler.On("AddClass", class, mock.Anything).Return(nil)
	_, _, err := handler.AddClass(context.Background(), nil, class)
	assert.Nil(t, err)
}

func testAddObjectClassExplicitVectorizer(t *testing.T, handler *Handler, fakeMetaHandler *fakeMetaHandler) {
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
	}
	fakeMetaHandler.On("AddClass", class, mock.Anything).Return(nil)
	_, _, err := handler.AddClass(context.Background(), nil, class)
	assert.Nil(t, err)
}

func testAddObjectClassImplicitVectorizer(t *testing.T, handler *Handler, fakeMetaHandler *fakeMetaHandler) {
	t.Parallel()
	handler.config.DefaultVectorizerModule = config.VectorizerModuleText2VecContextionary
	class := &models.Class{
		Class: "Car",
		Properties: []*models.Property{{
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
			Name:         "dummy",
		}},
	}

	fakeMetaHandler.On("AddClass", mock.Anything, mock.Anything).Return(nil)

	_, _, err := handler.AddClass(context.Background(), nil, class)
	assert.Nil(t, err)
}

func testAddObjectClassWrongVectorizer(t *testing.T, handler *Handler, fakeMetaHandler *fakeMetaHandler) {
	t.Parallel()

	class := &models.Class{
		Class:      "Car",
		Vectorizer: "vectorizer-5000000",
		Properties: []*models.Property{{
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
			Name:         "dummy",
		}},
	}

	_, _, err := handler.AddClass(context.Background(), nil, class)
	assert.Error(t, err)
}

func testAddObjectClassWrongIndexType(t *testing.T, handler *Handler, fakeMetaHandler *fakeMetaHandler) {
	t.Parallel()

	class := &models.Class{
		Class:           "Car",
		VectorIndexType: "vector-index-2-million",
		Properties: []*models.Property{{
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
			Name:         "dummy",
		}},
	}

	_, _, err := handler.AddClass(context.Background(), nil, class)
	require.NotNil(t, err)
	assert.Equal(t, "unrecognized or unsupported vectorIndexType \"vector-index-2-million\"", err.Error())
}

func testRemoveObjectClass(t *testing.T, handler *Handler, fakeMetaHandler *fakeMetaHandler) {
	t.Parallel()

	class := &models.Class{
		Class:      "Car",
		Vectorizer: "text2vec-contextionary",
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
	}

	fakeMetaHandler.On("AddClass", class, mock.Anything).Return(nil)
	_, _, err := handler.AddClass(context.Background(), nil, class)
	require.Nil(t, err)

	// Now delete the class
	fakeMetaHandler.On("DeleteClass", "Car").Return(nil)
	err = handler.DeleteClass(context.Background(), nil, "Car")
	assert.Nil(t, err)
}

func testCantAddSameClassTwice(t *testing.T, handler *Handler, fakeMetaHandler *fakeMetaHandler) {
	t.Parallel()

	reset := fakeMetaHandler.On("ReadOnlySchema").Return(models.Schema{})

	class := &models.Class{
		Class:      "Car",
		Vectorizer: "text2vec-contextionary",
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
	}
	fakeMetaHandler.On("AddClass", class, mock.Anything).Return(nil)
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
	}
	fakeMetaHandler.ExpectedCalls = fakeMetaHandler.ExpectedCalls[:0]
	fakeMetaHandler.On("AddClass", class, mock.Anything).Return(ErrNotFound)

	// Add it again
	_, _, err = handler.AddClass(context.Background(), nil, class)
	assert.NotNil(t, err)
}

func testCantAddSameClassTwiceDifferentKinds(t *testing.T, handler *Handler, fakeMetaHandler *fakeMetaHandler) {
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
	}
	fakeMetaHandler.On("AddClass", class, mock.Anything).Return(nil)
	_, _, err := handler.AddClass(ctx, nil, class)
	assert.Nil(t, err)

	class.ModuleConfig = map[string]interface{}{
		"my-module1": map[string]interface{}{
			"my-setting": "some-value",
		},
	}

	// Add it again, but with a different kind.
	fakeMetaHandler.On("AddClass", class, mock.Anything).Return(nil)
	_, _, err = handler.AddClass(context.Background(), nil, class)
	assert.NotNil(t, err)
}

// TODO: parts of this test contain text2vec-contextionary logic, but parts are
// also general logic

func testAddPropertyDuringCreation(t *testing.T, handler *Handler, fakeMetaHandler *fakeMetaHandler) {
	t.Parallel()

	vFalse := false
	vTrue := true

	var properties []*models.Property = []*models.Property{
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
		{
			Name:            "colorRaw1",
			DataType:        schema.DataTypeText.PropString(),
			Tokenization:    models.PropertyTokenizationWhitespace,
			IndexFilterable: &vFalse,
			IndexSearchable: &vFalse,
			ModuleConfig: map[string]interface{}{
				"text2vec-contextionary": map[string]interface{}{
					"skip": true,
				},
			},
		},
		{
			Name:            "colorRaw2",
			DataType:        schema.DataTypeText.PropString(),
			Tokenization:    models.PropertyTokenizationWhitespace,
			IndexFilterable: &vTrue,
			IndexSearchable: &vFalse,
			ModuleConfig: map[string]interface{}{
				"text2vec-contextionary": map[string]interface{}{
					"skip": true,
				},
			},
		},
		{
			Name:            "colorRaw3",
			DataType:        schema.DataTypeText.PropString(),
			Tokenization:    models.PropertyTokenizationWhitespace,
			IndexFilterable: &vFalse,
			IndexSearchable: &vTrue,
			ModuleConfig: map[string]interface{}{
				"text2vec-contextionary": map[string]interface{}{
					"skip": true,
				},
			},
		},
		{
			Name:            "colorRaw4",
			DataType:        schema.DataTypeText.PropString(),
			Tokenization:    models.PropertyTokenizationWhitespace,
			IndexFilterable: &vTrue,
			IndexSearchable: &vTrue,
			ModuleConfig: map[string]interface{}{
				"text2vec-contextionary": map[string]interface{}{
					"skip": true,
				},
			},
		},
		{
			Name:         "content",
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
			ModuleConfig: map[string]interface{}{
				"text2vec-contextionary": map[string]interface{}{
					"vectorizePropertyName": false,
				},
			},
		},
		{
			Name:         "allDefault",
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
		},
	}

	class := &models.Class{
		Class:      "Car",
		Properties: properties,
	}
	fakeMetaHandler.On("AddClass", class, mock.Anything).Return(nil)
	_, _, err := handler.AddClass(context.Background(), nil, class)
	assert.Nil(t, err)
}

func testAddInvalidPropertyDuringCreation(t *testing.T, handler *Handler, fakeMetaHandler *fakeMetaHandler) {
	t.Parallel()

	var properties []*models.Property = []*models.Property{
		{Name: "color", DataType: []string{"blurp"}},
	}

	_, _, err := handler.AddClass(context.Background(), nil, &models.Class{
		Class:      "Car",
		Properties: properties,
	})
	assert.NotNil(t, err)
}

func testAddInvalidPropertyWithEmptyDataTypeDuringCreation(t *testing.T, handler *Handler, fakeMetaHandler *fakeMetaHandler) {
	t.Parallel()

	var properties []*models.Property = []*models.Property{
		{Name: "color", DataType: []string{""}},
	}

	_, _, err := handler.AddClass(context.Background(), nil, &models.Class{
		Class:      "Car",
		Properties: properties,
	})
	assert.NotNil(t, err)
}

func testDropProperty(t *testing.T, handler *Handler, fakeMetaHandler *fakeMetaHandler) {
	// TODO: https://github.com/weaviate/weaviate/issues/973
	// Remove skip

	t.Skip()

	t.Parallel()

	fakeMetaHandler.On("ReadOnlySchema").Return(models.Schema{})

	var properties []*models.Property = []*models.Property{
		{Name: "color", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWhitespace},
	}
	class := &models.Class{
		Class:      "Car",
		Properties: properties,
	}
	fakeMetaHandler.On("AddClass", class, mock.Anything).Return(nil)
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
				handler, fakeMetaHandler := newTestHandler(t, &fakeDB{})
				defer fakeMetaHandler.AssertExpectations(t)
				testCase.fn(t, handler, fakeMetaHandler)
			})
		}
	})
}
