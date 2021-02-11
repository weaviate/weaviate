//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package schema

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TODO: These tests don't match the overall testing style in Weaviate.
// Refactor!

// The etcd manager requires a backend for now (to prevent lots of nil checks).
type NilMigrator struct{}

func (n *NilMigrator) AddClass(ctx context.Context, class *models.Class) error {
	return nil
}

func (n *NilMigrator) DropClass(ctx context.Context, className string) error {
	return nil
}

func (n *NilMigrator) UpdateClass(ctx context.Context, className string, newClassName *string) error {
	return nil
}

func (n *NilMigrator) AddProperty(ctx context.Context, className string, prop *models.Property) error {
	return nil
}

func (n *NilMigrator) UpdateProperty(ctx context.Context, className string, propName string, newName *string) error {
	return nil
}

func (n *NilMigrator) UpdatePropertyAddDataType(ctx context.Context, className string, propName string, newDataType string) error {
	return nil
}

func (n *NilMigrator) DropProperty(ctx context.Context, className string, propName string) error {
	return nil
}

var schemaTests = []struct {
	name string
	fn   func(*testing.T, *Manager)
}{
	{name: "UpdateMeta", fn: testUpdateMeta},
	{name: "AddObjectClass", fn: testAddObjectClass},
	{name: "AddObjectClassWithExplicitVectorizer", fn: testAddObjectClassExplicitVectorizer},
	{name: "AddObjectClassWithImplicitVectorizer", fn: testAddObjectClassImplicitVectorizer},
	{name: "AddObjectClassWithWrongVectorizer", fn: testAddObjectClassWrongVectorizer},
	{name: "AddObjectClassWithWrongIndexType", fn: testAddObjectClassWrongIndexType},
	{name: "RemoveObjectClass", fn: testRemoveObjectClass},
	{name: "CantAddSameClassTwice", fn: testCantAddSameClassTwice},
	{name: "CantAddSameClassTwiceDifferentKind", fn: testCantAddSameClassTwiceDifferentKinds},
	{name: "UpdateClassName", fn: testUpdateClassName},
	{name: "UpdateClassNameCollision", fn: testUpdateClassNameCollision},
	{name: "AddPropertyDuringCreation", fn: testAddPropertyDuringCreation},
	{name: "AddInvalidPropertyDuringCreation", fn: testAddInvalidPropertyDuringCreation},
	{name: "AddInvalidPropertyWithEmptyDataTypeDuringCreation", fn: testAddInvalidPropertyWithEmptyDataTypeDuringCreation},
	{name: "DropProperty", fn: testDropProperty},
	{name: "UpdatePropertyName", fn: testUpdatePropertyName},
	{name: "UpdatePropertyNameCollision", fn: testUpdatePropertyNameCollision},
	{name: "UpdatePropertyAddDataTypeNew", fn: testUpdatePropertyAddDataTypeNew},
	{name: "UpdatePropertyAddDataTypeExisting", fn: testUpdatePropertyAddDataTypeExisting},
}

func testUpdateMeta(t *testing.T, lsm *Manager) {
	t.Parallel()
	schema, err := lsm.GetSchema(nil)
	require.Nil(t, err)

	assert.Equal(t, schema.Objects.Maintainer, strfmt.Email(""))
	assert.Equal(t, schema.Objects.Name, "")

	assert.Nil(t, lsm.UpdateMeta(context.Background(), "http://new/context", "person@example.org", "somename"))

	schema, err = lsm.GetSchema(nil)
	require.Nil(t, err)

	assert.Equal(t, schema.Objects.Maintainer, strfmt.Email("person@example.org"))
	assert.Equal(t, schema.Objects.Name, "somename")
}

func testAddObjectClass(t *testing.T, lsm *Manager) {
	t.Parallel()

	objectClassesNames := testGetClassNames(lsm)
	assert.NotContains(t, objectClassesNames, "Car")

	err := lsm.AddObject(context.Background(), nil, &models.Class{
		Class: "Car",
		Properties: []*models.Property{{
			DataType: []string{"string"},
			Name:     "dummy",
		}},
		VectorIndexConfig: "this should be replaced by the parser",
	})

	assert.Nil(t, err)

	objectClassesNames = testGetClassNames(lsm)
	assert.Contains(t, objectClassesNames, "Car")

	objectClasses := testGetClasses(lsm)
	require.Len(t, objectClasses, 1)
	assert.Equal(t, config.VectorizerModuleNone, objectClasses[0].Vectorizer)
	assert.Equal(t, fakeVectorConfig{}, objectClasses[0].VectorIndexConfig)
	assert.Equal(t, int64(60), objectClasses[0].InvertedIndexConfig.CleanupIntervalSeconds,
		"the default was set")
}

func testAddObjectClassExplicitVectorizer(t *testing.T, lsm *Manager) {
	t.Parallel()

	objectClassesNames := testGetClassNames(lsm)
	assert.NotContains(t, objectClassesNames, "Car")

	err := lsm.AddObject(context.Background(), nil, &models.Class{
		Vectorizer:      config.VectorizerModuleText2VecContextionary,
		VectorIndexType: "hnsw",
		Class:           "Car",
		Properties: []*models.Property{{
			DataType: []string{"string"},
			Name:     "dummy",
		}},
	})

	assert.Nil(t, err)

	objectClassesNames = testGetClassNames(lsm)
	assert.Contains(t, objectClassesNames, "Car")

	objectClasses := testGetClasses(lsm)
	require.Len(t, objectClasses, 1)
	assert.Equal(t, config.VectorizerModuleText2VecContextionary, objectClasses[0].Vectorizer)
	assert.Equal(t, "hnsw", objectClasses[0].VectorIndexType)
}

func testAddObjectClassImplicitVectorizer(t *testing.T, lsm *Manager) {
	t.Parallel()
	lsm.config.DefaultVectorizerModule = config.VectorizerModuleText2VecContextionary

	objectClassesNames := testGetClassNames(lsm)
	assert.NotContains(t, objectClassesNames, "Car")

	err := lsm.AddObject(context.Background(), nil, &models.Class{
		Class: "Car",
		Properties: []*models.Property{{
			DataType: []string{"string"},
			Name:     "dummy",
		}},
	})

	assert.Nil(t, err)

	objectClassesNames = testGetClassNames(lsm)
	assert.Contains(t, objectClassesNames, "Car")

	objectClasses := testGetClasses(lsm)
	require.Len(t, objectClasses, 1)
	assert.Equal(t, config.VectorizerModuleText2VecContextionary, objectClasses[0].Vectorizer)
	assert.Equal(t, "hnsw", objectClasses[0].VectorIndexType)
}

func testAddObjectClassWrongVectorizer(t *testing.T, lsm *Manager) {
	t.Parallel()

	objectClassesNames := testGetClassNames(lsm)
	assert.NotContains(t, objectClassesNames, "Car")

	err := lsm.AddObject(context.Background(), nil, &models.Class{
		Class:      "Car",
		Vectorizer: "vectorizer-5000000",
		Properties: []*models.Property{{
			DataType: []string{"string"},
			Name:     "dummy",
		}},
	})

	require.NotNil(t, err)
	assert.Equal(t, "vectorizer: invalid vectorizer \"vectorizer-5000000\"",
		err.Error())
}

func testAddObjectClassWrongIndexType(t *testing.T, lsm *Manager) {
	t.Parallel()

	objectClassesNames := testGetClassNames(lsm)
	assert.NotContains(t, objectClassesNames, "Car")

	err := lsm.AddObject(context.Background(), nil, &models.Class{
		Class:           "Car",
		VectorIndexType: "vector-index-2-million",
		Properties: []*models.Property{{
			DataType: []string{"string"},
			Name:     "dummy",
		}},
	})

	require.NotNil(t, err)
	assert.Equal(t, "unrecognized or unsupported vectorIndexType "+
		"\"vector-index-2-million\"", err.Error())
}

func testRemoveObjectClass(t *testing.T, lsm *Manager) {
	t.Parallel()

	err := lsm.AddObject(context.Background(), nil, &models.Class{
		Class:      "Car",
		Vectorizer: "text2vec-contextionary",
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
	})

	assert.Nil(t, err)

	objectClasses := testGetClassNames(lsm)
	assert.Contains(t, objectClasses, "Car")

	// Now delete the class
	err = lsm.DeleteObject(context.Background(), nil, "Car")
	assert.Nil(t, err)

	objectClasses = testGetClassNames(lsm)
	assert.NotContains(t, objectClasses, "Car")
}

func testCantAddSameClassTwice(t *testing.T, lsm *Manager) {
	t.Parallel()

	err := lsm.AddObject(context.Background(), nil, &models.Class{
		Class:      "Car",
		Vectorizer: "text2vec-contextionary",
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
	})

	assert.Nil(t, err)

	// Add it again
	err = lsm.AddObject(context.Background(), nil, &models.Class{
		Class:      "Car",
		Vectorizer: "text2vec-contextionary",
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
	})

	assert.NotNil(t, err)
}

func testCantAddSameClassTwiceDifferentKinds(t *testing.T, lsm *Manager) {
	t.Parallel()

	err := lsm.AddObject(context.Background(), nil, &models.Class{
		Class:      "Car",
		Vectorizer: "text2vec-contextionary",
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
	})

	assert.Nil(t, err)

	// Add it again, but with a different kind.
	err = lsm.AddObject(context.Background(), nil, &models.Class{
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
		Class:      "Car",
		Vectorizer: "text2vec-contextionary",
	})

	assert.NotNil(t, err)
}

func testUpdateClassName(t *testing.T, lsm *Manager) {
	t.Parallel()

	// Create a simple class.
	assert.Nil(t, lsm.AddObject(context.Background(), nil,
		&models.Class{
			ModuleConfig: map[string]interface{}{
				"text2vec-contextionary": map[string]interface{}{
					"vectorizeClassName": true,
				},
			},
			Class:      "InitialName",
			Vectorizer: "text2vec-contextionary",
		}))

	// Rename it
	updated := models.Class{
		Class:      "NewName",
		Vectorizer: "text2vec-contextionary",
	}
	assert.Nil(t, lsm.UpdateObject(context.Background(), nil, "InitialName", &updated))

	objectClasses := testGetClassNames(lsm)
	require.Len(t, objectClasses, 1)
	assert.Equal(t, objectClasses[0], "NewName")
}

func testUpdateClassNameCollision(t *testing.T, lsm *Manager) {
	t.Parallel()

	// Create a class to rename
	assert.Nil(t, lsm.AddObject(context.Background(), nil,
		&models.Class{Class: "InitialName"}))

	// Create another class, that we'll collide names with.
	// For some extra action, use a Action class here.
	assert.Nil(t, lsm.AddObject(context.Background(), nil,
		&models.Class{Class: "ExistingClass"}))

	// Try to rename a class to one that already exists
	update := &models.Class{Class: "ExistingClass"}
	err := lsm.UpdateObject(context.Background(), nil, "InitialName", update)
	// Should fail
	assert.NotNil(t, err)

	// Should not change the original name
	objectClasses := testGetClassNames(lsm)
	require.Len(t, objectClasses, 2)
	assert.Equal(t, objectClasses[0], "InitialName")
	assert.Equal(t, objectClasses[1], "ExistingClass")
}

// TODO: parts of this test contain text2vec-contextionary logic, but parts are
// also general logic
func testAddPropertyDuringCreation(t *testing.T, lsm *Manager) {
	t.Parallel()

	var properties []*models.Property = []*models.Property{
		{
			Name:     "color",
			DataType: []string{"string"},
			ModuleConfig: map[string]interface{}{
				"text2vec-contextionary": map[string]interface{}{
					"vectorizePropertyName": true,
				},
			},
		},
		{
			Name:          "colorRaw",
			DataType:      []string{"string"},
			IndexInverted: pointerToFalse(),
			ModuleConfig: map[string]interface{}{
				"text2vec-contextionary": map[string]interface{}{
					"skip": true,
				},
			},
		},
		{
			Name:     "content",
			DataType: []string{"string"},
			ModuleConfig: map[string]interface{}{
				"text2vec-contextionary": map[string]interface{}{
					"vectorizePropertyName": false,
				},
			},
		},
		{
			Name:     "allDefault",
			DataType: []string{"string"},
		},
	}

	err := lsm.AddObject(context.Background(), nil, &models.Class{
		Class:      "Car",
		Properties: properties,
	})
	assert.Nil(t, err)

	objectClasses := testGetClasses(lsm)
	require.Len(t, objectClasses, 1)
	require.Len(t, objectClasses[0].Properties, 4)
	assert.Equal(t, objectClasses[0].Properties[0].Name, "color")
	assert.Equal(t, objectClasses[0].Properties[0].DataType, []string{"string"})

	assert.True(t, lsm.IndexedInverted("Car", "color"), "color should be indexed")
	assert.True(t, lsm.IndexedContextionary("Car", "color"), "color should be indexed")
	assert.False(t, lsm.IndexedInverted("Car", "colorRaw"), "color should not be indexed")
	assert.False(t, lsm.IndexedContextionary("Car", "colorRaw"), "color should not be indexed")
	assert.True(t, lsm.IndexedInverted("Car", "allDefault"), "allDefault should be indexed")
	assert.True(t, lsm.IndexedContextionary("Car", "allDefault"), "allDefault should be indexed")
}

func pointerToFalse() *bool {
	b := false
	return &b
}

func testAddInvalidPropertyDuringCreation(t *testing.T, lsm *Manager) {
	t.Parallel()

	var properties []*models.Property = []*models.Property{
		{Name: "color", DataType: []string{"blurp"}},
	}

	err := lsm.AddObject(context.Background(), nil, &models.Class{
		Class:      "Car",
		Properties: properties,
	})
	assert.NotNil(t, err)
}

func testAddInvalidPropertyWithEmptyDataTypeDuringCreation(t *testing.T, lsm *Manager) {
	t.Parallel()

	var properties []*models.Property = []*models.Property{
		{Name: "color", DataType: []string{""}},
	}

	err := lsm.AddObject(context.Background(), nil, &models.Class{
		Class:      "Car",
		Properties: properties,
	})
	assert.NotNil(t, err)
}

func testDropProperty(t *testing.T, lsm *Manager) {
	// TODO: https://github.com/semi-technologies/weaviate/issues/973
	// Remove skip

	t.Skip()

	t.Parallel()

	var properties []*models.Property = []*models.Property{
		{Name: "color", DataType: []string{"string"}},
	}

	err := lsm.AddObject(context.Background(), nil, &models.Class{
		Class:      "Car",
		Properties: properties,
	})
	assert.Nil(t, err)

	objectClasses := testGetClasses(lsm)
	require.Len(t, objectClasses, 1)
	assert.Len(t, objectClasses[0].Properties, 1)

	// Now drop the property
	lsm.DeleteObjectProperty(context.Background(), nil, "Car", "color")

	objectClasses = testGetClasses(lsm)
	require.Len(t, objectClasses, 1)
	assert.Len(t, objectClasses[0].Properties, 0)
}

func testUpdatePropertyName(t *testing.T, lsm *Manager) {
	t.Parallel()

	// Create a class & property
	var properties []*models.Property = []*models.Property{
		{Name: "color", DataType: []string{"string"}},
	}

	err := lsm.AddObject(context.Background(), nil, &models.Class{
		Class:      "Car",
		Properties: properties,
	})
	assert.Nil(t, err)

	// Update the property name
	updated := &models.Property{
		Name: "smell",
	}
	err = lsm.UpdateObjectProperty(context.Background(), nil, "Car", "color", updated)
	assert.Nil(t, err)

	// Check that the name is updated
	objectClasses := testGetClasses(lsm)
	require.Len(t, objectClasses, 1)
	require.Len(t, objectClasses[0].Properties, 1)
	assert.Equal(t, objectClasses[0].Properties[0].Name, "smell")
	assert.Equal(t, objectClasses[0].Properties[0].DataType, []string{"string"})
}

func testUpdatePropertyNameCollision(t *testing.T, lsm *Manager) {
	t.Parallel()

	// Create a class & property
	var properties []*models.Property = []*models.Property{
		{Name: "color", DataType: []string{"string"}},
		{Name: "smell", DataType: []string{"string"}},
	}

	err := lsm.AddObject(context.Background(), nil, &models.Class{
		Class:      "Car",
		Properties: properties,
	})
	assert.Nil(t, err)

	// Update the property name
	updated := &models.Property{
		Name: "smell",
	}
	err = lsm.UpdateObjectProperty(context.Background(), nil, "Car", "color", updated)
	assert.NotNil(t, err)

	// Check that the name is updated
	objectClasses := testGetClasses(lsm)
	require.Len(t, objectClasses, 1)
	require.Len(t, objectClasses[0].Properties, 2)
	assert.Equal(t, objectClasses[0].Properties[0].Name, "color")
	assert.Equal(t, objectClasses[0].Properties[1].Name, "smell")
}

func testUpdatePropertyAddDataTypeNew(t *testing.T, lsm *Manager) {
	t.Parallel()

	// Create a class & property
	properties := []*models.Property{
		{Name: "madeBy", DataType: []string{"RemoteInstance/Manufacturer"}},
	}

	err := lsm.AddObject(context.Background(), nil, &models.Class{
		Class:      "Car",
		Properties: properties,
	})
	assert.Nil(t, err)

	// Add a new datatype
	err = lsm.UpdatePropertyAddDataType(context.Background(), nil, "Car", "madeBy", "RemoteInstance/Builder")
	assert.Nil(t, err)

	// Check that the name is updated
	objectClasses := testGetClasses(lsm)
	require.Len(t, objectClasses, 1)
	require.Len(t, objectClasses[0].Properties, 1)
	assert.Equal(t, objectClasses[0].Properties[0].Name, "madeBy")
	require.Len(t, objectClasses[0].Properties[0].DataType, 2)
	assert.Equal(t, objectClasses[0].Properties[0].DataType[0], "RemoteInstance/Manufacturer")
	assert.Equal(t, objectClasses[0].Properties[0].DataType[1], "RemoteInstance/Builder")
}

func testUpdatePropertyAddDataTypeExisting(t *testing.T, lsm *Manager) {
	t.Parallel()

	// Create a class & property
	properties := []*models.Property{
		{Name: "madeBy", DataType: []string{"RemoteInstance/Manufacturer"}},
	}

	err := lsm.AddObject(context.Background(), nil, &models.Class{
		Class:      "Car",
		Properties: properties,
	})
	assert.Nil(t, err)

	// Add a new datatype
	err = lsm.UpdatePropertyAddDataType(context.Background(), nil, "Car", "madeBy", "RemoteInstance/Manufacturer")
	assert.Nil(t, err)

	// Check that the name is updated
	objectClasses := testGetClasses(lsm)
	require.Len(t, objectClasses, 1)
	require.Len(t, objectClasses[0].Properties, 1)
	assert.Equal(t, objectClasses[0].Properties[0].Name, "madeBy")
	require.Len(t, objectClasses[0].Properties[0].DataType, 1)
	assert.Equal(t, objectClasses[0].Properties[0].DataType[0], "RemoteInstance/Manufacturer")
}

// This grant parent test setups up the temporary directory needed for the tests.
func TestSchema(t *testing.T) {
	// We need this test here to make sure that we wait until all child tests
	// (that can be run in parallel) have finished, before cleaning up the temp directory.
	t.Run("group", func(t *testing.T) {
		for _, testCase := range schemaTests {
			// Create a test case, and inject the etcd schema manager in there
			// to reduce boilerplate in each separate test.
			t.Run(testCase.name, func(t *testing.T) {
				sm := newSchemaManager()
				testCase.fn(t, sm)
			})
		}
	})
}

type fakeVectorConfig struct{}

func (f fakeVectorConfig) IndexType() string {
	return "fake"
}

func dummyParseVectorConfig(in interface{}) (schema.VectorIndexConfig, error) {
	return fakeVectorConfig{}, nil
}

type fakeVectorizerValidator struct {
	valid string
}

func (f *fakeVectorizerValidator) ValidateVectorizer(moduleName string) error {
	if moduleName == f.valid {
		return nil
	}

	return errors.Errorf("invalid vectorizer %q", moduleName)
}

// New Local Schema *Manager
func newSchemaManager() *Manager {
	logger, _ := test.NewNullLogger()
	vectorizerValidator := &fakeVectorizerValidator{
		valid: config.VectorizerModuleText2VecContextionary,
	}
	sm, err := NewManager(&NilMigrator{}, newFakeRepo(),
		logger, &fakeC11y{}, &fakeAuthorizer{}, &fakeStopwordDetector{},
		config.Config{DefaultVectorizerModule: config.VectorizerModuleNone},
		dummyParseVectorConfig, // only option for now
		vectorizerValidator,
	)
	if err != nil {
		panic(err.Error())
	}

	return sm
}

func testGetClasses(l *Manager) []*models.Class {
	var classes []*models.Class
	schema, _ := l.GetSchema(nil)

	classes = append(classes, schema.SemanticSchemaFor().Classes...)

	return classes
}

func testGetClassNames(l *Manager) []string {
	var names []string
	schema, _ := l.GetSchema(nil)

	// Extract all names
	for _, class := range schema.SemanticSchemaFor().Classes {
		names = append(names, class.Class)
	}

	return names
}

func Test_ParseVectorConfigOnDiskLoad(t *testing.T) {
	logger, _ := test.NewNullLogger()

	repo := newFakeRepo()
	repo.schema = &State{
		ObjectSchema: &models.Schema{
			Classes: []*models.Class{{
				Class:             "Foo",
				VectorIndexConfig: "replace me through parsing",
				VectorIndexType:   "hnsw", // will always be set when loading from disk
			}},
		},
	}
	sm, err := NewManager(&NilMigrator{}, repo, logger, &fakeC11y{},
		&fakeAuthorizer{}, &fakeStopwordDetector{},
		config.Config{DefaultVectorizerModule: config.VectorizerModuleNone},
		dummyParseVectorConfig, // only option for now
		&fakeVectorizerValidator{},
	)
	require.Nil(t, err)

	classes := sm.GetSchemaSkipAuth().Objects.Classes
	assert.Equal(t, fakeVectorConfig{}, classes[0].VectorIndexConfig)
}
