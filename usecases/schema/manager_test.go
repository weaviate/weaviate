/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */

package schema

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/creativesoftwarefdn/weaviate/entities/schema/kind"
)

// The etcd manager requires a backend for now (to prevent lots of nil checks).
type NilMigrator struct{}

func (n *NilMigrator) AddClass(ctx context.Context, kind kind.Kind, class *models.SemanticSchemaClass) error {
	return nil
}
func (n *NilMigrator) DropClass(ctx context.Context, kind kind.Kind, className string) error {
	return nil
}

func (n *NilMigrator) UpdateClass(ctx context.Context, kind kind.Kind, className string, newClassName *string, newKeywords *models.SemanticSchemaKeywords) error {
	return nil
}

func (n *NilMigrator) AddProperty(ctx context.Context, kind kind.Kind, className string, prop *models.SemanticSchemaClassProperty) error {
	return nil
}

func (n *NilMigrator) UpdateProperty(ctx context.Context, kind kind.Kind, className string, propName string, newName *string, newKeywords *models.SemanticSchemaKeywords) error {
	return nil
}
func (n *NilMigrator) UpdatePropertyAddDataType(ctx context.Context, kind kind.Kind, className string, propName string, newDataType string) error {
	return nil
}

func (n *NilMigrator) DropProperty(ctx context.Context, kind kind.Kind, className string, propName string) error {
	return nil
}

var schemaTests = []struct {
	name string
	fn   func(*testing.T, *Manager)
}{
	{name: "UpdateMeta", fn: testUpdateMeta},
	{name: "AddThingClass", fn: testAddThingClass},
	{name: "RemoveThingClass", fn: testRemoveThingClass},
	{name: "CantAddSameClassTwice", fn: testCantAddSameClassTwice},
	{name: "CantAddSameClassTwiceDifferentKind", fn: testCantAddSameClassTwiceDifferentKinds},
	{name: "UpdateClassName", fn: testUpdateClassName},
	{name: "UpdateClassNameCollision", fn: testUpdateClassNameCollision},
	{name: "AddThingClassWithKeywords", fn: testAddThingClassWithKeywords},
	{name: "AddThingClassWithInvalidKeywordWeights", fn: testAddThingClassWithInvalidKeywordWeights},
	{name: "UpdateClassKeywords", fn: testUpdateClassKeywords},
	{name: "AddPropertyDuringCreation", fn: testAddPropertyDuringCreation},
	{name: "AddInvalidPropertyDuringCreation", fn: testAddInvalidPropertyDuringCreation},
	{name: "AddPropertyDWithInvalidKeywordWeightsDuringCreation", fn: testAddPropertyWithInvalidKeywordWeightsDuringCreation},
	{name: "DropProperty", fn: testDropProperty},
	{name: "UpdatePropertyName", fn: testUpdatePropertyName},
	{name: "UpdatePropertyNameCollision", fn: testUpdatePropertyNameCollision},
	{name: "UpdatePropertyKeywords", fn: testUpdatePropertyKeywords},
	{name: "UpdatePropertyAddDataTypeNew", fn: testUpdatePropertyAddDataTypeNew},
	{name: "UpdatePropertyAddDataTypeExisting", fn: testUpdatePropertyAddDataTypeExisting},
}

func testUpdateMeta(t *testing.T, lsm *Manager) {
	t.Parallel()

	assert.Equal(t, lsm.GetSchema().Things.Maintainer, strfmt.Email(""))
	assert.Equal(t, lsm.GetSchema().Things.Name, "")

	assert.Nil(t, lsm.UpdateMeta(context.TODO(), kind.Thing, "http://new/context", "person@example.org", "somename"))

	assert.Equal(t, lsm.GetSchema().Things.Maintainer, strfmt.Email("person@example.org"))
	assert.Equal(t, lsm.GetSchema().Things.Name, "somename")
}

func testAddThingClass(t *testing.T, lsm *Manager) {
	t.Parallel()

	thingClasses := testGetClassNames(lsm, kind.Thing)
	assert.NotContains(t, thingClasses, "Car")

	err := lsm.AddThing(context.TODO(), &models.SemanticSchemaClass{
		Class: "Car",
	})

	assert.Nil(t, err)

	thingClasses = testGetClassNames(lsm, kind.Thing)
	assert.Contains(t, thingClasses, "Car")
}

func testRemoveThingClass(t *testing.T, lsm *Manager) {
	t.Parallel()

	err := lsm.AddThing(context.TODO(), &models.SemanticSchemaClass{
		Class: "Car",
	})

	assert.Nil(t, err)

	thingClasses := testGetClassNames(lsm, kind.Thing)
	assert.Contains(t, thingClasses, "Car")

	// Now delete the class
	err = lsm.DeleteThing(context.TODO(), "Car")
	assert.Nil(t, err)

	thingClasses = testGetClassNames(lsm, kind.Thing)
	assert.NotContains(t, thingClasses, "Car")
}

func testCantAddSameClassTwice(t *testing.T, lsm *Manager) {
	t.Parallel()

	err := lsm.AddThing(context.TODO(), &models.SemanticSchemaClass{
		Class: "Car",
	})

	assert.Nil(t, err)

	// Add it again
	err = lsm.AddThing(context.TODO(), &models.SemanticSchemaClass{
		Class: "Car",
	})

	assert.NotNil(t, err)
}

func testCantAddSameClassTwiceDifferentKinds(t *testing.T, lsm *Manager) {
	t.Parallel()

	err := lsm.AddThing(context.TODO(), &models.SemanticSchemaClass{
		Class: "Car",
	})

	assert.Nil(t, err)

	// Add it again, but with a different kind.
	err = lsm.AddAction(context.TODO(), &models.SemanticSchemaClass{
		Class: "Car",
	})

	assert.NotNil(t, err)
}

func testUpdateClassName(t *testing.T, lsm *Manager) {
	t.Parallel()

	// Create a simple class.
	assert.Nil(t, lsm.AddThing(context.TODO(), &models.SemanticSchemaClass{Class: "InitialName"}))

	// Rename it
	updated := models.SemanticSchemaClass{
		Class: "NewName",
	}
	assert.Nil(t, lsm.UpdateThing(context.TODO(), "InitialName", &updated))

	thingClasses := testGetClassNames(lsm, kind.Thing)
	assert.Len(t, thingClasses, 1)
	assert.Equal(t, thingClasses[0], "NewName")
}

func testUpdateClassNameCollision(t *testing.T, lsm *Manager) {
	t.Parallel()

	// Create a class to rename
	assert.Nil(t, lsm.AddThing(context.TODO(), &models.SemanticSchemaClass{Class: "InitialName"}))

	// Create another class, that we'll collide names with.
	// For some extra action, use a Action class here.
	assert.Nil(t, lsm.AddAction(context.TODO(), &models.SemanticSchemaClass{Class: "ExistingClass"}))

	// Try to rename a class to one that already exists
	update := &models.SemanticSchemaClass{Class: "ExistingClass"}
	err := lsm.UpdateThing(context.TODO(), "InitialName", update)
	// Should fail
	assert.NotNil(t, err)

	// Should not change the original name
	thingClasses := testGetClassNames(lsm, kind.Thing)
	assert.Len(t, thingClasses, 1)
	assert.Equal(t, thingClasses[0], "InitialName")
}

func testAddThingClassWithKeywords(t *testing.T, lsm *Manager) {
	t.Parallel()

	keywords := models.SemanticSchemaKeywords{
		{Keyword: "vehicle", Weight: 0.6},
		{Keyword: "transport", Weight: 0.4},
	}

	err := lsm.AddThing(context.TODO(), &models.SemanticSchemaClass{
		Class:    "Car",
		Keywords: keywords,
	})
	assert.Nil(t, err)

	thingClasses := testGetClasses(lsm, kind.Thing)
	assert.Len(t, thingClasses, 1)
	assert.Len(t, thingClasses[0].Keywords, 2)
	assert.Equal(t, thingClasses[0].Keywords[0].Keyword, "vehicle")
	assert.Equal(t, thingClasses[0].Keywords[0].Weight, float32(0.6))
	assert.Equal(t, thingClasses[0].Keywords[1].Keyword, "transport")
	assert.Equal(t, thingClasses[0].Keywords[1].Weight, float32(0.4))
}

func testAddThingClassWithInvalidKeywordWeights(t *testing.T, lsm *Manager) {
	t.Parallel()

	// weight larger than 1.0
	keywords := models.SemanticSchemaKeywords{
		{Keyword: "vehicle", Weight: 1.2},
	}
	err := lsm.AddThing(context.TODO(), &models.SemanticSchemaClass{
		Class:    "Car",
		Keywords: keywords,
	})
	assert.NotNil(t, err)

	// weight smaller than 0
	keywords = models.SemanticSchemaKeywords{
		{Keyword: "vehicle", Weight: -0.1},
	}
	err = lsm.AddThing(context.TODO(), &models.SemanticSchemaClass{
		Class:    "Car",
		Keywords: keywords,
	})
	assert.NotNil(t, err)

	// weight exactly 1 should NOT error
	keywords = models.SemanticSchemaKeywords{
		{Keyword: "vehicle", Weight: 1},
	}
	err = lsm.AddThing(context.TODO(), &models.SemanticSchemaClass{
		Class:    "Car",
		Keywords: keywords,
	})
	assert.Nil(t, err)
}

func testUpdateClassKeywords(t *testing.T, lsm *Manager) {
	t.Parallel()

	// Create class with a keyword
	keywords := models.SemanticSchemaKeywords{
		{Keyword: "transport", Weight: 1.0},
	}

	err := lsm.AddThing(context.TODO(), &models.SemanticSchemaClass{
		Class:    "Car",
		Keywords: keywords,
	})
	assert.Nil(t, err)

	//Now update just the keyword
	updatedKeywords := models.SemanticSchemaClass{
		Class: "Car",
		Keywords: models.SemanticSchemaKeywords{

			{Keyword: "vehicle", Weight: 1.0},
		},
	}

	err = lsm.UpdateThing(context.TODO(), "Car", &updatedKeywords)

	thingClasses := testGetClasses(lsm, kind.Thing)
	assert.Len(t, thingClasses, 1)
	assert.Len(t, thingClasses[0].Keywords, 1)
	assert.Equal(t, thingClasses[0].Keywords[0].Keyword, "vehicle")
	assert.Equal(t, thingClasses[0].Keywords[0].Weight, float32(1.0))
}

func testAddPropertyDuringCreation(t *testing.T, lsm *Manager) {
	t.Parallel()

	var properties []*models.SemanticSchemaClassProperty = []*models.SemanticSchemaClassProperty{
		{Name: "color", DataType: []string{"string"}},
	}

	err := lsm.AddThing(context.TODO(), &models.SemanticSchemaClass{
		Class:      "Car",
		Properties: properties,
	})
	assert.Nil(t, err)

	thingClasses := testGetClasses(lsm, kind.Thing)
	assert.Len(t, thingClasses, 1)
	require.Len(t, thingClasses[0].Properties, 1)
	assert.Equal(t, thingClasses[0].Properties[0].Name, "color")
	assert.Equal(t, thingClasses[0].Properties[0].DataType, []string{"string"})
}

func testAddInvalidPropertyDuringCreation(t *testing.T, lsm *Manager) {
	t.Parallel()

	var properties []*models.SemanticSchemaClassProperty = []*models.SemanticSchemaClassProperty{
		{Name: "color", DataType: []string{"blurp"}},
	}

	err := lsm.AddThing(context.TODO(), &models.SemanticSchemaClass{
		Class:      "Car",
		Properties: properties,
	})
	assert.NotNil(t, err)
}

func testAddPropertyWithInvalidKeywordWeightsDuringCreation(t *testing.T, lsm *Manager) {
	t.Parallel()

	// keyword larger than 1
	var properties = []*models.SemanticSchemaClassProperty{
		{
			Name:     "color",
			DataType: []string{"string"},
			Keywords: models.SemanticSchemaKeywords{{
				Keyword: "paint",
				Weight:  1.2,
			}},
		},
	}

	err := lsm.AddThing(context.TODO(), &models.SemanticSchemaClass{
		Class:      "Car",
		Properties: properties,
	})
	assert.NotNil(t, err)

	// keyword smaller than 0
	properties = []*models.SemanticSchemaClassProperty{
		{
			Name:     "color",
			DataType: []string{"string"},
			Keywords: models.SemanticSchemaKeywords{{
				Keyword: "paint",
				Weight:  -0.1,
			}},
		},
	}

	err = lsm.AddThing(context.TODO(), &models.SemanticSchemaClass{
		Class:      "Car",
		Properties: properties,
	})
	assert.NotNil(t, err)

	// keyword exactly 1 should NOT error
	properties = []*models.SemanticSchemaClassProperty{
		{
			Name:     "color",
			DataType: []string{"string"},
			Keywords: models.SemanticSchemaKeywords{{
				Keyword: "paint",
				Weight:  1,
			}},
		},
	}

	err = lsm.AddThing(context.TODO(), &models.SemanticSchemaClass{
		Class:      "Car",
		Properties: properties,
	})
	assert.Nil(t, err)
}

func testDropProperty(t *testing.T, lsm *Manager) {
	t.Parallel()

	var properties []*models.SemanticSchemaClassProperty = []*models.SemanticSchemaClassProperty{
		{Name: "color", DataType: []string{"string"}},
	}

	err := lsm.AddThing(context.TODO(), &models.SemanticSchemaClass{
		Class:      "Car",
		Properties: properties,
	})
	assert.Nil(t, err)

	thingClasses := testGetClasses(lsm, kind.Thing)
	require.Len(t, thingClasses, 1)
	assert.Len(t, thingClasses[0].Properties, 1)

	// Now drop the property
	lsm.DeleteThingProperty(context.TODO(), "Car", "color")

	thingClasses = testGetClasses(lsm, kind.Thing)
	require.Len(t, thingClasses, 1)
	assert.Len(t, thingClasses[0].Properties, 0)
}

func testUpdatePropertyName(t *testing.T, lsm *Manager) {
	t.Parallel()

	// Create a class & property
	var properties []*models.SemanticSchemaClassProperty = []*models.SemanticSchemaClassProperty{
		{Name: "color", DataType: []string{"string"}},
	}

	err := lsm.AddThing(context.TODO(), &models.SemanticSchemaClass{
		Class:      "Car",
		Properties: properties,
	})
	assert.Nil(t, err)

	// Update the property name
	updated := &models.SemanticSchemaClassProperty{
		Name: "smell",
	}
	err = lsm.UpdateThingProperty(context.TODO(), "Car", "color", updated)
	assert.Nil(t, err)

	// Check that the name is updated
	thingClasses := testGetClasses(lsm, kind.Thing)
	assert.Len(t, thingClasses, 1)
	require.Len(t, thingClasses[0].Properties, 1)
	assert.Equal(t, thingClasses[0].Properties[0].Name, "smell")
	assert.Equal(t, thingClasses[0].Properties[0].DataType, []string{"string"})
}

func testUpdatePropertyNameCollision(t *testing.T, lsm *Manager) {
	t.Parallel()

	// Create a class & property
	var properties []*models.SemanticSchemaClassProperty = []*models.SemanticSchemaClassProperty{
		{Name: "color", DataType: []string{"string"}},
		{Name: "smell", DataType: []string{"string"}},
	}

	err := lsm.AddThing(context.TODO(), &models.SemanticSchemaClass{
		Class:      "Car",
		Properties: properties,
	})
	assert.Nil(t, err)

	// Update the property name
	updated := &models.SemanticSchemaClassProperty{
		Name: "smell",
	}
	err = lsm.UpdateThingProperty(context.TODO(), "Car", "color", updated)
	assert.NotNil(t, err)

	// Check that the name is updated
	thingClasses := testGetClasses(lsm, kind.Thing)
	assert.Len(t, thingClasses, 1)
	require.Len(t, thingClasses[0].Properties, 2)
	assert.Equal(t, thingClasses[0].Properties[0].Name, "color")
	assert.Equal(t, thingClasses[0].Properties[1].Name, "smell")
}

func testUpdatePropertyKeywords(t *testing.T, lsm *Manager) {
	t.Parallel()

	// Create a class Car with a property color.

	var properties []*models.SemanticSchemaClassProperty = []*models.SemanticSchemaClassProperty{
		{Name: "color", DataType: []string{"string"}},
	}

	err := lsm.AddThing(context.TODO(), &models.SemanticSchemaClass{
		Class:      "Car",
		Properties: properties,
	})
	assert.Nil(t, err)

	thingClasses := testGetClasses(lsm, kind.Thing)
	assert.Len(t, thingClasses, 1)
	require.Len(t, thingClasses[0].Properties, 1)
	assert.Equal(t, thingClasses[0].Properties[0].Name, "color")

	// Assert that there are no keywords.
	assert.Nil(t, thingClasses[0].Properties[0].Keywords)

	// Now update the property, add keywords
	newKeywords := &models.SemanticSchemaClassProperty{
		Keywords: models.SemanticSchemaKeywords{
			&models.SemanticSchemaKeywordsItems0{Keyword: "color", Weight: 0.9},
			&models.SemanticSchemaKeywordsItems0{Keyword: "paint", Weight: 0.1},
		},
		Name: "color",
	}

	err = lsm.UpdateThingProperty(context.TODO(), "Car", "color", newKeywords)
	assert.Nil(t, err)

	// Verify the content of the keywords.
	thingClasses = testGetClasses(lsm, kind.Thing)
	assert.Len(t, thingClasses, 1)
	require.Len(t, thingClasses[0].Properties, 1)
	assert.Equal(t, "color", thingClasses[0].Properties[0].Keywords[0].Keyword)
	assert.Equal(t, float32(0.9), thingClasses[0].Properties[0].Keywords[0].Weight)
	assert.Equal(t, "paint", thingClasses[0].Properties[0].Keywords[1].Keyword)
	assert.Equal(t, float32(0.1), thingClasses[0].Properties[0].Keywords[1].Weight)
}

func testUpdatePropertyAddDataTypeNew(t *testing.T, lsm *Manager) {
	t.Parallel()

	// Create a class & property
	var properties = []*models.SemanticSchemaClassProperty{
		{Name: "madeBy", DataType: []string{"RemoteInstance/Manufacturer"}},
	}

	err := lsm.AddThing(context.TODO(), &models.SemanticSchemaClass{
		Class:      "Car",
		Properties: properties,
	})
	assert.Nil(t, err)

	// Add a new datatype
	err = lsm.UpdatePropertyAddDataType(context.TODO(), kind.Thing, "Car", "madeBy", "RemoteInstance/Builder")
	assert.Nil(t, err)

	// Check that the name is updated
	thingClasses := testGetClasses(lsm, kind.Thing)
	assert.Len(t, thingClasses, 1)
	require.Len(t, thingClasses[0].Properties, 1)
	assert.Equal(t, thingClasses[0].Properties[0].Name, "madeBy")
	require.Len(t, thingClasses[0].Properties[0].DataType, 2)
	assert.Equal(t, thingClasses[0].Properties[0].DataType[0], "RemoteInstance/Manufacturer")
	assert.Equal(t, thingClasses[0].Properties[0].DataType[1], "RemoteInstance/Builder")
}

func testUpdatePropertyAddDataTypeExisting(t *testing.T, lsm *Manager) {
	t.Parallel()

	// Create a class & property
	var properties = []*models.SemanticSchemaClassProperty{
		{Name: "madeBy", DataType: []string{"RemoteInstance/Manufacturer"}},
	}

	err := lsm.AddThing(context.TODO(), &models.SemanticSchemaClass{
		Class:      "Car",
		Properties: properties,
	})
	assert.Nil(t, err)

	// Add a new datatype
	err = lsm.UpdatePropertyAddDataType(context.TODO(), kind.Thing, "Car", "madeBy", "RemoteInstance/Manufacturer")
	assert.Nil(t, err)

	// Check that the name is updated
	thingClasses := testGetClasses(lsm, kind.Thing)
	assert.Len(t, thingClasses, 1)
	require.Len(t, thingClasses[0].Properties, 1)
	assert.Equal(t, thingClasses[0].Properties[0].Name, "madeBy")
	require.Len(t, thingClasses[0].Properties[0].DataType, 1)
	assert.Equal(t, thingClasses[0].Properties[0].DataType[0], "RemoteInstance/Manufacturer")
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

// New Local Schema *Manager
func newSchemaManager() *Manager {
	logger, _ := test.NewNullLogger()
	sm, err := NewManager(&NilMigrator{}, newFakeRepo(), newFakeLocks(), nil, logger)
	if err != nil {
		panic(err.Error())
	}

	return sm
}

func testGetClasses(l *Manager, k kind.Kind) []*models.SemanticSchemaClass {
	var classes []*models.SemanticSchemaClass
	schema := l.GetSchema()

	for _, class := range schema.SemanticSchemaFor(k).Classes {
		classes = append(classes, class)
	}

	return classes
}

func testGetClassNames(l *Manager, k kind.Kind) []string {
	var names []string
	schema := l.GetSchema()

	// Extract all names
	for _, class := range schema.SemanticSchemaFor(k).Classes {
		names = append(names, class.Class)
	}

	return names
}
