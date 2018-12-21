/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */
package local

import (
	"io/ioutil"
	"os"
	"testing"

	log "github.com/sirupsen/logrus"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/creativesoftwarefdn/weaviate/database"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/go-openapi/strfmt"
)

// The local manager requires a backend for now (to prevent lots of nil checks).
type NilMigrator struct{}

func (n *NilMigrator) AddClass(kind kind.Kind, class *models.SemanticSchemaClass) error {
	return nil
}
func (n *NilMigrator) DropClass(kind kind.Kind, className string) error {
	return nil
}

func (n *NilMigrator) UpdateClass(kind kind.Kind, className string, newClassName *string, newKeywords *models.SemanticSchemaKeywords) error {
	return nil
}

func (n *NilMigrator) AddProperty(kind kind.Kind, className string, prop *models.SemanticSchemaClassProperty) error {
	return nil
}

func (n *NilMigrator) UpdateProperty(kind kind.Kind, className string, propName string, newName *string, newKeywords *models.SemanticSchemaKeywords) error {
	return nil
}

func (n *NilMigrator) DropProperty(kind kind.Kind, className string, propName string) error {
	return nil
}

var schemaTests = []struct {
	name string
	fn   func(*testing.T, database.SchemaManager)
}{
	{name: "UpdateMeta", fn: testUpdateMeta},
	{name: "AddThingClass", fn: testAddThingClass},
	{name: "RemoveThingClass", fn: testRemoveThingClass},
	{name: "CantAddSameClassTwice", fn: testCantAddSameClassTwice},
	{name: "CantAddSameClassTwiceDifferentKind", fn: testCantAddSameClassTwiceDifferentKinds},
	{name: "UpdateClassName", fn: testUpdateClassName},
	{name: "UpdateClassNameCollision", fn: testUpdateClassNameCollision},
	{name: "AddThingClassWithKeywords", fn: testAddThingClassWithKeywords},
	{name: "UpdateClassKeywords", fn: testUpdateClassKeywords},
	{name: "AddPropertyDuringCreation", fn: testAddPropertyDuringCreation},
	{name: "AddInvalidPropertyDuringCreation", fn: testAddInvalidPropertyDuringCreation},
	{name: "DropProperty", fn: testDropProperty},
	{name: "UpdatePropertyName", fn: testUpdatePropertyName},
	{name: "UpdatePropertyNameCollision", fn: testUpdatePropertyNameCollision},
	{name: "UpdatePropertyKeywords", fn: testUpdatePropertyKeywords},
}

func testUpdateMeta(t *testing.T, lsm database.SchemaManager) {
	t.Parallel()

	assert.Equal(t, lsm.GetSchema().Things.AtContext, strfmt.URI(""))
	assert.Equal(t, lsm.GetSchema().Things.Maintainer, strfmt.Email(""))
	assert.Equal(t, lsm.GetSchema().Things.Name, "")

	assert.Nil(t, lsm.UpdateMeta(kind.THING_KIND, "http://new/context", "person@example.org", "somename"))

	assert.Equal(t, lsm.GetSchema().Things.AtContext, strfmt.URI("http://new/context"))
	assert.Equal(t, lsm.GetSchema().Things.Maintainer, strfmt.Email("person@example.org"))
	assert.Equal(t, lsm.GetSchema().Things.Name, "somename")
}

func testAddThingClass(t *testing.T, lsm database.SchemaManager) {
	t.Parallel()

	thingClasses := testGetClassNames(lsm, kind.THING_KIND)
	assert.NotContains(t, thingClasses, "Car")

	err := lsm.AddClass(kind.THING_KIND, &models.SemanticSchemaClass{
		Class: "Car",
	})

	assert.Nil(t, err)

	thingClasses = testGetClassNames(lsm, kind.THING_KIND)
	assert.Contains(t, thingClasses, "Car")
}

func testRemoveThingClass(t *testing.T, lsm database.SchemaManager) {
	t.Parallel()

	err := lsm.AddClass(kind.THING_KIND, &models.SemanticSchemaClass{
		Class: "Car",
	})

	assert.Nil(t, err)

	thingClasses := testGetClassNames(lsm, kind.THING_KIND)
	assert.Contains(t, thingClasses, "Car")

	// Now delete the class
	err = lsm.DropClass(kind.THING_KIND, "Car")
	assert.Nil(t, err)

	thingClasses = testGetClassNames(lsm, kind.THING_KIND)
	assert.NotContains(t, thingClasses, "Car")
}

func testCantAddSameClassTwice(t *testing.T, lsm database.SchemaManager) {
	t.Parallel()

	err := lsm.AddClass(kind.THING_KIND, &models.SemanticSchemaClass{
		Class: "Car",
	})

	assert.Nil(t, err)

	// Add it again
	err = lsm.AddClass(kind.THING_KIND, &models.SemanticSchemaClass{
		Class: "Car",
	})

	assert.NotNil(t, err)
}

func testCantAddSameClassTwiceDifferentKinds(t *testing.T, lsm database.SchemaManager) {
	t.Parallel()

	err := lsm.AddClass(kind.THING_KIND, &models.SemanticSchemaClass{
		Class: "Car",
	})

	assert.Nil(t, err)

	// Add it again, but with a different kind.
	err = lsm.AddClass(kind.ACTION_KIND, &models.SemanticSchemaClass{
		Class: "Car",
	})

	assert.NotNil(t, err)
}

func testUpdateClassName(t *testing.T, lsm database.SchemaManager) {
	t.Parallel()

	// Create a simple class.
	assert.Nil(t, lsm.AddClass(kind.THING_KIND, &models.SemanticSchemaClass{Class: "InitialName"}))

	// Rename it
	newName := "NewName"
	assert.Nil(t, lsm.UpdateClass(kind.THING_KIND, "InitialName", &newName, nil))

	thingClasses := testGetClassNames(lsm, kind.THING_KIND)
	assert.Len(t, thingClasses, 1)
	assert.Equal(t, thingClasses[0], "NewName")
}

func testUpdateClassNameCollision(t *testing.T, lsm database.SchemaManager) {
	t.Parallel()

	// Create a class to rename
	assert.Nil(t, lsm.AddClass(kind.THING_KIND, &models.SemanticSchemaClass{Class: "InitialName"}))

	// Create another class, that we'll collide names with.
	// For some extra action, use a Action class here.
	assert.Nil(t, lsm.AddClass(kind.ACTION_KIND, &models.SemanticSchemaClass{Class: "ExistingClass"}))

	// Try to rename a class to one that already exists
	collidingNewName := "ExistingClass"
	err := lsm.UpdateClass(kind.THING_KIND, "InitialName", &collidingNewName, nil)
	// Should fail
	assert.NotNil(t, err)

	// Should not change the original name
	thingClasses := testGetClassNames(lsm, kind.THING_KIND)
	assert.Len(t, thingClasses, 1)
	assert.Equal(t, thingClasses[0], "InitialName")
}

func testAddThingClassWithKeywords(t *testing.T, lsm database.SchemaManager) {
	t.Parallel()

	keywords := models.SemanticSchemaKeywords{
		{Keyword: "vehicle", Weight: 0.6},
		{Keyword: "transport", Weight: 0.4},
	}

	err := lsm.AddClass(kind.THING_KIND, &models.SemanticSchemaClass{
		Class:    "Car",
		Keywords: keywords,
	})
	assert.Nil(t, err)

	thingClasses := testGetClasses(lsm, kind.THING_KIND)
	assert.Len(t, thingClasses, 1)
	assert.Len(t, thingClasses[0].Keywords, 2)
	assert.Equal(t, thingClasses[0].Keywords[0].Keyword, "vehicle")
	assert.Equal(t, thingClasses[0].Keywords[0].Weight, float32(0.6))
	assert.Equal(t, thingClasses[0].Keywords[1].Keyword, "transport")
	assert.Equal(t, thingClasses[0].Keywords[1].Weight, float32(0.4))
}

func testUpdateClassKeywords(t *testing.T, lsm database.SchemaManager) {
	t.Parallel()

	// Create class with a keyword
	keywords := models.SemanticSchemaKeywords{
		{Keyword: "transport", Weight: 1.0},
	}

	err := lsm.AddClass(kind.THING_KIND, &models.SemanticSchemaClass{
		Class:    "Car",
		Keywords: keywords,
	})
	assert.Nil(t, err)

	//Now update just the keyword
	updatedKeywords := models.SemanticSchemaKeywords{
		{Keyword: "vehicle", Weight: 1.0},
	}

	err = lsm.UpdateClass(kind.THING_KIND, "Car", nil, &updatedKeywords)

	thingClasses := testGetClasses(lsm, kind.THING_KIND)
	assert.Len(t, thingClasses, 1)
	assert.Len(t, thingClasses[0].Keywords, 1)
	assert.Equal(t, thingClasses[0].Keywords[0].Keyword, "vehicle")
	assert.Equal(t, thingClasses[0].Keywords[0].Weight, float32(1.0))
}

func testAddPropertyDuringCreation(t *testing.T, lsm database.SchemaManager) {
	t.Parallel()

	var properties []*models.SemanticSchemaClassProperty = []*models.SemanticSchemaClassProperty{
		{Name: "color", AtDataType: []string{"string"}},
	}

	err := lsm.AddClass(kind.THING_KIND, &models.SemanticSchemaClass{
		Class:      "Car",
		Properties: properties,
	})
	assert.Nil(t, err)

	thingClasses := testGetClasses(lsm, kind.THING_KIND)
	assert.Len(t, thingClasses, 1)
	require.Len(t, thingClasses[0].Properties, 1)
	assert.Equal(t, thingClasses[0].Properties[0].Name, "color")
	assert.Equal(t, thingClasses[0].Properties[0].AtDataType, []string{"string"})
}

func testAddInvalidPropertyDuringCreation(t *testing.T, lsm database.SchemaManager) {
	t.Skip("Validation")
	t.Parallel()

	var properties []*models.SemanticSchemaClassProperty = []*models.SemanticSchemaClassProperty{
		{Name: "color", AtDataType: []string{"blurp"}},
	}

	err := lsm.AddClass(kind.THING_KIND, &models.SemanticSchemaClass{
		Class:      "Car",
		Properties: properties,
	})
	assert.NotNil(t, err)
}

func testDropProperty(t *testing.T, lsm database.SchemaManager) {
	t.Parallel()

	var properties []*models.SemanticSchemaClassProperty = []*models.SemanticSchemaClassProperty{
		{Name: "color", AtDataType: []string{"string"}},
	}

	err := lsm.AddClass(kind.THING_KIND, &models.SemanticSchemaClass{
		Class:      "Car",
		Properties: properties,
	})
	assert.Nil(t, err)

	thingClasses := testGetClasses(lsm, kind.THING_KIND)
	require.Len(t, thingClasses, 1)
	assert.Len(t, thingClasses[0].Properties, 1)

	// Now drop the property
	lsm.DropProperty(kind.THING_KIND, "Car", "color")

	thingClasses = testGetClasses(lsm, kind.THING_KIND)
	require.Len(t, thingClasses, 1)
	assert.Len(t, thingClasses[0].Properties, 0)
}

func testUpdatePropertyName(t *testing.T, lsm database.SchemaManager) {
	t.Parallel()

	// Create a class & property
	var properties []*models.SemanticSchemaClassProperty = []*models.SemanticSchemaClassProperty{
		{Name: "color", AtDataType: []string{"string"}},
	}

	err := lsm.AddClass(kind.THING_KIND, &models.SemanticSchemaClass{
		Class:      "Car",
		Properties: properties,
	})
	assert.Nil(t, err)

	// Update the property name
	smell := "smell"
	err = lsm.UpdateProperty(kind.THING_KIND, "Car", "color", &smell, nil)
	assert.Nil(t, err)

	// Check that the name is updated
	thingClasses := testGetClasses(lsm, kind.THING_KIND)
	assert.Len(t, thingClasses, 1)
	require.Len(t, thingClasses[0].Properties, 1)
	assert.Equal(t, thingClasses[0].Properties[0].Name, "smell")
	assert.Equal(t, thingClasses[0].Properties[0].AtDataType, []string{"string"})
}

func testUpdatePropertyNameCollision(t *testing.T, lsm database.SchemaManager) {
	t.Parallel()

	// Create a class & property
	var properties []*models.SemanticSchemaClassProperty = []*models.SemanticSchemaClassProperty{
		{Name: "color", AtDataType: []string{"string"}},
		{Name: "smell", AtDataType: []string{"string"}},
	}

	err := lsm.AddClass(kind.THING_KIND, &models.SemanticSchemaClass{
		Class:      "Car",
		Properties: properties,
	})
	assert.Nil(t, err)

	// Update the property name
	smell := "smell"
	err = lsm.UpdateProperty(kind.THING_KIND, "Car", "color", &smell, nil)
	assert.NotNil(t, err)

	// Check that the name is updated
	thingClasses := testGetClasses(lsm, kind.THING_KIND)
	assert.Len(t, thingClasses, 1)
	require.Len(t, thingClasses[0].Properties, 2)
	assert.Equal(t, thingClasses[0].Properties[0].Name, "color")
	assert.Equal(t, thingClasses[0].Properties[1].Name, "smell")
}

func testUpdatePropertyKeywords(t *testing.T, lsm database.SchemaManager) {
	t.Parallel()

	// Create a class Car with a property color.

	var properties []*models.SemanticSchemaClassProperty = []*models.SemanticSchemaClassProperty{
		{Name: "color", AtDataType: []string{"string"}},
	}

	err := lsm.AddClass(kind.THING_KIND, &models.SemanticSchemaClass{
		Class:      "Car",
		Properties: properties,
	})
	assert.Nil(t, err)

	thingClasses := testGetClasses(lsm, kind.THING_KIND)
	assert.Len(t, thingClasses, 1)
	require.Len(t, thingClasses[0].Properties, 1)
	assert.Equal(t, thingClasses[0].Properties[0].Name, "color")

	// Assert that there are no keywords.
	assert.Nil(t, thingClasses[0].Properties[0].Keywords)

	// Now update the property, add keywords
	newKeywords := &models.SemanticSchemaKeywords{
		&models.SemanticSchemaKeywordsItems0{Keyword: "color", Weight: 0.9},
		&models.SemanticSchemaKeywordsItems0{Keyword: "paint", Weight: 0.1},
	}

	err = lsm.UpdateProperty(kind.THING_KIND, "Car", "color", nil, newKeywords)
	assert.Nil(t, err)

	// Verify the content of the keywords.
	thingClasses = testGetClasses(lsm, kind.THING_KIND)
	assert.Len(t, thingClasses, 1)
	require.Len(t, thingClasses[0].Properties, 1)
	assert.Equal(t, "color", thingClasses[0].Properties[0].Keywords[0].Keyword)
	assert.Equal(t, float32(0.9), thingClasses[0].Properties[0].Keywords[0].Weight)
	assert.Equal(t, "paint", thingClasses[0].Properties[0].Keywords[1].Keyword)
	assert.Equal(t, float32(0.1), thingClasses[0].Properties[0].Keywords[1].Weight)
}

// This grant parent test setups up the temporary directory needed for the tests.
func TestSchema(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "test-schema-manager")
	if err != nil {
		log.Fatalf("Could not initialize temporary directory: %v\n", err)
	}

	// We need this test here to make sure that we wait until all child tests
	// (that can be run in parallel) have finished, before cleaning up the temp directory.
	t.Run("group", func(t *testing.T) {
		for _, testCase := range schemaTests {

			// Create a test case, and inject the local schema manager in there
			// to reduce boilerplate in each separate test.
			t.Run(testCase.name, func(t *testing.T) {
				lsm := newLSM(tempDir)
				testCase.fn(t, lsm)
			})
		}
	})

	os.RemoveAll(tempDir)
}

// New Local Schema Manager
func newLSM(baseTempDir string) database.SchemaManager {
	tempDir, err := ioutil.TempDir(baseTempDir, "test-schema-manager")
	if err != nil {
		log.Fatalf("Could not initialize temporary directory: %v\n", err)
	}

	lsm, err := New(tempDir, &NilMigrator{}, nil)
	if err != nil {
		panic(err)
	}

	return lsm
}

func testGetClasses(l database.SchemaManager, k kind.Kind) []*models.SemanticSchemaClass {
	var classes []*models.SemanticSchemaClass
	schema := l.GetSchema()

	for _, class := range schema.SemanticSchemaFor(k).Classes {
		classes = append(classes, class)
	}

	return classes
}

func testGetClassNames(l database.SchemaManager, k kind.Kind) []string {
	var names []string
	schema := l.GetSchema()

	// Extract all names
	for _, class := range schema.SemanticSchemaFor(k).Classes {
		names = append(names, class.Class)
	}

	return names
}
