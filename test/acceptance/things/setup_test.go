package test

import (
	"testing"

	"github.com/semi-technologies/weaviate/client/schema"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
)

func Test_Things(t *testing.T) {
	t.Run("setup", func(t *testing.T) {
		createThingClass(t, &models.Class{
			Class: "TestThing",
			Properties: []*models.Property{
				&models.Property{
					Name:     "testString",
					DataType: []string{"string"},
				},
				&models.Property{
					Name:     "testWholeNumber",
					DataType: []string{"int"},
				},
				&models.Property{
					Name:     "testNumber",
					DataType: []string{"number"},
				},
				&models.Property{
					Name:     "testDateTime",
					DataType: []string{"date"},
				},
				&models.Property{
					Name:     "testTrueFalse",
					DataType: []string{"boolean"},
				},
			},
		})
	})

	// tests
	t.Run("listing things", listingThings)
	t.Run("creating things", creatingThings)

	// tear down
	deleteThingClass(t, "TestThing")
}

func createThingClass(t *testing.T, class *models.Class) {
	params := schema.NewSchemaThingsCreateParams().WithThingClass(class)
	resp, err := helper.Client(t).Schema.SchemaThingsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)
}

func deleteThingClass(t *testing.T, class string) {
	delParams := schema.NewSchemaThingsDeleteParams().WithClassName(class)
	delRes, err := helper.Client(t).Schema.SchemaThingsDelete(delParams, nil)
	helper.AssertRequestOk(t, delRes, err, nil)
}
