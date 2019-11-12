package test

import (
	"testing"

	"github.com/semi-technologies/weaviate/client/schema"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
)

func Test_Actions(t *testing.T) {
	t.Run("setup", func(t *testing.T) {
		createActionClass(t, &models.Class{
			Class: "TestAction",
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
		many := "many"
		createActionClass(t, &models.Class{
			Class: "TestActionTwo",
			Properties: []*models.Property{
				&models.Property{
					Name:     "testReference",
					DataType: []string{"TestAction"},
				},
				&models.Property{
					Name:        "testReferences",
					DataType:    []string{"TestAction"},
					Cardinality: &many,
				},
				&models.Property{
					Name:     "testString",
					DataType: []string{"string"},
				},
			},
		})
	})

	// tests
	t.Run("adding actions", addingActions)
	t.Run("removing actions", removingActions)
	t.Run("action references", actionReferences)
	t.Run("updating actions", updateActions)

	// tear down
	deleteActionClass(t, "TestAction")
	deleteActionClass(t, "TestActionTwo")
}

func createActionClass(t *testing.T, class *models.Class) {
	params := schema.NewSchemaActionsCreateParams().WithActionClass(class)
	resp, err := helper.Client(t).Schema.SchemaActionsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)
}

func deleteActionClass(t *testing.T, class string) {
	delParams := schema.NewSchemaActionsDeleteParams().WithClassName(class)
	delRes, err := helper.Client(t).Schema.SchemaActionsDelete(delParams, nil)
	helper.AssertRequestOk(t, delRes, err, nil)
}
