package helper

import (
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/models"
)

var many string = "many"

var SimpleSchema = schema.Schema{
	Things: &models.SemanticSchema{
		Classes: []*models.SemanticSchemaClass{
			&models.SemanticSchemaClass{
				Class: "SomeThing",
			},
		},
	},
	Actions: &models.SemanticSchema{
		Classes: []*models.SemanticSchemaClass{
			&models.SemanticSchemaClass{
				Class: "SomeAction",
				Properties: []*models.SemanticSchemaClassProperty{
					&models.SemanticSchemaClassProperty{
						Name:       "intField",
						AtDataType: []string{"int"},
					},
					&models.SemanticSchemaClassProperty{
						Name:       "hasAction",
						AtDataType: []string{"SomeAction"},
					},
					&models.SemanticSchemaClassProperty{
						Name:        "hasActions",
						AtDataType:  []string{"SomeAction"},
						Cardinality: &many,
					},
				},
			},
		},
	},
}
