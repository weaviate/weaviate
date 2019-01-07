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
package helper

import (
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/models"
)

var many = "many"

var SimpleSchema = schema.Schema{
	Things: &models.SemanticSchema{
		Classes: []*models.SemanticSchemaClass{
			&models.SemanticSchemaClass{
				Class: "SomeThing",
				Properties: []*models.SemanticSchemaClassProperty{
					&models.SemanticSchemaClassProperty{
						Name:       "NetworkRefField",
						AtDataType: []string{"OtherInstance/SomeRemoteClass"},
					},
				},
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
