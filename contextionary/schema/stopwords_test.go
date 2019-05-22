/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */package schema

import (
	"testing"

	"github.com/semi-technologies/weaviate/contextionary"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/stretchr/testify/assert"
)

func Test_SchemaContextionary_WithStopwords(t *testing.T) {

	rawC := fakeRawContextinoaryForTest()

	schema := schema.Schema{
		Actions: &models.SemanticSchema{
			Classes: []*models.SemanticSchemaClass{
				&models.SemanticSchemaClass{
					Class: "Car",
					Properties: []*models.SemanticSchemaClassProperty{
						&models.SemanticSchemaClassProperty{
							Name: "power",
						},
					},
					Keywords: models.SemanticSchemaKeywords{
						&models.SemanticSchemaKeywordsItems0{
							Keyword: "car",
							Weight:  0.5,
						},
					},
				},
				&models.SemanticSchemaClass{
					Class: "ACar",
					Properties: []*models.SemanticSchemaClassProperty{
						&models.SemanticSchemaClassProperty{
							Name: "power",
							Keywords: models.SemanticSchemaKeywords{
								&models.SemanticSchemaKeywordsItems0{
									Keyword: "car",
									Weight:  0.5,
								},
								&models.SemanticSchemaKeywordsItems0{
									Keyword: "the",
									Weight:  0.5,
								},
							},
						},
						&models.SemanticSchemaClassProperty{
							Name: "thePower",
						},
					},
					Keywords: models.SemanticSchemaKeywords{
						&models.SemanticSchemaKeywordsItems0{
							Keyword: "power",
							Weight:  0.5,
						},
						&models.SemanticSchemaKeywordsItems0{
							Keyword: "in",
							Weight:  0.5,
						},
					},
				},
				&models.SemanticSchemaClass{
					Class: "TheCarInA",
					Properties: []*models.SemanticSchemaClassProperty{
						&models.SemanticSchemaClassProperty{
							Name: "power",
						},
						&models.SemanticSchemaClassProperty{
							Name: "thePowerInACar",
						},
					},
				},
			},
		},
		Things: &models.SemanticSchema{
			Classes: []*models.SemanticSchemaClass{},
		},
	}

	_, err := BuildInMemoryContextionaryFromSchema(schema, &rawC, &fakeStopWordDetector{})
	assert.Nil(t, err)
}

type fakeStopWordDetector struct{}

func (f *fakeStopWordDetector) IsStopWord(word string) bool {
	return word == "the" || word == "a" || word == "in"
}

func fakeRawContextinoaryForTest() contextionary.Contextionary {
	builder := contextionary.InMemoryBuilder(3)
	builder.AddWord("car", contextionary.NewVector([]float32{1, 1, 1}))
	builder.AddWord("power", contextionary.NewVector([]float32{1, 1, 1}))
	return contextionary.Contextionary(builder.Build(3))
}
