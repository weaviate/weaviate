//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package books

import (
	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
)

const defaultClassName = "Books"

const (
	Dune                  strfmt.UUID = "67b79643-cf8b-4b22-b206-6e63dbb4e000"
	ProjectHailMary       strfmt.UUID = "67b79643-cf8b-4b22-b206-6e63dbb4e001"
	TheLordOfTheIceGarden strfmt.UUID = "67b79643-cf8b-4b22-b206-6e63dbb4e002"
)

func ClassContextionaryVectorizer() *models.Class {
	return class(defaultClassName, "text2vec-contextionary")
}

func ClassTransformersVectorizer() *models.Class {
	return class(defaultClassName, "text2vec-transformers")
}

func ClassTransformersVectorizerWithName(className string) *models.Class {
	return class(className, "text2vec-transformers")
}

func class(className, vectorizer string) *models.Class {
	return &models.Class{
		Class:      className,
		Vectorizer: vectorizer,
		ModuleConfig: map[string]interface{}{
			vectorizer: map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
		Properties: []*models.Property{
			{
				Name:     "title",
				DataType: []string{"string"},
				ModuleConfig: map[string]interface{}{
					vectorizer: map[string]interface{}{
						"skip": false,
					},
				},
			},
			{
				Name:     "description",
				DataType: []string{"string"},
				ModuleConfig: map[string]interface{}{
					vectorizer: map[string]interface{}{
						"skip": false,
					},
				},
			},
		},
	}
}

func Objects() []*models.Object {
	return objects(defaultClassName)
}

func ObjectsWithName(className string) []*models.Object {
	return objects(className)
}

func objects(className string) []*models.Object {
	return []*models.Object{
		{
			Class: className,
			ID:    Dune,
			Properties: map[string]interface{}{
				"title":       "Dune",
				"description": "Dune is a 1965 epic science fiction novel by American author Frank Herbert.",
			},
		},
		{
			Class: className,
			ID:    ProjectHailMary,
			Properties: map[string]interface{}{
				"title":       "Project Hail Mary",
				"description": "Project Hail Mary is a 2021 science fiction novel by American novelist Andy Weir.",
			},
		},
		{
			Class: className,
			ID:    TheLordOfTheIceGarden,
			Properties: map[string]interface{}{
				"title":       "The Lord of the Ice Garden",
				"description": "The Lord of the Ice Garden (Polish: Pan Lodowego Ogrodu) is a four-volume science fiction and fantasy novel by Polish writer Jaroslaw Grzedowicz.",
			},
		},
	}
}
