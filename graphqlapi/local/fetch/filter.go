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

package fetch

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/descriptions"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
	"github.com/graphql-go/graphql"
)

func whereFilterFields(k kind.Kind) graphql.InputObjectConfigFieldMap {
	return graphql.InputObjectConfigFieldMap{
		"class": &graphql.InputObjectFieldConfig{
			Type:        graphql.NewNonNull(whereInpObjClassInpObj(k)),
			Description: descriptions.WhereClass,
		},
		"properties": &graphql.InputObjectFieldConfig{
			Type:        graphql.NewNonNull(graphql.NewList(whereInpObjPropertiesObj(k))),
			Description: descriptions.WhereProperties,
		},
		"first": &graphql.InputObjectFieldConfig{
			Type:        graphql.Int,
			Description: descriptions.First,
		},
	}
}

func whereInpObjPropertiesObj(k kind.Kind) *graphql.InputObject {
	filterPropertiesElements := common_filters.BuildNew(fmt.Sprintf("WeaviateLocalFetch%s", k.TitleizedName()))

	// Remove path and operands fields as they are not required here
	delete(filterPropertiesElements, "path")
	delete(filterPropertiesElements, "operands")

	filterPropertiesElements["certainty"] = &graphql.InputObjectFieldConfig{
		Type:        graphql.Float,
		Description: descriptions.WhereCertainty,
	}
	filterPropertiesElements["name"] = &graphql.InputObjectFieldConfig{
		Type:        graphql.String,
		Description: descriptions.WhereName,
	}
	filterPropertiesElements["keywords"] = &graphql.InputObjectFieldConfig{
		Type:        graphql.NewList(keywordInpObj(fmt.Sprintf("LocalFetch%sWhereProperties", k.TitleizedName()))),
		Description: descriptions.WhereKeywords,
	}

	networkFetchWhereInpObjPropertiesObj := graphql.NewInputObject(
		graphql.InputObjectConfig{
			Name:        fmt.Sprintf("WeaviateLocalFetch%sWhereInpObjProperties", k.TitleizedName()),
			Fields:      filterPropertiesElements,
			Description: descriptions.WhereProperties,
		},
	)

	return networkFetchWhereInpObjPropertiesObj
}

func keywordInpObj(prefix string) *graphql.InputObject {
	outputObject := graphql.NewInputObject(
		graphql.InputObjectConfig{
			Name: fmt.Sprintf("%sKeywordsInpObj", prefix),
			Fields: graphql.InputObjectConfigFieldMap{
				"value": &graphql.InputObjectFieldConfig{
					Type:        graphql.String,
					Description: descriptions.WhereKeywordsValue,
				},
				"weight": &graphql.InputObjectFieldConfig{
					Type:        graphql.Float,
					Description: descriptions.WhereKeywordsWeight,
				},
			},
			Description: descriptions.WhereKeywordsInpObj,
		},
	)
	return outputObject
}

func whereInpObjClassInpObj(k kind.Kind) *graphql.InputObject {
	filterClassElements := graphql.InputObjectConfigFieldMap{
		"name": &graphql.InputObjectFieldConfig{
			Type:        graphql.String,
			Description: descriptions.WhereName,
		},
		"certainty": &graphql.InputObjectFieldConfig{
			Type:        graphql.Float,
			Description: descriptions.WhereCertainty,
		},
		"keywords": &graphql.InputObjectFieldConfig{
			Type:        graphql.NewList(keywordInpObj(fmt.Sprintf("LocalFetch%sWhereClass", k.TitleizedName()))),
			Description: descriptions.WhereKeywords,
		},
		"first": &graphql.InputObjectFieldConfig{
			Type:        graphql.Int,
			Description: descriptions.First,
		},
	}

	networkFetchWhereInpObjClassInpObj := graphql.NewInputObject(
		graphql.InputObjectConfig{
			Name:        fmt.Sprintf("WeaviateLocalFetch%sWhereInpObjClassInpObj", k.TitleizedName()),
			Fields:      filterClassElements,
			Description: descriptions.WhereClass,
		},
	)
	return networkFetchWhereInpObjClassInpObj
}
