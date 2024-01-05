//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package fetch

import (
	"fmt"

	"github.com/tailor-inc/graphql"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/common_filters"
)

// FilterBuilder can build where filters for both local and
type FilterBuilder struct {
	prefix string
}

// NewFilterBuilder with kind and prefix
func NewFilterBuilder(prefix string) *FilterBuilder {
	return &FilterBuilder{
		prefix: prefix,
	}
}

// Build a where filter ArgumentConfig
func (b *FilterBuilder) Build() *graphql.ArgumentConfig {
	return &graphql.ArgumentConfig{
		Description: descriptions.FetchWhereFilterFields,
		Type: graphql.NewNonNull(graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:        fmt.Sprintf("%sFetchObjectWhereInpObj", b.prefix),
				Fields:      b.fields(),
				Description: descriptions.FetchWhereFilterFieldsInpObj,
			},
		)),
	}
}

func (b *FilterBuilder) fields() graphql.InputObjectConfigFieldMap {
	return graphql.InputObjectConfigFieldMap{
		"class": &graphql.InputObjectFieldConfig{
			Type:        graphql.NewNonNull(b.class()),
			Description: descriptions.WhereClass,
		},
		"properties": &graphql.InputObjectFieldConfig{
			Type:        graphql.NewNonNull(graphql.NewList(b.properties())),
			Description: descriptions.WhereProperties,
		},
		"first": &graphql.InputObjectFieldConfig{
			Type:        graphql.Int,
			Description: descriptions.First,
		},
	}
}

func (b *FilterBuilder) properties() *graphql.InputObject {
	elements := common_filters.BuildNew(fmt.Sprintf("%sFetchObject", b.prefix))

	// Remove path and operands fields as they are not required here
	delete(elements, "path")
	delete(elements, "operands")

	// make operator required
	elements["operator"].Type = graphql.NewNonNull(elements["operator"].Type)

	elements["certainty"] = &graphql.InputObjectFieldConfig{
		Type:        graphql.NewNonNull(graphql.Float),
		Description: descriptions.WhereCertainty,
	}
	elements["name"] = &graphql.InputObjectFieldConfig{
		Type:        graphql.NewNonNull(graphql.String),
		Description: descriptions.WhereName,
	}
	elements["keywords"] = &graphql.InputObjectFieldConfig{
		Type:        graphql.NewList(b.keywordInpObj(fmt.Sprintf("%sFetchObjectWhereProperties", b.prefix))),
		Description: descriptions.WhereKeywords,
	}

	networkFetchWhereInpObjPropertiesObj := graphql.NewInputObject(
		graphql.InputObjectConfig{
			Name:        fmt.Sprintf("%sFetchObjectWhereInpObjProperties", b.prefix),
			Fields:      elements,
			Description: descriptions.WhereProperties,
		},
	)

	return networkFetchWhereInpObjPropertiesObj
}

func (b *FilterBuilder) keywordInpObj(prefix string) *graphql.InputObject {
	return graphql.NewInputObject(
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
}

func (b *FilterBuilder) class() *graphql.InputObject {
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
			Type:        graphql.NewList(b.keywordInpObj(fmt.Sprintf("%sFetchObjectWhereClass", b.prefix))),
			Description: descriptions.WhereKeywords,
		},
		"first": &graphql.InputObjectFieldConfig{
			Type:        graphql.Int,
			Description: descriptions.First,
		},
	}

	networkFetchWhereInpObjClassInpObj := graphql.NewInputObject(
		graphql.InputObjectConfig{
			Name:        fmt.Sprintf("%sFetchObjectWhereInpObjClassInpObj", b.prefix),
			Fields:      filterClassElements,
			Description: descriptions.WhereClass,
		},
	)
	return networkFetchWhereInpObjClassInpObj
}
