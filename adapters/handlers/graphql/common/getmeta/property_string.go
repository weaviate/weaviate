//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
//  DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package getmeta

import (
	"fmt"

	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/common"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/semi-technologies/weaviate/entities/models"
)

func stringPropertyFields(class *models.Class,
	property *models.Property, prefix string) *graphql.Object {
	getMetaDateFields := graphql.Fields{
		"type": &graphql.Field{
			Name:        fmt.Sprintf("%s%sType", prefix, class.Class),
			Description: descriptions.MetaPropertyType,
			Type:        graphql.String,
		},
		"count": &graphql.Field{
			Name:        fmt.Sprintf("%s%sCount", prefix, class.Class),
			Description: descriptions.MetaPropertyCount,
			Type:        graphql.Int,
			Resolve:     common.JSONNumberResolver,
		},
		"topOccurrences": &graphql.Field{
			Name:        fmt.Sprintf("%s%sTopOccurrences", prefix, class.Class),
			Description: descriptions.MetaPropertyTopOccurrences,
			Type:        graphql.NewList(stringTopOccurrences(class, property, prefix)),
			Args: graphql.FieldConfigArgument{
				"first": &graphql.ArgumentConfig{
					Description: descriptions.First,
					Type:        graphql.Int,
				},
				"after": &graphql.ArgumentConfig{
					Description: descriptions.After,
					Type:        graphql.Int,
				},
			},
		},
	}

	return graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%sObj", prefix, class.Class, property.Name),
		Fields:      getMetaDateFields,
		Description: descriptions.MetaPropertyObject,
	})
}

func stringTopOccurrences(class *models.Class,
	property *models.Property, prefix string) *graphql.Object {
	getMetaMetaPointingFields := graphql.Fields{
		"value": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sTopOccurrencesValue", prefix, class.Class, property.Name),
			Description: descriptions.MetaPropertyTopOccurrencesValue,
			Type:        graphql.String,
		},
		"occurs": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sTopOccurrencesOccurs", prefix, class.Class, property.Name),
			Description: descriptions.MetaPropertyTopOccurrencesOccurs,
			Type:        graphql.Int,
			Resolve:     common.JSONNumberResolver,
		},
	}

	getMetaMetaPointing := graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%sTopOccurrencesObj", prefix, class.Class, property.Name),
		Fields:      getMetaMetaPointingFields,
		Description: descriptions.MetaPropertyTopOccurrences,
	}

	return graphql.NewObject(getMetaMetaPointing)
}
