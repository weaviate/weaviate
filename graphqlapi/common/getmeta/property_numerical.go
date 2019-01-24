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
package getmeta

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/graphqlapi/common"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/descriptions"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/graphql-go/graphql"
)

func numericalPropertyField(class *models.SemanticSchemaClass,
	property *models.SemanticSchemaClassProperty, prefix string) *graphql.Object {
	getMetaNumberFields := graphql.Fields{
		"sum": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sSum", prefix, class.Class, property.Name),
			Description: descriptions.GetMetaPropertySumDesc,
			Type:        graphql.Float,
			Resolve:     common.JSONNumberResolver,
		},
		"type": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sType", prefix, class.Class, property.Name),
			Description: descriptions.GetMetaPropertyTypeDesc,
			Type:        graphql.String,
		},
		"lowest": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sLowest", prefix, class.Class, property.Name),
			Description: descriptions.GetMetaPropertyLowestDesc,
			Type:        graphql.Float,
			Resolve:     common.JSONNumberResolver,
		},
		"highest": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sHighest", prefix, class.Class, property.Name),
			Description: descriptions.GetMetaPropertyHighestDesc,
			Type:        graphql.Float,
			Resolve:     common.JSONNumberResolver,
		},
		"average": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sAverage", prefix, class.Class, property.Name),
			Description: descriptions.GetMetaPropertyAverageDesc,
			Type:        graphql.Float,
			Resolve:     common.JSONNumberResolver,
		},
		"count": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sCount", prefix, class.Class, property.Name),
			Description: descriptions.GetMetaPropertyCountDesc,
			Type:        graphql.Int,
			Resolve:     common.JSONNumberResolver,
		},
	}

	return graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%sObj", prefix, class.Class, property.Name),
		Fields:      getMetaNumberFields,
		Description: descriptions.GetMetaPropertyObjectDesc,
	})
}
