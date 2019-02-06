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
			Description: descriptions.GetMetaPropertySum,
			Type:        graphql.Float,
			Resolve:     common.JSONNumberResolver,
		},
		"type": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sType", prefix, class.Class, property.Name),
			Description: descriptions.GetMetaPropertyType,
			Type:        graphql.String,
		},
		"minimum": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sMinimum", prefix, class.Class, property.Name),
			Description: descriptions.GetMetaPropertyMinimum,
			Type:        graphql.Float,
			Resolve:     common.JSONNumberResolver,
		},
		"maximum": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sMaximum", prefix, class.Class, property.Name),
			Description: descriptions.GetMetaPropertyMaximum,
			Type:        graphql.Float,
			Resolve:     common.JSONNumberResolver,
		},
		"mean": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sMean", prefix, class.Class, property.Name),
			Description: descriptions.GetMetaPropertyMean,
			Type:        graphql.Float,
			Resolve:     common.JSONNumberResolver,
		},
		"count": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sCount", prefix, class.Class, property.Name),
			Description: descriptions.GetMetaPropertyCount,
			Type:        graphql.Int,
			Resolve:     common.JSONNumberResolver,
		},
	}

	return graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%sObj", prefix, class.Class, property.Name),
		Fields:      getMetaNumberFields,
		Description: descriptions.GetMetaPropertyObject,
	})
}
