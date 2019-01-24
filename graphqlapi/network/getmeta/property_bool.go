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

	"github.com/creativesoftwarefdn/weaviate/graphqlapi/descriptions"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/network/common"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/graphql-go/graphql"
)

func booleanPropertyFields(class *models.SemanticSchemaClass,
	property *models.SemanticSchemaClassProperty, prefix string) *graphql.Object {
	getMetaBooleanFields := graphql.Fields{
		"type": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sType", prefix, class.Class, property.Name),
			Description: descriptions.GetMetaPropertyTypeDesc,
			Type:        graphql.String,
		},
		"count": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sCount", prefix, class.Class, property.Name),
			Description: descriptions.GetMetaPropertyCountDesc,
			Type:        graphql.Int,
			Resolve:     common.JSONNumberResolver,
		},
		"totalTrue": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sTotalTrue", prefix, class.Class, property.Name),
			Description: descriptions.GetMetaClassPropertyTotalTrueDesc,
			Type:        graphql.Int,
			Resolve:     common.JSONNumberResolver,
		},
		"percentageTrue": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sPercentageTrue", prefix, class.Class, property.Name),
			Description: descriptions.GetMetaClassPropertyPercentageTrueDesc,
			Type:        graphql.Float,
			Resolve:     common.JSONNumberResolver,
		},
		"totalFalse": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sTotalFalse", prefix, class.Class, property.Name),
			Description: descriptions.GetMetaClassPropertyTotalFalseDesc,
			Type:        graphql.Int,
			Resolve:     common.JSONNumberResolver,
		},
		"percentageFalse": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sPercentageFalse", prefix, class.Class, property.Name),
			Description: descriptions.GetMetaClassPropertyPercentageFalseDesc,
			Type:        graphql.Float,
			Resolve:     common.JSONNumberResolver,
		},
	}

	return graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%sObj", prefix, class.Class, property.Name),
		Fields:      getMetaBooleanFields,
		Description: descriptions.GetMetaPropertyObjectDesc,
	})
}
