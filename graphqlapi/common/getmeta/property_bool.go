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

	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/common"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/descriptions"
	"github.com/graphql-go/graphql"
)

func booleanPropertyFields(class *models.SemanticSchemaClass,
	property *models.SemanticSchemaClassProperty, prefix string) *graphql.Object {
	getMetaBooleanFields := graphql.Fields{
		"type": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sType", prefix, class.Class, property.Name),
			Description: descriptions.GetMetaPropertyType,
			Type:        graphql.String,
		},
		"count": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sCount", prefix, class.Class, property.Name),
			Description: descriptions.GetMetaPropertyCount,
			Type:        graphql.Int,
			Resolve:     common.JSONNumberResolver,
		},
		"totalTrue": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sTotalTrue", prefix, class.Class, property.Name),
			Description: descriptions.GetMetaClassPropertyTotalTrue,
			Type:        graphql.Int,
			Resolve:     common.JSONNumberResolver,
		},
		"percentageTrue": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sPercentageTrue", prefix, class.Class, property.Name),
			Description: descriptions.GetMetaClassPropertyPercentageTrue,
			Type:        graphql.Float,
			Resolve:     common.JSONNumberResolver,
		},
		"totalFalse": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sTotalFalse", prefix, class.Class, property.Name),
			Description: descriptions.GetMetaClassPropertyTotalFalse,
			Type:        graphql.Int,
			Resolve:     common.JSONNumberResolver,
		},
		"percentageFalse": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sPercentageFalse", prefix, class.Class, property.Name),
			Description: descriptions.GetMetaClassPropertyPercentageFalse,
			Type:        graphql.Float,
			Resolve:     common.JSONNumberResolver,
		},
	}

	return graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%sObj", prefix, class.Class, property.Name),
		Fields:      getMetaBooleanFields,
		Description: descriptions.GetMetaPropertyObject,
	})
}
