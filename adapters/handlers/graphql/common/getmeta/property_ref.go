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

	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/common"
	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/graphql-go/graphql"
)

func refPropertyObj(class *models.SemanticSchemaClass,
	property *models.SemanticSchemaClassProperty, prefix string) *graphql.Object {
	fields := graphql.Fields{
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
		"pointingTo": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sPointingTo", prefix, class.Class, property.Name),
			Description: descriptions.GetMetaClassPropertyPointingTo,
			Type:        graphql.NewList(graphql.String),
		},
	}

	return graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%sObj", prefix, class.Class, property.Name),
		Fields:      fields,
		Description: descriptions.GetMetaPropertyObject,
	})
}
