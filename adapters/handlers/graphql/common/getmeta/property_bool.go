//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
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

func booleanPropertyFields(class *models.Class,
	property *models.Property, prefix string) *graphql.Object {
	getMetaBooleanFields := graphql.Fields{
		"type": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sType", prefix, class.Class, property.Name),
			Description: descriptions.MetaPropertyType,
			Type:        graphql.String,
		},
		"count": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sCount", prefix, class.Class, property.Name),
			Description: descriptions.MetaPropertyCount,
			Type:        graphql.Int,
			Resolve:     common.JSONNumberResolver,
		},
		"totalTrue": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sTotalTrue", prefix, class.Class, property.Name),
			Description: descriptions.MetaClassPropertyTotalTrue,
			Type:        graphql.Int,
			Resolve:     common.JSONNumberResolver,
		},
		"percentageTrue": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sPercentageTrue", prefix, class.Class, property.Name),
			Description: descriptions.MetaClassPropertyPercentageTrue,
			Type:        graphql.Float,
			Resolve:     common.JSONNumberResolver,
		},
		"totalFalse": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sTotalFalse", prefix, class.Class, property.Name),
			Description: descriptions.MetaClassPropertyTotalFalse,
			Type:        graphql.Int,
			Resolve:     common.JSONNumberResolver,
		},
		"percentageFalse": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sPercentageFalse", prefix, class.Class, property.Name),
			Description: descriptions.MetaClassPropertyPercentageFalse,
			Type:        graphql.Float,
			Resolve:     common.JSONNumberResolver,
		},
	}

	return graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%sObj", prefix, class.Class, property.Name),
		Fields:      getMetaBooleanFields,
		Description: descriptions.MetaPropertyObject,
	})
}
