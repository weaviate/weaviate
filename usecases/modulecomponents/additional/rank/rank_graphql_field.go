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

package rank

import (
	"fmt"

	"github.com/tailor-inc/graphql"
)

func (p *ReRankerProvider) additionalReRankerField(classname string) *graphql.Field {
	return &graphql.Field{
		Args: graphql.FieldConfigArgument{
			"query": &graphql.ArgumentConfig{
				Description:  "Properties which contains text",
				Type:         graphql.String,
				DefaultValue: nil,
			},
			"property": &graphql.ArgumentConfig{
				Description:  "Property to rank from",
				Type:         graphql.String,
				DefaultValue: nil,
			},
		},
		Type: graphql.NewList(graphql.NewObject(graphql.ObjectConfig{
			Name: fmt.Sprintf("%sAdditionalReranker", classname),
			Fields: graphql.Fields{
				"score": &graphql.Field{Type: graphql.Float},
			},
		})),
	}
}
