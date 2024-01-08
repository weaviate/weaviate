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

package additional

import (
	"fmt"

	"github.com/tailor-inc/graphql"
)

func additionalFeatureProjectionField(classname string) *graphql.Field {
	return &graphql.Field{
		Args: graphql.FieldConfigArgument{
			"algorithm": &graphql.ArgumentConfig{
				Type:         graphql.String,
				DefaultValue: nil,
			},
			"dimensions": &graphql.ArgumentConfig{
				Type:         graphql.Int,
				DefaultValue: nil,
			},
			"learningRate": &graphql.ArgumentConfig{
				Type:         graphql.Int,
				DefaultValue: nil,
			},
			"iterations": &graphql.ArgumentConfig{
				Type:         graphql.Int,
				DefaultValue: nil,
			},
			"perplexity": &graphql.ArgumentConfig{
				Type:         graphql.Int,
				DefaultValue: nil,
			},
		},
		Type: graphql.NewObject(graphql.ObjectConfig{
			Name: fmt.Sprintf("%sAdditionalFeatureProjection", classname),
			Fields: graphql.Fields{
				"vector": &graphql.Field{Type: graphql.NewList(graphql.Float)},
			},
		}),
	}
}
