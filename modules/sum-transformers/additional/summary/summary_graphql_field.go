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

package summary

import (
	"fmt"

	"github.com/tailor-inc/graphql"
)

func (p *SummaryProvider) additionalSummaryField(classname string) *graphql.Field {
	return &graphql.Field{
		Args: graphql.FieldConfigArgument{
			"properties": &graphql.ArgumentConfig{
				Description:  "Properties which contains text",
				Type:         graphql.NewList(graphql.String),
				DefaultValue: nil,
			},
		},
		Type: graphql.NewList(graphql.NewObject(graphql.ObjectConfig{
			Name: fmt.Sprintf("%sAdditionalSummary", classname),
			Fields: graphql.Fields{
				"property": &graphql.Field{Type: graphql.String},
				"result":   &graphql.Field{Type: graphql.String},
			},
		})),
	}
}
