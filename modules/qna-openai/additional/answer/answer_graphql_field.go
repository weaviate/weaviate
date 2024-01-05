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

package answer

import (
	"fmt"

	"github.com/tailor-inc/graphql"
)

func (p *AnswerProvider) additionalAnswerField(classname string) *graphql.Field {
	return &graphql.Field{
		Type: graphql.NewObject(graphql.ObjectConfig{
			Name: fmt.Sprintf("%sAdditionalAnswer", classname),
			Fields: graphql.Fields{
				"result":        &graphql.Field{Type: graphql.String},
				"startPosition": &graphql.Field{Type: graphql.Int},
				"endPosition":   &graphql.Field{Type: graphql.Int},
				"property":      &graphql.Field{Type: graphql.String},
				"hasAnswer":     &graphql.Field{Type: graphql.Boolean},
			},
		}),
	}
}
