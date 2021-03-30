//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package answer

import (
	"fmt"

	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/descriptions"
)

func getAnswerArgumentFn(classname string) *graphql.ArgumentConfig {
	return answerArgument("GetObjects", classname)
}

func exploreAnswerArgumentFn() *graphql.ArgumentConfig {
	return answerArgument("Explore", "")
}

func answerArgument(prefix, className string) *graphql.ArgumentConfig {
	prefixName := fmt.Sprintf("QnATransformers%s%s", prefix, className)
	return &graphql.ArgumentConfig{
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:        fmt.Sprintf("%sAnswerInpObj", prefixName),
				Fields:      answerFields(prefixName),
				Description: descriptions.GetWhereInpObj,
			},
		),
	}
}

func answerFields(prefix string) graphql.InputObjectConfigFieldMap {
	return graphql.InputObjectConfigFieldMap{
		"question": &graphql.InputObjectFieldConfig{
			Description: "Question to be answered",
			Type:        graphql.NewNonNull(graphql.String),
		},
		"certainty": &graphql.InputObjectFieldConfig{
			Description: descriptions.Certainty,
			Type:        graphql.Float,
		},
	}
}
