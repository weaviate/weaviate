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

func additionalNearestNeighborsField(classname string) *graphql.Field {
	return &graphql.Field{
		Type: graphql.NewObject(graphql.ObjectConfig{
			Name: fmt.Sprintf("%sAdditionalNearestNeighbors", classname),
			Fields: graphql.Fields{
				"neighbors": &graphql.Field{Type: graphql.NewList(graphql.NewObject(graphql.ObjectConfig{
					Name: fmt.Sprintf("%sAdditionalNearestNeighborsNeighbor", classname),
					Fields: graphql.Fields{
						"concept":  &graphql.Field{Type: graphql.String},
						"distance": &graphql.Field{Type: graphql.Float},
					},
				}))},
			},
		}),
	}
}

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

func additionalSemanticPathField(classname string) *graphql.Field {
	return &graphql.Field{
		Type: graphql.NewObject(graphql.ObjectConfig{
			Name: fmt.Sprintf("%sAdditionalSemanticPath", classname),
			Fields: graphql.Fields{
				"path": &graphql.Field{Type: graphql.NewList(graphql.NewObject(graphql.ObjectConfig{
					Name: fmt.Sprintf("%sAdditionalSemanticPathElement", classname),
					Fields: graphql.Fields{
						"concept":            &graphql.Field{Type: graphql.String},
						"distanceToQuery":    &graphql.Field{Type: graphql.Float},
						"distanceToResult":   &graphql.Field{Type: graphql.Float},
						"distanceToNext":     &graphql.Field{Type: graphql.Float},
						"distanceToPrevious": &graphql.Field{Type: graphql.Float},
					},
				}))},
			},
		}),
	}
}

func additionalInterpretationField(classname string) *graphql.Field {
	return &graphql.Field{
		Type: graphql.NewObject(graphql.ObjectConfig{
			Name: fmt.Sprintf("%sAdditionalInterpretation", classname),
			Fields: graphql.Fields{
				"source": &graphql.Field{Type: graphql.NewList(graphql.NewObject(graphql.ObjectConfig{
					Name: fmt.Sprintf("%sAdditionalInterpretationSource", classname),
					Fields: graphql.Fields{
						"concept":    &graphql.Field{Type: graphql.String},
						"weight":     &graphql.Field{Type: graphql.Float},
						"occurrence": &graphql.Field{Type: graphql.Int},
					},
				}))},
			},
		}),
	}
}
