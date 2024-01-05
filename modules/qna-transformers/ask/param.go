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

package ask

import (
	"github.com/pkg/errors"
)

type AskParams struct {
	Question     string
	Certainty    float64
	Distance     float64
	WithDistance bool
	Properties   []string
	Autocorrect  bool
	Rerank       bool
}

func (n AskParams) GetCertainty() float64 {
	return n.Certainty
}

func (n AskParams) GetDistance() float64 {
	return n.Distance
}

func (n AskParams) SimilarityMetricProvided() bool {
	return n.Certainty != 0 || n.WithDistance
}

func (g *GraphQLArgumentsProvider) validateAskFn(param interface{}) error {
	ask, ok := param.(*AskParams)
	if !ok {
		return errors.New("'ask' invalid parameter")
	}

	if len(ask.Question) == 0 {
		return errors.Errorf("'ask.question' needs to be defined")
	}

	if ask.Certainty != 0 && ask.WithDistance {
		return errors.Errorf(
			"nearText cannot provide both distance and certainty")
	}

	return nil
}
