//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package ask

import (
	"github.com/pkg/errors"
)

type AskParams struct {
	Question    string
	Certainty   float64
	Distance    float64
	Properties  []string
	Autocorrect bool
	Rerank      bool
}

func (n AskParams) GetCertainty() float64 {
	return n.Certainty
}

func (n AskParams) GetDistance() float64 {
	return n.Distance
}

func (n AskParams) SimilarityMetricProvided() bool {
	return n.Certainty != 0 || n.Distance != 0
}

func (g *GraphQLArgumentsProvider) validateAskFn(param interface{}) error {
	ask, ok := param.(*AskParams)
	if !ok {
		return errors.New("'ask' invalid parameter")
	}

	if len(ask.Question) == 0 {
		return errors.Errorf("'ask.question' needs to be defined")
	}

	if ask.Certainty != 0 && ask.Distance != 0 {
		return errors.Errorf(
			"nearText cannot provide both distance and certainty")
	}

	// because the modules all still accept certainty as
	// the only similarity metric input, me must make the
	// conversion to certainty if distance is provided
	if ask.Distance != 0 {
		ask.Certainty = 1 - ask.Distance
	}

	return nil
}
