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

package ask

import (
	"github.com/pkg/errors"
)

type AskParams struct {
	Question    string
	Certainty   float64
	Properties  []string
	Autocorrect bool
}

func (n AskParams) GetCertainty() float64 {
	return n.Certainty
}

func (g *GraphQLArgumentsProvider) validateAskFn(param interface{}) error {
	ask, ok := param.(*AskParams)
	if !ok {
		return errors.New("'ask' invalid parameter")
	}

	if len(ask.Question) == 0 {
		return errors.Errorf("'ask.question' needs to be defined")
	}

	return nil
}
