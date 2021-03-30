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
	"github.com/pkg/errors"
)

type AnswerParams struct {
	Question  string
	Certainty float64
	Limit     int
}

func (n AnswerParams) GetCertainty() float64 {
	return n.Certainty
}

func validateAnswerFn(param interface{}) error {
	answer, ok := param.(*AnswerParams)
	if !ok {
		return errors.New("'answer' invalid parameter")
	}

	if len(answer.Question) == 0 {
		return errors.Errorf("'answer.question' needs to be defined")
	}

	return nil
}
