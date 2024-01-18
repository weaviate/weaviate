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

type ParamsHelper struct{}

func NewParamsHelper() *ParamsHelper {
	return &ParamsHelper{}
}

func (p *ParamsHelper) GetQuestion(params interface{}) string {
	if parameters, ok := params.(*AskParams); ok {
		return parameters.Question
	}
	return ""
}

func (p *ParamsHelper) GetProperties(params interface{}) []string {
	if parameters, ok := params.(*AskParams); ok {
		return parameters.Properties
	}
	return nil
}

func (p *ParamsHelper) GetCertainty(params interface{}) float64 {
	if parameters, ok := params.(*AskParams); ok {
		return parameters.Certainty
	}
	return 0
}

func (p *ParamsHelper) GetDistance(params interface{}) float64 {
	if parameters, ok := params.(*AskParams); ok {
		return parameters.Distance
	}
	return 0
}
