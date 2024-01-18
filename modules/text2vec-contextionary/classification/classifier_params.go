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

package classification

type ParamsContextual struct {
	MinimumUsableWords              *int32 `json:"minimumUsableWords"`
	InformationGainCutoffPercentile *int32 `json:"informationGainCutoffPercentile"`
	InformationGainMaximumBoost     *int32 `json:"informationGainMaximumBoost"`
	TfidfCutoffPercentile           *int32 `json:"tfidfCutoffPercentile"`
}

func (params *ParamsContextual) SetDefaults() {
	if params.MinimumUsableWords == nil {
		defaultParam := int32(3)
		params.MinimumUsableWords = &defaultParam
	}

	if params.InformationGainCutoffPercentile == nil {
		defaultParam := int32(50)
		params.InformationGainCutoffPercentile = &defaultParam
	}

	if params.InformationGainMaximumBoost == nil {
		defaultParam := int32(3)
		params.InformationGainMaximumBoost = &defaultParam
	}

	if params.TfidfCutoffPercentile == nil {
		defaultParam := int32(80)
		params.TfidfCutoffPercentile = &defaultParam
	}
}
