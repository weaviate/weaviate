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
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/modulecapabilities"
	generativegenerate "github.com/weaviate/weaviate/usecases/modulecomponents/additional/generate"
)

const PropertyGenerate = "generate"

type GraphQLAdditionalGenerativeProvider struct {
	generative AdditionalProperty
}

func NewGenericGenerativeProvider(
	className string,
	additionalGenerativeParameters map[string]modulecapabilities.GenerativeProperty,
	defaultProviderName string,
	logger logrus.FieldLogger,
) *GraphQLAdditionalGenerativeProvider {
	return &GraphQLAdditionalGenerativeProvider{generativegenerate.NewGeneric(additionalGenerativeParameters, defaultProviderName, logger)}
}

func (p *GraphQLAdditionalGenerativeProvider) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	additionalProperties := map[string]modulecapabilities.AdditionalProperty{}
	additionalProperties[PropertyGenerate] = p.getGenerate()
	return additionalProperties
}

func (p *GraphQLAdditionalGenerativeProvider) getGenerate() modulecapabilities.AdditionalProperty {
	return modulecapabilities.AdditionalProperty{
		GraphQLNames:           []string{PropertyGenerate},
		GraphQLFieldFunction:   p.generative.AdditionalFieldFn,
		GraphQLExtractFunction: p.generative.ExtractAdditionalFn,
		SearchFunctions: modulecapabilities.AdditionalSearch{
			ExploreGet:  p.generative.AdditionalPropertyFn,
			ExploreList: p.generative.AdditionalPropertyFn,
		},
	}
}
