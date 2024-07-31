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
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional/projector"
)

const PropertyFeatureProjection = "featureProjection"

type GraphQLAdditionalArgumentsProvider struct {
	projector *projector.FeatureProjector
}

func NewText2VecProvider() *GraphQLAdditionalArgumentsProvider {
	return &GraphQLAdditionalArgumentsProvider{projector.New()}
}

func (p *GraphQLAdditionalArgumentsProvider) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	additionalProperties := map[string]modulecapabilities.AdditionalProperty{}
	additionalProperties[PropertyFeatureProjection] = p.getFeatureProjection()
	return additionalProperties
}

func (p *GraphQLAdditionalArgumentsProvider) getFeatureProjection() modulecapabilities.AdditionalProperty {
	return modulecapabilities.AdditionalProperty{
		RestNames: []string{
			PropertyFeatureProjection,
			"featureprojection",
			"feature-projection",
			"feature_projection",
		},
		DefaultValue:           p.projector.AdditionalPropertyDefaultValue(),
		GraphQLNames:           []string{PropertyFeatureProjection},
		GraphQLFieldFunction:   p.projector.AdditionalFeatureProjectionField,
		GraphQLExtractFunction: p.projector.ExtractAdditionalFn,
		SearchFunctions: modulecapabilities.AdditionalSearch{
			ObjectList:  p.projector.AdditionalPropertyFn,
			ExploreGet:  p.projector.AdditionalPropertyFn,
			ExploreList: p.projector.AdditionalPropertyFn,
		},
	}
}
