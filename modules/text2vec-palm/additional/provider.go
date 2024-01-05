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
	"github.com/weaviate/weaviate/modules/text2vec-palm/additional/projector"
)

type GraphQLAdditionalArgumentsProvider struct {
	projector *projector.FeatureProjector
}

func New(projector *projector.FeatureProjector) *GraphQLAdditionalArgumentsProvider {
	return &GraphQLAdditionalArgumentsProvider{projector}
}

func (p *GraphQLAdditionalArgumentsProvider) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	additionalProperties := map[string]modulecapabilities.AdditionalProperty{}
	additionalProperties["featureProjection"] = p.getFeatureProjection()
	return additionalProperties
}

func (p *GraphQLAdditionalArgumentsProvider) getFeatureProjection() modulecapabilities.AdditionalProperty {
	return modulecapabilities.AdditionalProperty{
		RestNames: []string{
			"featureProjection",
			"featureprojection",
			"feature-projection",
			"feature_projection",
		},
		DefaultValue:           p.projector.AdditonalPropertyDefaultValue(),
		GraphQLNames:           []string{"featureProjection"},
		GraphQLFieldFunction:   additionalFeatureProjectionField,
		GraphQLExtractFunction: p.projector.ExtractAdditionalFn,
		SearchFunctions: modulecapabilities.AdditionalSearch{
			ObjectList:  p.projector.AdditionalPropertyFn,
			ExploreGet:  p.projector.AdditionalPropertyFn,
			ExploreList: p.projector.AdditionalPropertyFn,
		},
	}
}
