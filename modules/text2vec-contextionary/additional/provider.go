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
	"context"

	"github.com/weaviate/weaviate/entities/moduletools"

	"github.com/tailor-inc/graphql/language/ast"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/search"
)

type AdditionalProperty interface {
	AdditionalPropertyFn(ctx context.Context,
		in []search.Result, params interface{}, limit *int,
		argumentModuleParams map[string]interface{}, cfg moduletools.ClassConfig) ([]search.Result, error)
	ExtractAdditionalFn(param []*ast.Argument) interface{}
	AdditionalPropertyDefaultValue() interface{}
}

type GraphQLAdditionalArgumentsProvider struct {
	nnExtender     AdditionalProperty
	projector      AdditionalProperty
	sempathBuilder AdditionalProperty
	interpretation AdditionalProperty
}

func New(nnExtender, projector, sempath, interpretation AdditionalProperty) *GraphQLAdditionalArgumentsProvider {
	return &GraphQLAdditionalArgumentsProvider{nnExtender, projector, sempath, interpretation}
}

func (p *GraphQLAdditionalArgumentsProvider) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	additionalProperties := map[string]modulecapabilities.AdditionalProperty{}
	additionalProperties["nearestNeighbors"] = p.getNearestNeighbors()
	additionalProperties["featureProjection"] = p.getFeatureProjection()
	additionalProperties["semanticPath"] = p.getSemanticPath()
	additionalProperties["interpretation"] = p.getInterpretation()
	return additionalProperties
}

func (p *GraphQLAdditionalArgumentsProvider) getNearestNeighbors() modulecapabilities.AdditionalProperty {
	return modulecapabilities.AdditionalProperty{
		RestNames: []string{
			"nearestNeighbors",
			"nearestneighbors",
			"nearest-neighbors",
			"nearest_neighbors",
		},
		DefaultValue:           p.nnExtender.AdditionalPropertyDefaultValue(),
		GraphQLNames:           []string{"nearestNeighbors"},
		GraphQLFieldFunction:   additionalNearestNeighborsField,
		GraphQLExtractFunction: p.nnExtender.ExtractAdditionalFn,
		SearchFunctions: modulecapabilities.AdditionalSearch{
			ObjectGet:   p.nnExtender.AdditionalPropertyFn,
			ObjectList:  p.nnExtender.AdditionalPropertyFn,
			ExploreGet:  p.nnExtender.AdditionalPropertyFn,
			ExploreList: p.nnExtender.AdditionalPropertyFn,
		},
	}
}

func (p *GraphQLAdditionalArgumentsProvider) getFeatureProjection() modulecapabilities.AdditionalProperty {
	return modulecapabilities.AdditionalProperty{
		RestNames: []string{
			"featureProjection",
			"featureprojection",
			"feature-projection",
			"feature_projection",
		},
		DefaultValue:           p.projector.AdditionalPropertyDefaultValue(),
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

func (p *GraphQLAdditionalArgumentsProvider) getSemanticPath() modulecapabilities.AdditionalProperty {
	return modulecapabilities.AdditionalProperty{
		DefaultValue:           p.sempathBuilder.AdditionalPropertyDefaultValue(),
		GraphQLNames:           []string{"semanticPath"},
		GraphQLFieldFunction:   additionalSemanticPathField,
		GraphQLExtractFunction: p.sempathBuilder.ExtractAdditionalFn,
		SearchFunctions: modulecapabilities.AdditionalSearch{
			ExploreGet: p.sempathBuilder.AdditionalPropertyFn,
		},
	}
}

func (p *GraphQLAdditionalArgumentsProvider) getInterpretation() modulecapabilities.AdditionalProperty {
	return modulecapabilities.AdditionalProperty{
		RestNames: []string{
			"interpretation",
		},
		DefaultValue:           p.interpretation.AdditionalPropertyDefaultValue(),
		GraphQLNames:           []string{"interpretation"},
		GraphQLFieldFunction:   additionalInterpretationField,
		GraphQLExtractFunction: p.interpretation.ExtractAdditionalFn,
		SearchFunctions: modulecapabilities.AdditionalSearch{
			ObjectGet:   p.interpretation.AdditionalPropertyFn,
			ObjectList:  p.interpretation.AdditionalPropertyFn,
			ExploreGet:  p.interpretation.AdditionalPropertyFn,
			ExploreList: p.interpretation.AdditionalPropertyFn,
		},
	}
}
