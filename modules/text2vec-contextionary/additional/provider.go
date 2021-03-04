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

package additional

import (
	"context"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/search"
)

type AdditionalProperty interface {
	AdditionalPropertyFn(ctx context.Context,
		in []search.Result, params interface{}, limit *int) ([]search.Result, error)
	ExtractAdditionalFn(param []*ast.Argument) interface{}
	DefaultValueFn() interface{}
}

type GraphQLAdditionalArgumentsProvider struct {
	nnExtender     AdditionalProperty
	projector      AdditionalProperty
	sempathBuilder AdditionalProperty
}

func New(nnExtender, projector, sempath AdditionalProperty) *GraphQLAdditionalArgumentsProvider {
	return &GraphQLAdditionalArgumentsProvider{nnExtender, projector, sempath}
}

func (p *GraphQLAdditionalArgumentsProvider) GetAdditionalFields(classname string) map[string]*graphql.Field {
	additionalProperties := map[string]*graphql.Field{}
	additionalProperties["nearestNeighbors"] = additionalNearestNeighborsField(classname)
	additionalProperties["featureProjection"] = additionalFeatureProjectionField(classname)
	additionalProperties["semanticPath"] = additionalSemanticPathField(classname)
	return additionalProperties
}

func (p *GraphQLAdditionalArgumentsProvider) ExtractAdditionalFunctions() map[string]modulecapabilities.ExtractAdditionalFn {
	extractFns := map[string]modulecapabilities.ExtractAdditionalFn{}
	extractFns["nearestNeighbors"] = p.nnExtender.ExtractAdditionalFn
	extractFns["featureProjection"] = p.projector.ExtractAdditionalFn
	extractFns["semanticPath"] = p.sempathBuilder.ExtractAdditionalFn
	return extractFns
}

func (p *GraphQLAdditionalArgumentsProvider) AdditionalPropertiesFunctions() map[string]modulecapabilities.AdditionalPropertyFn {
	additionalPropertiesFns := map[string]modulecapabilities.AdditionalPropertyFn{}
	additionalPropertiesFns["nearestNeighbors"] = p.nnExtender.AdditionalPropertyFn
	additionalPropertiesFns["featureProjection"] = p.projector.AdditionalPropertyFn
	additionalPropertiesFns["semanticPath"] = p.sempathBuilder.AdditionalPropertyFn
	return additionalPropertiesFns
}

func (p *GraphQLAdditionalArgumentsProvider) RestApiAdditionalProperties() map[string][]string {
	restApiAdditionalProperties := map[string][]string{}
	restApiAdditionalProperties["nearestNeighbors"] = []string{
		"nearestNeighbors",
		"nearestneighbors",
		"nearest-neighbors",
		"nearest_neighbors",
	}
	restApiAdditionalProperties["featureProjection"] = []string{
		"featureProjection",
		"featureprojection",
		"feature-projection",
		"feature_projection",
	}
	return restApiAdditionalProperties
}

func (p *GraphQLAdditionalArgumentsProvider) GraphQLAdditionalProperties() map[string][]string {
	graphQLAdditionalProperties := map[string][]string{}
	graphQLAdditionalProperties["nearestNeighbors"] = []string{"nearestNeighbors"}
	graphQLAdditionalProperties["featureProjection"] = []string{"featureProjection"}
	graphQLAdditionalProperties["semanticPath"] = []string{"semanticPath"}
	return graphQLAdditionalProperties
}

func (p *GraphQLAdditionalArgumentsProvider) AdditionalPropertiesDefaultValues() map[string]modulecapabilities.DefaultValueFn {
	defaultValues := map[string]modulecapabilities.DefaultValueFn{}
	defaultValues["nearestNeighbors"] = p.nnExtender.DefaultValueFn
	defaultValues["featureProjection"] = p.projector.DefaultValueFn
	defaultValues["semanticPath"] = p.sempathBuilder.DefaultValueFn
	return defaultValues
}

func (p *GraphQLAdditionalArgumentsProvider) SearchAdditionalFunctions() map[string]modulecapabilities.AdditionalSearch {
	additionalPropertiesFns := map[string]modulecapabilities.AdditionalSearch{}
	additionalPropertiesFns["nearestNeighbors"] = modulecapabilities.AdditionalSearch{
		ObjectGet:   p.nnExtender.AdditionalPropertyFn,
		ObjectList:  p.nnExtender.AdditionalPropertyFn,
		ExploreGet:  p.nnExtender.AdditionalPropertyFn,
		ExploreList: p.nnExtender.AdditionalPropertyFn,
	}
	additionalPropertiesFns["featureProjection"] = modulecapabilities.AdditionalSearch{
		ObjectList:  p.projector.AdditionalPropertyFn,
		ExploreGet:  p.projector.AdditionalPropertyFn,
		ExploreList: p.projector.AdditionalPropertyFn,
	}
	additionalPropertiesFns["semanticPath"] = modulecapabilities.AdditionalSearch{
		ExploreGet: p.sempathBuilder.AdditionalPropertyFn,
	}
	return additionalPropertiesFns
}
