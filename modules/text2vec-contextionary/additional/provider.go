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

func (p *GraphQLAdditionalArgumentsProvider) AdditionalPropetiesFunctions() map[string]modulecapabilities.AdditionalPropertyFn {
	additionalPropertiesFns := map[string]modulecapabilities.AdditionalPropertyFn{}
	additionalPropertiesFns["nearestNeighbors"] = p.nnExtender.AdditionalPropertyFn
	additionalPropertiesFns["featureProjection"] = p.projector.AdditionalPropertyFn
	additionalPropertiesFns["semanticPath"] = p.sempathBuilder.AdditionalPropertyFn
	return additionalPropertiesFns
}
