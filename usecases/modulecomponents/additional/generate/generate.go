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

package generate

import (
	"context"
	"errors"
	"fmt"

	"github.com/weaviate/weaviate/entities/models"

	"github.com/sirupsen/logrus"

	"github.com/tailor-inc/graphql"
	"github.com/tailor-inc/graphql/language/ast"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/search"
)

const maximumNumberOfGoroutines = 10

type GenerateProvider struct {
	additionalGenerativeParameters map[string]modulecapabilities.GenerativeProperty
	defaultProviderName            string
	maximumNumberOfGoroutines      int
	logger                         logrus.FieldLogger
}

func NewGeneric(
	additionalGenerativeParameters map[string]modulecapabilities.GenerativeProperty,
	defaultProviderName string,
	logger logrus.FieldLogger,
) *GenerateProvider {
	return &GenerateProvider{
		additionalGenerativeParameters: additionalGenerativeParameters,
		defaultProviderName:            defaultProviderName,
		maximumNumberOfGoroutines:      maximumNumberOfGoroutines,
		logger:                         logger,
	}
}

func (p *GenerateProvider) AdditionalPropertyDefaultValue() interface{} {
	return &Params{}
}

func (p *GenerateProvider) ExtractAdditionalFn(param []*ast.Argument, class *models.Class) interface{} {
	return p.parseGenerateArguments(param, class)
}

func (p *GenerateProvider) AdditionalFieldFn(classname string) *graphql.Field {
	return p.additionalGenerateField(classname)
}

func (p *GenerateProvider) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params interface{}, limit *int,
	argumentModuleParams map[string]interface{}, cfg moduletools.ClassConfig,
) ([]search.Result, error) {
	if parameters, ok := params.(*Params); ok {
		if len(parameters.Options) > 1 {
			var providerNames []string
			for name := range parameters.Options {
				providerNames = append(providerNames, name)
			}
			return nil, fmt.Errorf("multiple providers selected: %v, please choose only one", providerNames)
		}
		return p.generateResult(ctx, in, parameters, limit, argumentModuleParams, cfg)
	}
	return nil, errors.New("wrong parameters")
}
