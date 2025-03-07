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

package modulecapabilities

import (
	"context"

	"github.com/tailor-inc/graphql"
	"github.com/tailor-inc/graphql/language/ast"
	"github.com/weaviate/weaviate/entities/moduletools"
)

// GraphQLFieldFn generates graphql input fields
type GraphQLInputFieldFn = func(classname string) *graphql.InputObjectFieldConfig

// ExtractRequestParamsFn extracts specific generative API parameters from graphql queries
type ExtractRequestParamsFn = func(field *ast.ObjectField) interface{}

// GenerateDebugInformation exposes debug information
type GenerateDebugInformation struct {
	Prompt string
}

// GenerateResponse defines generative response. Params files hold module specific
// response parameters
type GenerateResponse struct {
	Result *string
	Params map[string]interface{}
	Debug  *GenerateDebugInformation
}

// GenerateProperties defines the properties to be supplied as part of the generative request.
// They must be differentiated at this point due to the different ways third-parties handle them.
type GenerateProperties struct {
	Text map[string]string
	Blob map[string]*string
}

// GenerativeClient defines generative client
type GenerativeClient interface {
	GenerateSingleResult(ctx context.Context,
		properties *GenerateProperties, prompt string, requestParams interface{}, debug bool, cfg moduletools.ClassConfig,
	) (*GenerateResponse, error)
	GenerateAllResults(ctx context.Context,
		properties []*GenerateProperties, task string, requestParams interface{}, debug bool, cfg moduletools.ClassConfig,
	) (*GenerateResponse, error)
}

// GenerativeProperty defines all needed additional request / response parameters
// only client setting is manadatory as we can have generative modules
// that don't expose any additional request / response params.
type GenerativeProperty struct {
	Client                       GenerativeClient
	RequestParamsFunction        GraphQLInputFieldFn
	ResponseParamsFunction       GraphQLFieldFn
	ExtractRequestParamsFunction ExtractRequestParamsFn
}

// AdditionalGenerativeProperties groups whole interface methods needed
// for adding the capability of additional generative properties
type AdditionalGenerativeProperties interface {
	AdditionalGenerativeProperties() map[string]GenerativeProperty
}
