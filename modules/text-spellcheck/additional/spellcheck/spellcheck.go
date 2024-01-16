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

package spellcheck

import (
	"context"
	"errors"

	"github.com/tailor-inc/graphql"
	"github.com/tailor-inc/graphql/language/ast"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/modules/text-spellcheck/ent"
)

type Params struct{}

type spellCheckClient interface {
	Check(ctx context.Context, text []string) (*ent.SpellCheckResult, error)
}

type SpellCheckProvider struct {
	spellCheck  spellCheckClient
	paramHelper *paramHelper
}

func New(spellCheck spellCheckClient) *SpellCheckProvider {
	return &SpellCheckProvider{spellCheck, newParamHelper()}
}

func (p *SpellCheckProvider) AdditionalPropertyDefaultValue() interface{} {
	return &Params{}
}

func (p *SpellCheckProvider) ExtractAdditionalFn(param []*ast.Argument) interface{} {
	return &Params{}
}

func (p *SpellCheckProvider) AdditionalFieldFn(classname string) *graphql.Field {
	return p.additionalSpellCheckField(classname)
}

func (p *SpellCheckProvider) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params interface{}, limit *int,
	argumentModuleParams map[string]interface{}, cfg moduletools.ClassConfig,
) ([]search.Result, error) {
	if parameters, ok := params.(*Params); ok {
		return p.findSpellCheck(ctx, in, parameters, limit, argumentModuleParams)
	}
	return nil, errors.New("wrong parameters")
}
