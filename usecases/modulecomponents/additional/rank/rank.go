//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package rank

import (
	"context"
	"errors"
	"fmt"

	"github.com/weaviate/weaviate/entities/models"

	"github.com/tailor-platform/graphql"
	"github.com/tailor-platform/graphql/language/ast"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

// const maximumNumberOfGoroutines = 10

// ReRankerClient ranks the values of a text property.
type ReRankerClient interface {
	Rank(ctx context.Context, query string, documents []string, cfg moduletools.ClassConfig) (*ent.RankResult, error)
}

// PropertyReRankerClient ranks the values of a text or blob property. It is
// given the name of the rerank property and, per document, the object ID and
// the property value. The property name lets the client read a blob property
// as an image. It returns one score per document, in the order of documents.
// An error about a document's value names the object's ID.
type PropertyReRankerClient interface {
	RankProperty(ctx context.Context, query, property string, documents []ent.Document, cfg moduletools.ClassConfig) (*ent.RankResult, error)
}

type textOnly struct{ ReRankerClient }

func (t textOnly) RankProperty(ctx context.Context, query, property string, documents []ent.Document, cfg moduletools.ClassConfig) (*ent.RankResult, error) {
	if cfg != nil && cfg.PropertiesDataTypes()[property] == schema.DataTypeBlob {
		return nil, fmt.Errorf("property %q is a blob property; this reranker ranks text properties", property)
	}
	values := make([]string, len(documents))
	for i := range documents {
		values[i] = documents[i].Value
	}
	return t.Rank(ctx, query, values, cfg)
}

type ReRankerProvider struct {
	client PropertyReRankerClient
}

// New is for a client that ranks text only.
func New(reranker ReRankerClient) *ReRankerProvider {
	return &ReRankerProvider{textOnly{reranker}}
}

// NewWithProperty is for a client that ranks text and blob properties.
func NewWithProperty(reranker PropertyReRankerClient) *ReRankerProvider {
	return &ReRankerProvider{reranker}
}

func (p *ReRankerProvider) AdditionalPropertyDefaultValue() interface{} {
	return &Params{}
}

func (p *ReRankerProvider) ExtractAdditionalFn(param []*ast.Argument, class *models.Class) interface{} {
	return p.parseReRankerArguments(param)
}

func (p *ReRankerProvider) AdditionalFieldFn(classname string) *graphql.Field {
	return p.additionalReRankerField(classname)
}

func (p *ReRankerProvider) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params interface{}, limit *int,
	argumentModuleParams map[string]interface{}, cfg moduletools.ClassConfig,
) ([]search.Result, error) {
	if parameters, ok := params.(*Params); ok {
		return p.getScore(ctx, cfg, in, parameters)
	}
	return nil, errors.New("wrong parameters")
}
