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

package modtransformers

import (
	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/modules/text2vec-transformers/neartext"
)

func (m *TransformersModule) initNearText() error {
	m.searcher = neartext.NewSearcher(m.vectorizer)
	m.graphqlProvider = neartext.New()
	return nil
}

func (m *TransformersModule) GetArguments(classname string) map[string]*graphql.ArgumentConfig {
	return m.graphqlProvider.GetArguments(classname)
}

func (m *TransformersModule) ExploreArguments() map[string]*graphql.ArgumentConfig {
	return m.graphqlProvider.ExploreArguments()
}

func (m *TransformersModule) ExtractFunctions() map[string]modulecapabilities.ExtractFn {
	return m.graphqlProvider.ExtractFunctions()
}

func (m *TransformersModule) ValidateFunctions() map[string]modulecapabilities.ValidateFn {
	return m.graphqlProvider.ValidateFunctions()
}

func (m *TransformersModule) VectorSearches() map[string]modulecapabilities.VectorForParams {
	return m.searcher.VectorSearches()
}

var (
	_ = modulecapabilities.GraphQLArguments(New())
	_ = modulecapabilities.Searcher(New())
)
