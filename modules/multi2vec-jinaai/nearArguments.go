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

package modclip

import (
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearImage"
	"github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearText"
)

func (m *Module) initNearImage() error {
	m.nearImageSearcher = nearImage.NewSearcher(m.vectorizer)
	m.nearImageGraphqlProvider = nearImage.New()
	return nil
}

func (m *Module) initNearText() error {
	m.nearTextSearcher = nearText.NewSearcher(m.vectorizer)
	m.nearTextGraphqlProvider = nearText.New(m.nearTextTransformer)
	return nil
}

func (m *Module) Arguments() map[string]modulecapabilities.GraphQLArgument {
	arguments := map[string]modulecapabilities.GraphQLArgument{}
	for name, arg := range m.nearImageGraphqlProvider.Arguments() {
		arguments[name] = arg
	}
	for name, arg := range m.nearTextGraphqlProvider.Arguments() {
		arguments[name] = arg
	}
	return arguments
}

func (m *Module) VectorSearches() map[string]modulecapabilities.VectorForParams {
	vectorSearches := map[string]modulecapabilities.VectorForParams{}
	for name, arg := range m.nearImageSearcher.VectorSearches() {
		vectorSearches[name] = arg
	}
	for name, arg := range m.nearTextSearcher.VectorSearches() {
		vectorSearches[name] = arg
	}
	return vectorSearches
}

var (
	_ = modulecapabilities.GraphQLArguments(New())
	_ = modulecapabilities.Searcher(New())
)
