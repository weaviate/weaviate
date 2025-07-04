//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package modclip

import (
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearImage"
	"github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearText"
)

func (m *ClipModule) initNearImage() error {
	m.nearImageSearcher = nearImage.NewSearcher(m.imageVectorizer)
	m.nearImageGraphqlProvider = nearImage.New()
	return nil
}

func (m *ClipModule) initNearText() error {
	m.nearTextSearcher = nearText.NewSearcher(m.textVectorizer)
	m.nearTextGraphqlProvider = nearText.New(m.nearTextTransformer)
	return nil
}

func (m *ClipModule) Arguments() map[string]modulecapabilities.GraphQLArgument {
	arguments := map[string]modulecapabilities.GraphQLArgument{}
	for name, arg := range m.nearImageGraphqlProvider.Arguments() {
		arguments[name] = arg
	}
	for name, arg := range m.nearTextGraphqlProvider.Arguments() {
		arguments[name] = arg
	}
	return arguments
}

func (m *ClipModule) VectorSearches() map[string]modulecapabilities.VectorForParams[[]float32] {
	vectorSearches := map[string]modulecapabilities.VectorForParams[[]float32]{}
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
	_ = modulecapabilities.Searcher[[]float32](New())
)
