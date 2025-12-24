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
	"maps"

	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearImage"
	"github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearText"
	"github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearVideo"
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

func (m *Module) initNearVideo() error {
	m.nearVideoSearcher = nearVideo.NewSearcher(m.vectorizer)
	m.nearVideoGraphqlProvider = nearVideo.New()
	return nil
}

func (m *Module) Arguments() map[string]modulecapabilities.GraphQLArgument {
	arguments := map[string]modulecapabilities.GraphQLArgument{}
	maps.Copy(arguments, m.nearTextGraphqlProvider.Arguments())
	maps.Copy(arguments, m.nearImageGraphqlProvider.Arguments())
	maps.Copy(arguments, m.nearVideoGraphqlProvider.Arguments())
	return arguments
}

func (m *Module) VectorSearches() map[string]modulecapabilities.VectorForParams[[]float32] {
	vectorSearches := map[string]modulecapabilities.VectorForParams[[]float32]{}
	maps.Copy(vectorSearches, m.nearTextSearcher.VectorSearches())
	maps.Copy(vectorSearches, m.nearImageSearcher.VectorSearches())
	maps.Copy(vectorSearches, m.nearVideoSearcher.VectorSearches())
	return vectorSearches
}

var (
	_ = modulecapabilities.GraphQLArguments(New())
	_ = modulecapabilities.Searcher[[]float32](New())
)
