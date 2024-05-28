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

package modoctoai

import (
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearText"
)

func (m *OctoAIModule) initNearText() error {
	m.searcher = nearText.NewSearcher(m.vectorizer)
	m.graphqlProvider = nearText.New(m.nearTextTransformer)
	return nil
}

func (m *OctoAIModule) Arguments() map[string]modulecapabilities.GraphQLArgument {
	return m.graphqlProvider.Arguments()
}

func (m *OctoAIModule) VectorSearches() map[string]modulecapabilities.VectorForParams {
	return m.searcher.VectorSearches()
}

var (
	_ = modulecapabilities.GraphQLArguments(New())
	_ = modulecapabilities.Searcher(New())
)
