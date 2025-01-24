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

package t2vbigram

import (
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearText"
)

func (m *BigramModule) initNearText() error {
	m.GraphqlProvider = nearText.New(m.NearTextTransformer)
	return nil
}

func (m *BigramModule) Arguments() map[string]modulecapabilities.GraphQLArgument {
	return m.GraphqlProvider.Arguments()
}

func (m *BigramModule) VectorSearches() map[string]modulecapabilities.VectorForParams[[]float32] {
	vectorSearches := map[string]modulecapabilities.VectorForParams[[]float32]{}

	vectorSearches["nearText"] = &vectorForParams{m.VectorFromParams}
	return vectorSearches
}

var (
	_ = modulecapabilities.GraphQLArguments(New())
	_ = modulecapabilities.Searcher[[]float32](New())
)
