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

package modaws

import (
	"github.com/liutizhong/weaviate/entities/modulecapabilities"
	"github.com/liutizhong/weaviate/usecases/modulecomponents/arguments/nearText"
)

func (m *AwsModule) initNearText() error {
	m.searcher = nearText.NewSearcher(m.vectorizer)
	m.graphqlProvider = nearText.New(m.nearTextTransformer)
	return nil
}

func (m *AwsModule) Arguments() map[string]modulecapabilities.GraphQLArgument {
	return m.graphqlProvider.Arguments()
}

func (m *AwsModule) VectorSearches() map[string]modulecapabilities.VectorForParams[[]float32] {
	return m.searcher.VectorSearches()
}

var (
	_ = modulecapabilities.GraphQLArguments(New())
	_ = modulecapabilities.Searcher[[]float32](New())
)
