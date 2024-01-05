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

// import (
// 	"github.com/weaviate/weaviate/entities/modulecapabilities"
// 	"github.com/weaviate/weaviate/modules/multi2vec-clip/neartext"
// )

// func (m *ClipModule) initNearText() error {
// 	m.searcher = neartext.NewSearcher(m.vectorizer)
// 	m.graphqlProvider = neartext.New(m.nearTextTransformer)
// 	return nil
// }

// func (m *ClipModule) Arguments() map[string]modulecapabilities.GraphQLArgument {
// 	return m.graphqlProvider.Arguments()
// }

// func (m *ClipModule) VectorSearches() map[string]modulecapabilities.VectorForParams {
// 	return m.searcher.VectorSearches()
// }

// var (
// 	_ = modulecapabilities.GraphQLArguments(New())
// 	_ = modulecapabilities.Searcher(New())
// )
