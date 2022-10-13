//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package modqna

import (
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/modules/qna-transformers/ask"
)

func (m *QnAModule) initAskSearcher() error {
	m.searcher = ask.NewSearcher(m.nearTextDependencies)
	return nil
}

func (m *QnAModule) initAskProvider() error {
	m.graphqlProvider = ask.New(m.askTextTransformer)
	return nil
}

func (m *QnAModule) Arguments() map[string]modulecapabilities.GraphQLArgument {
	return m.graphqlProvider.Arguments()
}

func (m *QnAModule) VectorSearches() modulecapabilities.ModuleArgumentVectorForParams {
	return m.searcher.VectorSearches()
}

var (
	_ = modulecapabilities.GraphQLArguments(New())
	_ = modulecapabilities.DependencySearcher(New())
)
