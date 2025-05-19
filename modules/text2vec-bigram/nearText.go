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
)

func (m *BigramModule) initNearText() error {
	return nil
}


var (
	_ = modulecapabilities.GraphQLArguments(New())
	_ = modulecapabilities.Searcher[[]float32](New())
)
