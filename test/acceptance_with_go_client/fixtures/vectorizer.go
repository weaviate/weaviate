//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package fixtures

import "github.com/weaviate/weaviate/entities/models"

const (
	DefaultVectorName = "default"
	Text2VecModel2Vec = "text2vec-model2vec"
)

// DefaultVectorConfig returns a named vector configuration holding a single
// "default" vector that is vectorized by text2vec-model2vec.
func DefaultVectorConfig() map[string]models.VectorConfig {
	return DefaultVectorConfigWithSettings(map[string]any{})
}

// DefaultVectorConfigWithSettings is like DefaultVectorConfig, but passes the
// given settings (e.g. "properties") to the text2vec-model2vec module.
func DefaultVectorConfigWithSettings(settings map[string]any) map[string]models.VectorConfig {
	return map[string]models.VectorConfig{
		DefaultVectorName: {
			Vectorizer:      map[string]any{Text2VecModel2Vec: settings},
			VectorIndexType: "hnsw",
		},
	}
}
