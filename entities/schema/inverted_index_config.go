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

package schema

import (
	"github.com/weaviate/weaviate/entities/models"
)

type InvertedIndexConfig struct {

	// bm25
	BM25 *BM25Config `json:"bm25,omitempty"`

	// Asynchronous index clean up happens every n seconds
	CleanupIntervalSeconds uint64 `json:"cleanupIntervalSeconds,omitempty"`

	// Index each object with the null state
	IndexNullState bool `json:"indexNullState,omitempty"`

	// Index length of properties
	IndexPropertyLength bool `json:"indexPropertyLength,omitempty"`

	// Index each object by its internal timestamps
	IndexTimestamps bool `json:"indexTimestamps,omitempty"`

	// stopwords
	Stopwords models.StopwordConfig `json:"stopwords,omitempty"`
}


type BM25Config struct {
	K1 float64
	B  float64
}

func InvertedIndexConfigFromModel(m models.InvertedIndexConfig) InvertedIndexConfig {
	i := InvertedIndexConfig{}

	if m.Bm25 != nil {
		i.BM25.K1 = float64(m.Bm25.K1)
		i.BM25.B = float64(m.Bm25.B)
	}
	if m.Stopwords != nil {
		i.Stopwords = *m.Stopwords
	}
	i.CleanupIntervalSeconds = uint64(m.CleanupIntervalSeconds)
	i.IndexTimestamps = m.IndexTimestamps
	i.IndexNullState = m.IndexNullState
	i.IndexPropertyLength = m.IndexPropertyLength

	return i
}

func InvertedIndexConfigToModel(i InvertedIndexConfig) models.InvertedIndexConfig {
	m := models.InvertedIndexConfig{}

	m.Bm25 = &models.BM25Config{}
	m.Bm25.K1 = float32(i.BM25.K1)
	m.Bm25.B = float32(i.BM25.B)

	m.Stopwords = &models.StopwordConfig{}
	// Force a copy to avoid references
	*m.Stopwords = i.Stopwords

	m.CleanupIntervalSeconds = int64(i.CleanupIntervalSeconds)
	m.IndexTimestamps = i.IndexTimestamps
	m.IndexNullState = i.IndexNullState
	m.IndexPropertyLength = i.IndexPropertyLength

	return m
}
