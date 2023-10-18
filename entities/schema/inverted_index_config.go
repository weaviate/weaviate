//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package schema

import (
	"github.com/weaviate/weaviate/entities/models"
)

type InvertedIndexConfig struct {
	BM25                   BM25Config
	Stopwords              models.StopwordConfig
	CleanupIntervalSeconds uint64
	IndexTimestamps        bool
	IndexNullState         bool
	IndexPropertyLength    bool
}

type BM25Config struct {
	K1 float64
	B  float64
}

func (i *InvertedIndexConfig) FromModel(m *models.InvertedIndexConfig) {
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
}

func (i *InvertedIndexConfig) ToModel() *models.InvertedIndexConfig {
	m := &models.InvertedIndexConfig{}

	if i.BM25.K1 != 0 || i.BM25.B != 0 {
		m.Bm25 = &models.BM25Config{}
		m.Bm25.K1 = float32(i.BM25.K1)
		m.Bm25.B = float32(i.BM25.B)
	}
	if len(i.Stopwords.Additions) > 0 || len(i.Stopwords.Removals) > 0 || len(i.Stopwords.Preset) > 0 {
		m.Stopwords = &models.StopwordConfig{}
		// Force a copy to avoid references
		*m.Stopwords = i.Stopwords
	}

	m.CleanupIntervalSeconds = int64(i.CleanupIntervalSeconds)
	m.IndexTimestamps = i.IndexTimestamps
	m.IndexNullState = i.IndexNullState
	m.IndexPropertyLength = i.IndexPropertyLength

	return m
}
