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

package clients

import (
	"errors"
	"fmt"
)

type bertEmbeddingsDecoder struct{}

func newBertEmbeddingsDecoder() *bertEmbeddingsDecoder {
	return &bertEmbeddingsDecoder{}
}

func (d bertEmbeddingsDecoder) calculateVector(embeddings [][]float32) ([]float32, error) {
	if len(embeddings) == 0 {
		return nil, errors.New("missing embeddings")
	}
	vectorLen := len(embeddings[0])
	if vectorLen == 0 {
		return nil, errors.New("token vector 0 has no dimensions")
	}
	for j := range embeddings {
		if len(embeddings[j]) != vectorLen {
			return nil, fmt.Errorf("token vector %d has %d dimensions, expected %d", j, len(embeddings[j]), vectorLen)
		}
	}
	sumEmbeddings := make([]float32, vectorLen)
	embeddingsLen := len(embeddings)
	var sum float32
	for i := 0; i < vectorLen; i++ {
		sum = 0
		for j := 0; j < embeddingsLen; j++ {
			sum += embeddings[j][i]
		}
		sumEmbeddings[i] = sum
	}
	for i := range sumEmbeddings {
		sumEmbeddings[i] = sumEmbeddings[i] / float32(embeddingsLen)
	}
	return sumEmbeddings, nil
}
