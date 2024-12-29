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

package configvalidation

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	vectorIndex "github.com/weaviate/weaviate/entities/vectorindex/common"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestCertainty(t *testing.T) {
	cases := []struct {
		name          string
		targetVectors []string
		class         *models.Class
		fail          bool
	}{
		{name: "no vectorizer", targetVectors: nil, class: &models.Class{}, fail: true},
		{name: "cosine", targetVectors: nil, class: &models.Class{VectorIndexConfig: hnsw.UserConfig{Distance: vectorIndex.DistanceCosine}}, fail: false},
		{name: "dot", targetVectors: nil, class: &models.Class{VectorIndexConfig: hnsw.UserConfig{Distance: vectorIndex.DistanceDot}}, fail: true},
		{name: "single target vector", targetVectors: []string{"named"}, class: &models.Class{VectorConfig: map[string]models.VectorConfig{
			"named":  {VectorIndexConfig: hnsw.UserConfig{Distance: vectorIndex.DistanceCosine}},
			"named2": {VectorIndexConfig: hnsw.UserConfig{Distance: vectorIndex.DistanceCosine}},
		}}, fail: false},
		{name: "multi target vector", targetVectors: []string{"named", "named2"}, class: &models.Class{VectorConfig: map[string]models.VectorConfig{
			"named":  {VectorIndexConfig: hnsw.UserConfig{Distance: vectorIndex.DistanceCosine}},
			"named2": {VectorIndexConfig: hnsw.UserConfig{Distance: vectorIndex.DistanceCosine}},
		}}, fail: true},
		{name: "single target vector and dot", targetVectors: []string{"named"}, class: &models.Class{VectorConfig: map[string]models.VectorConfig{
			"named":  {VectorIndexConfig: hnsw.UserConfig{Distance: vectorIndex.DistanceDot}},
			"named2": {VectorIndexConfig: hnsw.UserConfig{Distance: vectorIndex.DistanceCosine}},
		}}, fail: true},
		{name: "single target vector and dot for non selected", targetVectors: []string{"named2"}, class: &models.Class{VectorConfig: map[string]models.VectorConfig{
			"named":  {VectorIndexConfig: hnsw.UserConfig{Distance: vectorIndex.DistanceDot}},
			"named2": {VectorIndexConfig: hnsw.UserConfig{Distance: vectorIndex.DistanceCosine}},
		}}, fail: false},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckCertaintyCompatibility(tt.class, tt.targetVectors)
			if tt.fail {
				require.NotNil(t, err)
			} else {
				require.Nil(t, err)
			}
		})
	}
}
