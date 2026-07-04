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

package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
	"github.com/weaviate/weaviate/entities/storobj"
)

func TestDropVectorIndex_IsDropped(t *testing.T) {
	none := modelsext.VectorIndexTypeNone

	classWith := func(cfg map[string]models.VectorConfig) *models.Class {
		return &models.Class{Class: "TestClass", VectorConfig: cfg}
	}

	tests := []struct {
		name         string
		class        *models.Class
		targetVector string
		want         bool
	}{
		{name: "nil class", class: nil, targetVector: "foo", want: false},
		{name: "legacy vector empty target is never dropped", class: classWith(map[string]models.VectorConfig{"": {VectorIndexType: none}}), targetVector: "", want: false},
		{name: "target absent from config", class: classWith(map[string]models.VectorConfig{"foo": {VectorIndexType: "hnsw"}}), targetVector: "bar", want: false},
		{name: "active hnsw", class: classWith(map[string]models.VectorConfig{"foo": {VectorIndexType: "hnsw"}}), targetVector: "foo", want: false},
		{name: "active flat", class: classWith(map[string]models.VectorConfig{"foo": {VectorIndexType: "flat"}}), targetVector: "foo", want: false},
		{name: "active dynamic", class: classWith(map[string]models.VectorConfig{"foo": {VectorIndexType: "dynamic"}}), targetVector: "foo", want: false},
		{name: "dropped", class: classWith(map[string]models.VectorConfig{"foo": {VectorIndexType: none}}), targetVector: "foo", want: true},
		{name: "empty config map", class: classWith(map[string]models.VectorConfig{}), targetVector: "foo", want: false},
		{name: "nil config map", class: &models.Class{Class: "TestClass"}, targetVector: "foo", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isDroppedVectorIndex(tt.class, tt.targetVector))
		})
	}
}

func TestDropVectorIndex_RejectObjectVectors(t *testing.T) {
	none := modelsext.VectorIndexTypeNone

	classWith := func(cfg map[string]models.VectorConfig) *models.Class {
		return &models.Class{Class: "TestClass", VectorConfig: cfg}
	}

	objWith := func(vectors map[string][]float32, multi map[string][][]float32) *storobj.Object {
		return &storobj.Object{Object: models.Object{Class: "TestClass"}, Vectors: vectors, MultiVectors: multi}
	}

	tests := []struct {
		name       string
		class      *models.Class
		object     *storobj.Object
		wantErr    bool
		wantTarget string
	}{
		{
			name:   "no vectors",
			class:  classWith(map[string]models.VectorConfig{"foo": {VectorIndexType: none}}),
			object: objWith(nil, nil),
		},
		{
			name:   "active single vector",
			class:  classWith(map[string]models.VectorConfig{"foo": {VectorIndexType: "hnsw"}}),
			object: objWith(map[string][]float32{"foo": {1, 2, 3}}, nil),
		},
		{
			name:       "dropped single vector",
			class:      classWith(map[string]models.VectorConfig{"foo": {VectorIndexType: none}}),
			object:     objWith(map[string][]float32{"foo": {1, 2, 3}}, nil),
			wantErr:    true,
			wantTarget: "foo",
		},
		{
			name:       "dropped multi vector",
			class:      classWith(map[string]models.VectorConfig{"bar": {VectorIndexType: none}}),
			object:     objWith(nil, map[string][][]float32{"bar": {{1, 2}, {3, 4}}}),
			wantErr:    true,
			wantTarget: "bar",
		},
		{
			name:  "mixed - one active one dropped",
			class: classWith(map[string]models.VectorConfig{"foo": {VectorIndexType: "hnsw"}, "baz": {VectorIndexType: none}}),
			// only the dropped one must trigger; assert the target appears in the error
			object:     objWith(map[string][]float32{"foo": {1}, "baz": {2}}, nil),
			wantErr:    true,
			wantTarget: "baz",
		},
		{
			name:   "all active across vectors and multivectors",
			class:  classWith(map[string]models.VectorConfig{"foo": {VectorIndexType: "hnsw"}, "bar": {VectorIndexType: "hnsw"}}),
			object: objWith(map[string][]float32{"foo": {1}}, map[string][][]float32{"bar": {{1}}}),
		},
		{
			name:   "nil class falls back to allow (existing !ok branch handles it)",
			class:  nil,
			object: objWith(map[string][]float32{"foo": {1}}, nil),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := rejectDroppedObjectVectors(tt.class, tt.object)
			if !tt.wantErr {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), "vector index not found")
			assert.Contains(t, err.Error(), tt.wantTarget)
		})
	}
}
