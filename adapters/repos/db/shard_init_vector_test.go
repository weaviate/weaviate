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
	"github.com/weaviate/weaviate/entities/storobj"
)

func TestVectorFromObjectForTarget(t *testing.T) {
	tests := []struct {
		name         string
		legacy       []float32
		named        map[string][]float32
		targetVector string
		want         []float32
	}{
		{name: "legacy vector", legacy: []float32{1, 2}, targetVector: "", want: []float32{1, 2}},
		{name: "no legacy vector is skipped", targetVector: ""},
		{name: "named vector", named: map[string][]float32{"custom": {3, 4}}, targetVector: "custom", want: []float32{3, 4}},
		{name: "missing named vector is skipped", named: map[string][]float32{"other": {5}}, targetVector: "custom"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj := storobj.New(1)
			obj.Object = models.Object{ID: "00000000-0000-4000-8000-000000000001", Class: "Test"}
			obj.Vector = tt.legacy
			obj.Vectors = tt.named
			data, err := obj.MarshalBinary()
			require.NoError(t, err)

			got, err := vectorFromObjectForTarget(tt.targetVector)(data)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
