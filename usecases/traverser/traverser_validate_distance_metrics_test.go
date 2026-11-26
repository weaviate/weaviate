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

package traverser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestValidateCrossClassDistanceCompatibility(t *testing.T) {
	classWithDistance := func(name, distance string) *models.Class {
		cfg := hnsw.NewDefaultUserConfig()
		cfg.Distance = distance
		return &models.Class{Class: name, VectorIndexConfig: cfg}
	}

	tests := []struct {
		name     string
		schema   schema.Schema
		want     string
		wantErrs []string
	}{
		{
			name:   "no schema falls back to the default metric",
			schema: schema.Schema{},
			want:   common.DefaultDistanceMetric,
		},
		{
			name:   "schema without classes falls back to the default metric",
			schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{}}},
			want:   common.DefaultDistanceMetric,
		},
		{
			name: "nil classes are skipped",
			schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
				nil, classWithDistance("A", common.DistanceDot),
			}}},
			want: common.DistanceDot,
		},
		{
			name: "classes sharing a metric return it",
			schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
				classWithDistance("A", common.DistanceL2Squared),
				classWithDistance("B", common.DistanceL2Squared),
			}}},
			want: common.DistanceL2Squared,
		},
		{
			name: "classes with different metrics are rejected",
			schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
				classWithDistance("A", common.DistanceCosine),
				classWithDistance("B", common.DistanceDot),
			}}},
			wantErrs: []string{"found different distance metrics", "class 'A' uses distance metric 'cosine'", "class 'B' uses distance metric 'dot'"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &Traverser{schemaGetter: &fakeSchemaGetter{schema: tt.schema}}

			got, err := tr.validateCrossClassDistanceCompatibility(nil)
			if len(tt.wantErrs) > 0 {
				require.Error(t, err)
				for _, want := range tt.wantErrs {
					assert.Contains(t, err.Error(), want)
				}
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
