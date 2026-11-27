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
	"github.com/weaviate/weaviate/usecases/config"
)

func TestValidateCrossClassDistanceCompatibility(t *testing.T) {
	cases := []struct {
		name              string
		distances         map[string]string // class name to distance metric
		namespacesEnabled bool
		wantErr           string // empty when the classes are compatible
	}{
		{
			name:      "classes with one metric are compatible",
			distances: map[string]string{"Plain": common.DistanceCosine, "Other": common.DistanceCosine},
		},
		{
			name:      "classes with different metrics are refused",
			distances: map[string]string{"Plain": common.DistanceCosine, "Other": common.DistanceL2Squared},
			wantErr:   "vector search across classes not possible",
		},
		{
			name:              "NAMESPACES_ENABLED refuses even with one metric",
			distances:         map[string]string{"ns1:Plain": common.DistanceCosine},
			namespacesEnabled: true,
			wantErr:           "not supported with NAMESPACES_ENABLED",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var classes []*models.Class
			for name, distance := range tc.distances {
				cfg := hnsw.NewDefaultUserConfig()
				cfg.Distance = distance
				classes = append(classes, &models.Class{Class: name, VectorIndexConfig: cfg})
			}
			traverser := &Traverser{
				config: &config.WeaviateConfig{Config: config.Config{
					Namespaces: config.Namespaces{Enabled: tc.namespacesEnabled},
				}},
				schemaGetter: &fakeSchemaGetter{
					schema: schema.Schema{Objects: &models.Schema{Classes: classes}},
				},
			}

			distType, err := traverser.validateCrossClassDistanceCompatibility(nil)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, common.DistanceCosine, distType)
		})
	}
}
