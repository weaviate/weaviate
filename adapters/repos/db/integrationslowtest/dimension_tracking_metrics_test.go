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

//go:build integrationTest

package integrationslowtest

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func TestTotalDimensionTrackingMetrics(t *testing.T) {
	const (
		objectCount         = 100
		multiVecCard        = 3
		dimensionsPerVector = 64
	)

	for _, tt := range []struct {
		name              string
		vectorConfig      func() enthnsw.UserConfig
		namedVectorConfig func() enthnsw.UserConfig
		multiVectorConfig func() enthnsw.UserConfig

		expectDimensions float64
		expectSegments   float64
	}{
		{
			name:         "legacy",
			vectorConfig: func() enthnsw.UserConfig { return enthnsw.NewDefaultUserConfig() },

			expectDimensions: dimensionsPerVector * objectCount,
		},
		{
			name:              "named",
			namedVectorConfig: func() enthnsw.UserConfig { return enthnsw.NewDefaultUserConfig() },

			expectDimensions: dimensionsPerVector * objectCount,
		},
		{
			name:              "multi",
			multiVectorConfig: func() enthnsw.UserConfig { return enthnsw.NewDefaultUserConfig() },

			expectDimensions: multiVecCard * dimensionsPerVector * objectCount,
		},
		{
			name:              "mixed",
			vectorConfig:      func() enthnsw.UserConfig { return enthnsw.NewDefaultUserConfig() },
			namedVectorConfig: func() enthnsw.UserConfig { return enthnsw.NewDefaultUserConfig() },

			expectDimensions: 2 * dimensionsPerVector * objectCount,
		},
		{
			name: "named_with_bq",
			namedVectorConfig: func() enthnsw.UserConfig {
				cfg := enthnsw.NewDefaultUserConfig()
				cfg.BQ.Enabled = true
				return cfg
			},

			expectSegments: (dimensionsPerVector / 8) * objectCount,
		},
		{
			name: "named_with_pq",
			namedVectorConfig: func() enthnsw.UserConfig {
				cfg := enthnsw.NewDefaultUserConfig()
				cfg.PQ.Enabled = true
				cfg.PQ.Segments = 16 // segments should be a divisor of dimensions
				return cfg
			},

			expectSegments: 16 * objectCount,
		},
		{
			name: "named_with_pq_zero_segments",
			namedVectorConfig: func() enthnsw.UserConfig {
				cfg := enthnsw.NewDefaultUserConfig()
				cfg.PQ.Enabled = true
				return cfg
			},
			expectSegments: (dimensionsPerVector / 2) * objectCount,
		},
		{
			name: "multi_and_bq_named",
			namedVectorConfig: func() enthnsw.UserConfig {
				cfg := enthnsw.NewDefaultUserConfig()
				cfg.BQ.Enabled = true
				return cfg
			},
			multiVectorConfig: func() enthnsw.UserConfig { return enthnsw.NewDefaultUserConfig() },
			expectDimensions:  multiVecCard * dimensionsPerVector * objectCount,
			expectSegments:    (dimensionsPerVector / 8) * objectCount,
		},
		{
			name: "named_with_rq_8bit",
			namedVectorConfig: func() enthnsw.UserConfig {
				cfg := enthnsw.NewDefaultUserConfig()
				cfg.RQ.Enabled = true
				cfg.RQ.Bits = 8
				return cfg
			},

			expectDimensions: dimensionsPerVector * objectCount,
		},
		{
			name: "named_with_rq_4bit",
			namedVectorConfig: func() enthnsw.UserConfig {
				cfg := enthnsw.NewDefaultUserConfig()
				cfg.RQ.Enabled = true
				cfg.RQ.Bits = 4
				return cfg
			},
			expectSegments: (dimensionsPerVector / 2) * objectCount,
		},
		{
			name: "named_with_rq_1bit",
			namedVectorConfig: func() enthnsw.UserConfig {
				cfg := enthnsw.NewDefaultUserConfig()
				cfg.RQ.Enabled = true
				cfg.RQ.Bits = 1
				return cfg
			},
			expectSegments: (dimensionsPerVector / 8) * objectCount,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var (
				class = &models.Class{
					Class:               tt.name,
					InvertedIndexConfig: invertedConfig(),
					VectorConfig:        map[string]models.VectorConfig{},
				}

				namedVectorName = "namedVector"
				multiVectorName = "multiVector"

				legacyVec []float32
				namedVecs map[string][]float32
				multiVecs map[string][][]float32
			)

			if tt.vectorConfig != nil {
				class.VectorIndexConfig = tt.vectorConfig()
				legacyVec = randVector(dimensionsPerVector)
			}

			if tt.namedVectorConfig != nil {
				class.VectorConfig[namedVectorName] = models.VectorConfig{
					VectorIndexConfig: tt.namedVectorConfig(),
				}
				namedVecs = map[string][]float32{
					namedVectorName: randVector(dimensionsPerVector),
				}
			}

			if tt.multiVectorConfig != nil {
				config := tt.multiVectorConfig()
				config.Multivector = enthnsw.MultivectorConfig{Enabled: true}
				class.VectorConfig[multiVectorName] = models.VectorConfig{
					VectorIndexConfig: config,
				}

				multiVecs = map[string][][]float32{}
				for range multiVecCard {
					multiVecs[multiVectorName] = append(multiVecs[multiVectorName], randVector(dimensionsPerVector))
				}
			}

			metrics := *monitoring.GetMetrics()
			metrics.Registerer = monitoring.NoopRegisterer
			repo, _ := newRepo(t, repoParams{
				promMetrics: &metrics,
				config: func(c *db.Config) {
					c.TrackVectorDimensions = true
					c.TrackVectorDimensionsInterval = 50 * time.Millisecond
				},
			}, class)

			var (
				shardName = singleShard(t, repo, class.Class).Name()

				insertData = func() {
					for i := range objectCount {
						obj := &models.Object{
							Class: tt.name,
							ID:    intToUUID(i),
						}
						err := repo.PutObject(context.Background(), obj, legacyVec, namedVecs, multiVecs, nil, 0)
						require.Nil(t, err)
					}
				}

				removeData = func() {
					for i := range objectCount {
						err := repo.DeleteObject(context.Background(), class.Class, intToUUID(i), time.Now(), nil, "", 0)
						require.NoError(t, err)
					}
				}

				// the observer publishes on its own interval, so the totals
				// are checked eventually rather than after a manual publish
				assertTotalMetrics = func(expectDims, expectSegs float64) {
					metrics := monitoring.GetMetrics()
					require.EventuallyWithT(t, func(c *assert.CollectT) {
						metric, err := metrics.VectorDimensionsSum.GetMetricWithLabelValues(class.Class, shardName, "")
						require.NoError(c, err)
						assert.Equal(c, expectDims, testutil.ToFloat64(metric))

						metric, err = metrics.VectorSegmentsSum.GetMetricWithLabelValues(class.Class, shardName, "")
						require.NoError(c, err)
						assert.Equal(c, expectSegs, testutil.ToFloat64(metric))
					}, 10*time.Second, 50*time.Millisecond)
				}
			)

			insertData()
			assertTotalMetrics(tt.expectDimensions, tt.expectSegments)
			removeData()
			assertTotalMetrics(0, 0)
			insertData()
			assertTotalMetrics(tt.expectDimensions, tt.expectSegments)
			require.NoError(t, repo.DeleteIndex(schema.ClassName(class.Class)))
			assertTotalMetrics(0, 0)
		})
	}
}
