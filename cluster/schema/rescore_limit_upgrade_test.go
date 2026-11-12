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

package schema_test

import (
	"encoding/json"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	clusterSchema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/vectorindex"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// TestRestoreLegacyQuantizerRescoreLimit restores a RAFT snapshot written by a
// version that had no rescoreLimit on PQ and BQ, which is what every upgraded
// node replays on startup. The restored class must resolve the missing key to
// the quantizer's default and not to the disabled zero value, while a limit
// that was frozen into the schema stays as stored. No older version wrote a
// rescoreFactor, so every restored class must land on the automatic factor.
func TestRestoreLegacyQuantizerRescoreLimit(t *testing.T) {
	tests := []struct {
		name      string
		legacy    string
		stored    func(uc hnswent.UserConfig) int
		effective func(uc hnswent.UserConfig) int
		expect    int
		expectEff int
		policy    func(uc hnswent.UserConfig) hnswent.RescorePolicy
		factor    float64
	}{
		{
			name:      "pq stored without rescoreLimit",
			legacy:    `{"pq":{"enabled":true,"bitCompression":false,"segments":0,"centroids":256,"trainingLimit":100000,"encoder":{"type":"kmeans","distribution":"log-normal"}}}`,
			stored:    func(uc hnswent.UserConfig) int { return uc.PQ.RescoreLimit },
			effective: func(uc hnswent.UserConfig) int { return uc.PQ.EffectiveRescoreLimit() },
			expect:    hnswent.RescoreLimitAuto,
			expectEff: hnswent.DefaultPQRescoreLimit,
			policy:    func(uc hnswent.UserConfig) hnswent.RescorePolicy { return uc.PQ.RescorePolicy() },
			factor:    hnswent.DefaultPQRescoreFactor,
		},
		{
			name:      "bq stored without rescoreLimit",
			legacy:    `{"bq":{"enabled":true}}`,
			stored:    func(uc hnswent.UserConfig) int { return uc.BQ.RescoreLimit },
			effective: func(uc hnswent.UserConfig) int { return uc.BQ.EffectiveRescoreLimit() },
			expect:    hnswent.RescoreLimitAuto,
			expectEff: hnswent.DefaultBQRescoreLimit,
			policy:    func(uc hnswent.UserConfig) hnswent.RescorePolicy { return uc.BQ.RescorePolicy() },
			factor:    hnswent.DefaultBQRescoreFactor,
		},
		{
			name:      "sq stored with its old default",
			legacy:    `{"sq":{"enabled":true,"trainingLimit":100000,"rescoreLimit":20}}`,
			stored:    func(uc hnswent.UserConfig) int { return uc.SQ.RescoreLimit },
			effective: func(uc hnswent.UserConfig) int { return uc.SQ.EffectiveRescoreLimit() },
			expect:    20,
			expectEff: 20,
			policy:    func(uc hnswent.UserConfig) hnswent.RescorePolicy { return uc.SQ.RescorePolicy() },
			factor:    hnswent.DefaultSQRescoreFactor,
		},
		{
			name:      "rq 8 bit stored with its old default",
			legacy:    `{"rq":{"enabled":true,"bits":8,"rescoreLimit":20,"centering":false,"trainingLimit":10000}}`,
			stored:    func(uc hnswent.UserConfig) int { return uc.RQ.RescoreLimit },
			effective: func(uc hnswent.UserConfig) int { return uc.RQ.EffectiveRescoreLimit() },
			expect:    20,
			expectEff: 20,
			policy:    func(uc hnswent.UserConfig) hnswent.RescorePolicy { return uc.RQ.RescorePolicy() },
			factor:    hnswent.DefaultRQRescoreFactor,
		},
		{
			name:      "rq 1 bit stored with its old default",
			legacy:    `{"rq":{"enabled":true,"bits":1,"rescoreLimit":512,"centering":false,"trainingLimit":10000}}`,
			stored:    func(uc hnswent.UserConfig) int { return uc.RQ.RescoreLimit },
			effective: func(uc hnswent.UserConfig) int { return uc.RQ.EffectiveRescoreLimit() },
			expect:    512,
			expectEff: 512,
			policy:    func(uc hnswent.UserConfig) hnswent.RescorePolicy { return uc.RQ.RescorePolicy() },
			factor:    hnswent.DefaultBRQRescoreFactor,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parser := schema.NewParser(fakes.NewFakeClusterState(), vectorindex.ParseAndValidateConfig,
				acceptAllIndexUpdates{}, endpointModules{}, nil, nil)
			sm := clusterSchema.NewSchemaManager("node1", nil, parser, prometheus.NewPedanticRegistry(), logrus.New())

			// the snapshot is the on-disk form of the schema's class map: a
			// metaClass per name, whose Class is the swagger model with the
			// vector index config as the JSON the writing version produced
			shardState, err := json.Marshal(sharding.State{Physical: map[string]sharding.Physical{}})
			require.NoError(t, err)
			snapshot := `{"` + className + `":{"Class":{"class":"` + className + `","vectorizer":"none",` +
				`"vectorIndexType":"hnsw","vectorIndexConfig":` + test.legacy + `},` +
				`"ClassVersion":1,"Sharding":` + string(shardState) + `,"ShardVersion":1}}`

			require.NoError(t, sm.Restore([]byte(snapshot), parser))

			cls := sm.NewSchemaReader().ReadOnlyClass(className)
			require.NotNil(t, cls)
			uc, ok := cls.VectorIndexConfig.(hnswent.UserConfig)
			require.True(t, ok, "restored config is %T", cls.VectorIndexConfig)
			require.Equal(t, test.expect, test.stored(uc), "stored")
			require.Equal(t, test.expectEff, test.effective(uc), "effective")
			require.Equal(t, hnswent.RescorePolicy{Limit: test.expectEff, Factor: test.factor}, test.policy(uc), "policy")
			for _, stored := range []float64{uc.PQ.RescoreFactor, uc.BQ.RescoreFactor, uc.SQ.RescoreFactor, uc.RQ.RescoreFactor} {
				require.Equal(t, hnswent.RescoreFactorAuto, stored, "stored rescoreFactor")
			}
		})
	}
}
