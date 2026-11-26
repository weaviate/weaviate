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

package vectorindex

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	"github.com/weaviate/weaviate/entities/vectorindex/flat"
	"github.com/weaviate/weaviate/entities/vectorindex/hfresh"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Every index type's config survives a trip through its JSON, which is how
// a shard's mapping stores the config its index adopted.
func TestUnmarshalConfig(t *testing.T) {
	hnswCfg := hnsw.NewDefaultUserConfig()
	hnswCfg.EF = 200
	hnswCfg.PQ.Enabled = true
	hnswCfg.PQ.Segments = 4
	hnswCfg.Multivector.Enabled = true
	hnswCfg.Multivector.MuveraConfig.Enabled = true
	flatCfg := flat.NewDefaultUserConfig()
	flatCfg.BQ.Enabled = true
	flatCfg.BQ.RescoreLimit = 50
	dynamicCfg := dynamic.NewDefaultUserConfig()
	dynamicCfg.Threshold = 123
	dynamicCfg.HnswUC.EF = 64
	dynamicCfg.FlatUC.VectorCacheMaxObjects = 10
	hfreshCfg := hfresh.NewDefaultUserConfig()
	hfreshCfg.SearchProbe = 7
	hfreshCfg.RQ.RescoreLimit = 99

	for _, cfg := range []schemaConfig.VectorIndexConfig{hnswCfg, flatCfg, dynamicCfg, hfreshCfg} {
		t.Run(cfg.IndexType(), func(t *testing.T) {
			data, err := json.Marshal(cfg)
			require.NoError(t, err)
			got, err := UnmarshalConfig(cfg.IndexType(), data)
			require.NoError(t, err)
			assert.Equal(t, cfg, got)
		})
	}

	t.Run("unknown type", func(t *testing.T) {
		_, err := UnmarshalConfig("none", []byte(`{}`))
		require.ErrorContains(t, err, `unknown vector index type "none"`)
	})

	t.Run("fills in no defaults", func(t *testing.T) {
		got, err := UnmarshalConfig(VectorIndexTypeHNSW, []byte(`{"ef":5}`))
		require.NoError(t, err)
		assert.Equal(t, hnsw.UserConfig{EF: 5}, got)
	})

	t.Run("malformed", func(t *testing.T) {
		_, err := UnmarshalConfig(VectorIndexTypeFLAT, []byte(`{"vectorCacheMaxObjects":"ten"}`))
		require.Error(t, err)
	})
}
