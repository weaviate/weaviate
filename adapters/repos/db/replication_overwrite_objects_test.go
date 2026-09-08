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

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/objects"
)

func TestIndexOverwriteObjects(t *testing.T) {
	t.Run("repair write doesn't mutate the caller's object", func(t *testing.T) {
		ctx := testCtx()
		className := "OverwriteMut_" + uuid.NewString()[:8]

		class := &models.Class{
			Class:             className,
			VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
			InvertedIndexConfig: &models.InvertedIndexConfig{
				CleanupIntervalSeconds: 60,
			},
			Properties: []*models.Property{
				{
					Name:         "name",
					DataType:     []string{"text"},
					Tokenization: models.PropertyTokenizationWord,
				},
			},
		}

		shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
			false, false, false)
		shard := shd.(*Shard)
		defer shard.Shutdown(ctx)

		obj := &models.Object{
			ID:                 strfmt.UUID(uuid.NewString()),
			Class:              className,
			CreationTimeUnix:   1,
			LastUpdateTimeUnix: 1,
			Properties: map[string]interface{}{
				"name":  "x",
				"other": nil,
			},
		}

		results, err := idx.OverwriteObjects(ctx, shard.Name(), []*objects.VObject{{
			LatestObject:            obj,
			StaleUpdateTime:         0,
			LastUpdateTimeUnixMilli: 1,
		}})
		require.NoError(t, err)

		for _, r := range results {
			assert.Empty(t, r.Err, "repair write must succeed, got error: %s", r.Err)
		}

		props, ok := obj.Properties.(map[string]interface{})
		require.True(t, ok)
		_, hasOther := props["other"]
		assert.True(t, hasOther, "nil-valued key 'other' must survive in the caller's object")
	})
}
