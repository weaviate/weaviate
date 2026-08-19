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
	"sync"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestAnalyzeObject(t *testing.T) {
	ctx := testCtx()
	className := "AnalyzeTS_" + uuid.NewString()[:8]

	class := &models.Class{
		Class:             className,
		VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60,
			IndexTimestamps:        true,
		},
		Properties: []*models.Property{
			{
				Name:         "name",
				DataType:     []string{"text"},
				Tokenization: models.PropertyTokenizationWord,
			},
		},
	}

	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	makeObj := func() *storobj.Object {
		return &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:                 strfmt.UUID(uuid.NewString()),
				Class:              className,
				CreationTimeUnix:   1000,
				LastUpdateTimeUnix: 2000,
				Properties: map[string]interface{}{
					"name": "hello",
				},
			},
		}
	}

	t.Run("timestamp props don't leak into object properties", func(t *testing.T) {
		obj := makeObj()
		props, _, _, err := shard.AnalyzeObject(obj)
		require.NoError(t, err)

		var hasCreation, hasUpdate bool
		for _, p := range props {
			switch p.Name {
			case filters.InternalPropCreationTimeUnix:
				hasCreation = true
			case filters.InternalPropLastUpdateTimeUnix:
				hasUpdate = true
			}
		}
		assert.True(t, hasCreation, "analyzer must emit _creationTimeUnix property")
		assert.True(t, hasUpdate, "analyzer must emit _lastUpdateTimeUnix property")

		m, ok := obj.Properties().(map[string]interface{})
		require.True(t, ok)
		_, leaked1 := m[filters.InternalPropCreationTimeUnix]
		_, leaked2 := m[filters.InternalPropLastUpdateTimeUnix]
		assert.False(t, leaked1, "_creationTimeUnix must not leak into object properties")
		assert.False(t, leaked2, "_lastUpdateTimeUnix must not leak into object properties")
	})

	t.Run("no map write racing a concurrent marshal", func(t *testing.T) {
		const n = 100
		objs := make([]*storobj.Object, n)
		for i := range objs {
			objs[i] = makeObj()
		}

		var wg sync.WaitGroup
		wg.Add(2)

		// Goroutine A: marshals the object (reads Properties).
		go func() {
			defer wg.Done()
			for _, o := range objs {
				_, _ = o.PrepareMarshalOptional(additional.Properties{Vector: true})
			}
		}()

		// Goroutine B: analyzes the object (touches the Properties map).
		go func() {
			defer wg.Done()
			for _, o := range objs {
				_, _, _, _ = shard.AnalyzeObject(o)
			}
		}()

		wg.Wait()

		for i, o := range objs {
			m, ok := o.Properties().(map[string]interface{})
			require.True(t, ok)
			_, leaked := m[filters.InternalPropCreationTimeUnix]
			assert.False(t, leaked, "object %d: _creationTimeUnix leaked", i)
		}
	})
}
