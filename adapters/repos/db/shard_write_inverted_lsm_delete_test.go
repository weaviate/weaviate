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
	"context"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	entinverted "github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestDeleteFromInvertedIndices_MissingBucketKeepsFilterableEntries(t *testing.T) {
	ageKey, err := entinverted.LexicographicallySortableInt64(30)
	require.NoError(t, err)

	tests := []struct {
		name          string
		missingBucket string
		update        bool
	}{
		{name: "delete, rangeable bucket missing", missingBucket: helpers.BucketRangeableFromPropNameLSM("age")},
		{name: "update, rangeable bucket missing", missingBucket: helpers.BucketRangeableFromPropNameLSM("age"), update: true},
		{name: "update, searchable bucket missing", missingBucket: helpers.BucketSearchableFromPropNameLSM("name"), update: true},
		{name: "delete, null-state bucket missing", missingBucket: helpers.BucketFromPropNameNullLSM("age")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			rangeable := true
			class := &models.Class{
				Class:             "MissingBucket_" + uuid.NewString()[:8],
				VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
				InvertedIndexConfig: &models.InvertedIndexConfig{
					CleanupIntervalSeconds: 60,
					Stopwords:              &models.StopwordConfig{Preset: "none"},
					IndexNullState:         true,
					IndexPropertyLength:    true,
				},
				Properties: []*models.Property{
					{Name: "name", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWord},
					{Name: "age", DataType: schema.DataTypeInt.PropString(), IndexRangeFilters: &rangeable},
				},
			}
			shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)

			newObject := func(id strfmt.UUID, name string, age int64) *storobj.Object {
				return &storobj.Object{MarshallerVersion: 1, Object: models.Object{
					ID: id, Class: class.Class,
					Properties: map[string]interface{}{"name": name, "age": age},
				}}
			}
			filterableDocIDs := func(prop string, key []byte) []uint64 {
				bm, release, err := shard.store.Bucket(helpers.BucketFromPropNameLSM(prop)).RoaringSetGet(ctx, key)
				require.NoError(t, err)
				defer release()
				return bm.ToArray()
			}

			id := strfmt.UUID(uuid.NewString())
			require.NoError(t, shard.PutObject(ctx, newObject(id, "alice", 30)))
			nameBefore := filterableDocIDs("name", []byte("alice"))
			ageBefore := filterableDocIDs("age", ageKey)
			require.Len(t, nameBefore, 1)
			require.Len(t, ageBefore, 1)

			require.NoError(t, shard.store.ShutdownBucket(ctx, tt.missingBucket))
			if tt.update {
				err = shard.PutObject(ctx, newObject(id, "bob", 40))
			} else {
				err = shard.DeleteObject(ctx, id, time.Now())
			}
			require.Error(t, err)
			require.Equal(t, nameBefore, filterableDocIDs("name", []byte("alice")))
			require.Equal(t, ageBefore, filterableDocIDs("age", ageKey))
			require.ErrorContains(t, err, tt.missingBucket)
		})
	}
}
