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
	"sync/atomic"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// The routing sends writes into a bucket the schema does not name, so the two
// events that make that bucket the wrong destination each have to stop it: a
// property update whose apply removes the index, and the cluster flipping the
// flag on.
//
// Both are asserted the same way — a write afterwards behaves as the schema
// says it should — because that is what a caller notices. A write into a
// bucket that was just deleted does not merely miss the index; it fails, and
// takes the whole object write with it.
func TestTheRoutingStopsWhenTheBucketOrTheSchemaChanges(t *testing.T) {
	const propName = "score"

	newArmedShard := func(t *testing.T) (*Shard, *models.Class, string) {
		t.Helper()
		ctx := testCtx()
		className := "PromotedDisarm_" + uuid.NewString()[:8]
		off := false
		class := &models.Class{
			Class:             className,
			VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
			InvertedIndexConfig: &models.InvertedIndexConfig{
				CleanupIntervalSeconds: 60,
				Stopwords:              &models.StopwordConfig{Preset: "none"},
				IndexNullState:         true,
				IndexPropertyLength:    true,
				UsingBlockMaxWAND:      false,
			},
			Properties: []*models.Property{{
				Name:              propName,
				DataType:          schema.DataTypeInt.PropString(),
				IndexFilterable:   &off,
				IndexSearchable:   &off,
				IndexRangeFilters: &off,
			}},
		}
		shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
			false, false, false)
		shard := shd.(*Shard)
		t.Cleanup(func() { shard.Shutdown(context.Background()) })

		(&EnableFilterableStrategy{propNames: []string{propName}}).
			PreReindexHook(shard, []string{propName})
		require.NoError(t, shard.armPromotedIndex(ctx, propName, "filterable"))
		require.True(t, shard.isPromotedIndexArmed(propName, "filterable"))
		return shard, class, className
	}

	putOne := func(t *testing.T, shard *Shard, className string, score int64) error {
		t.Helper()
		return shard.PutObject(testCtx(), &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:         strfmt.UUID(uuid.NewString()),
				Class:      className,
				Properties: map[string]interface{}{propName: score},
			},
		})
	}

	t.Run("a property update removes the bucket the routing points at", func(t *testing.T) {
		ctx := testCtx()
		shard, class, className := newArmedShard(t)

		dropped := &models.Property{
			Name:              propName,
			DataType:          class.Properties[0].DataType,
			IndexFilterable:   boolPtr(false),
			IndexSearchable:   boolPtr(false),
			IndexRangeFilters: boolPtr(false),
		}
		eg := enterrors.NewErrorGroupWrapper(nullLogger())
		var payloadReads atomic.Int64
		shard.updatePropertyBuckets(ctx, eg, dropped, &payloadReads)
		require.NoError(t, eg.Wait())

		require.Nil(t, shard.store.Bucket(helpers.BucketFromPropNameLSM(propName)),
			"the DELETE must have removed the bucket the routing pointed at")
		assert.NoError(t, putOne(t, shard, className, 7),
			"a write after the index was dropped must succeed, not chase the removed bucket")
	})

	t.Run("the schema flip makes the routing redundant", func(t *testing.T) {
		shard, class, className := newArmedShard(t)

		class.Properties[0].IndexFilterable = boolPtr(true)
		require.NoError(t, putOne(t, shard, className, 7))

		assert.False(t, shard.isPromotedIndexArmed(propName, "filterable"),
			"once the schema advertises the index the write path resolves it on its own; "+
				"a routing entry that outlives that would survive a later DELETE too")
	})
}
