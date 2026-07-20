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
	"encoding/binary"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/filters"
	entinverted "github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
)

// restartShardIntoTidyButNotFlippedRangeableWindow returns a shard whose
// rangeableLocalReady[propName] is seeded true from on-disk migration
// history, but whose live class schema IndexRangeFilters is still
// nil/false - the caller decides whether and when to flip it.
func restartShardIntoTidyButNotFlippedRangeableWindow(
	t *testing.T, ctx context.Context, propName string, numObjects int,
) (shard2 *Shard, idx *Index, class *models.Class) {
	t.Helper()
	className := "RangeableBucketLoad_" + uuid.NewString()[:8]
	class = newFilterableToRangeableTestClass(className)

	shard, idx := driveFilterableToRangeableMigrationToLocalTidy(t, ctx, class, propName, numObjects)
	shard2 = restartShardAfterLocalTidy(t, ctx, idx, shard, class)

	require.True(t, shard2.IsRangeableLocallyReady(propName),
		"sanity: seedRangeableLocalReadyFromMigrationHistory must have marked this prop ready post-restart")

	return shard2, idx, class
}

// TestRangeableBucketLoadAfterRestartBeforeSchemaFlip pins
// weaviate/0-weaviate-issues#335 (variant b): seeding
// rangeableLocalReady[prop]=true from migration history without also
// loading the canonical bucket into s.store leaves every consumer of
// that entry (write overlay, read gate) hitting a nil
// s.store.Bucket(...) and hard-erroring.
func TestRangeableBucketLoadAfterRestartBeforeSchemaFlip(t *testing.T) {
	const propName = filterableToRangeablePropName
	const numObjects = 10
	const numDistinctValues = filterableToRangeableNumDistinctValues // 5

	t.Run("write: PutObject after restart lands in the loaded rangeable bucket", func(t *testing.T) {
		ctx := testCtx()
		shard2, _, _ := restartShardIntoTidyButNotFlippedRangeableWindow(t, ctx, propName, numObjects)
		defer shard2.Shutdown(ctx)

		newObj := &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:    strfmt.UUID(uuid.NewString()),
				Class: shard2.class.Class,
				Properties: map[string]interface{}{
					propName: int64(0),
				},
			},
		}
		require.NoError(t, shard2.PutObject(ctx, newObj),
			"a write on the migrating prop in the post-restart tidy-but-not-flipped window "+
				"must not fail with a nil-bucket error; the rangeable bucket physically exists "+
				"on disk from the completed local swap and must be loaded at shard init")

		bucket := shard2.store.Bucket(helpers.BucketRangeableFromPropNameLSM(propName))
		require.NotNil(t, bucket, "rangeable bucket must be registered in the store post-restart")

		fp := filterableToRangeableFingerprint(t, bucket)
		lex, err := entinverted.LexicographicallySortableInt64(0)
		require.NoError(t, err)
		key := binary.BigEndian.Uint64(lex)
		expectedPerValue := numObjects / numDistinctValues
		require.Lenf(t, fp[key], expectedPerValue+1,
			"the new object's posting for value 0 must be present in the loaded rangeable "+
				"bucket alongside the %d pre-restart postings for that value", expectedPerValue)
	})

	t.Run("read: range FindUUIDs succeeds after the schema flip lands without a further restart", func(t *testing.T) {
		ctx := testCtx()
		shard2, _, class := restartShardIntoTidyButNotFlippedRangeableWindow(t, ctx, propName, numObjects)
		defer shard2.Shutdown(ctx)

		// Simulate the cluster-wide RAFT flip landing without a further
		// restart: mutate the same *models.Class pointer the fake schema
		// getter holds.
		trueVal := true
		for _, p := range class.Properties {
			if p.Name == propName {
				p.IndexRangeFilters = &trueVal
			}
		}

		uuids, err := shard2.FindUUIDs(ctx, &filters.LocalFilter{
			Root: &filters.Clause{
				Operator: filters.OperatorGreaterThanEqual,
				On: &filters.Path{
					Class:    schema.ClassName(class.Class),
					Property: propName,
				},
				Value: &filters.Value{
					Value: 0,
					Type:  schema.DataTypeInt,
				},
			},
		}, 1000)
		require.NoError(t, err,
			"a range query on the migrating prop must not fail with a nil-bucket error once the "+
				"live schema flips post-restart; the rangeable bucket must already be loaded")
		require.Len(t, uuids, numObjects,
			"the range query must return every pre-restart object")
	})

	t.Run("idempotent: a later schema-driven CreateOrLoadBucket no-ops on the already-loaded bucket", func(t *testing.T) {
		ctx := testCtx()
		shard2, _, _ := restartShardIntoTidyButNotFlippedRangeableWindow(t, ctx, propName, numObjects)
		defer shard2.Shutdown(ctx)

		bucketName := helpers.BucketRangeableFromPropNameLSM(propName)
		before := shard2.store.Bucket(bucketName)
		require.NotNil(t, before, "the fix must have already loaded the bucket by the time restart completes")

		// Mirror the schema-driven load createPropertyValueIndex performs
		// once the live schema flips (shard_init_properties.go:527-533):
		// same bucket name, same strategy, same option-builder.
		require.NoError(t, shard2.store.CreateOrLoadBucket(
			ctx, bucketName,
			shard2.makeDefaultBucketOptions(lsmkv.StrategyRoaringSetRange)...,
		))

		after := shard2.store.Bucket(bucketName)
		require.Same(t, before, after,
			"CreateOrLoadBucket must no-op on an already-registered bucket, not reload or duplicate it")
	})
}
