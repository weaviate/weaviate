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
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/objects"
)

// colocatedSkewClass builds a class with three co-located properties: a
// searchable text prop (the reindex target), a filterable-only text prop, and
// a self-reference. MapToBlockmax targets only the searchable prop; the other
// two stand in for "co-located data a concurrent write touches while the
// searchable prop is being reindexed".
func colocatedSkewClass(className string) *models.Class {
	return &models.Class{
		Class:             className,
		VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60,
			Stopwords:              &models.StopwordConfig{Preset: "none"},
			IndexNullState:         true,
			IndexPropertyLength:    true,
			UsingBlockMaxWAND:      false, // force MapCollection so MapToBlockmax has a source
		},
		Properties: []*models.Property{
			{
				Name:         "title",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWord,
			},
			{
				Name:            "extra",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationField,
				IndexFilterable: boolPtr(true),
				IndexSearchable: boolPtr(false),
			},
			{
				Name:     "toRef",
				DataType: []string{className},
			},
		},
	}
}

func makeColocatedSkewObject(className, id, title, extra string, ts int64) *storobj.Object {
	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:                 strfmt.UUID(id),
			Class:              className,
			CreationTimeUnix:   ts,
			LastUpdateTimeUnix: ts,
			Properties: map[string]interface{}{
				"title": title,
				"extra": extra,
			},
		},
	}
}

func colocatedBatchRefTo(className, sourceID, targetID string, updateTime int64) objects.BatchReference {
	return objects.BatchReference{
		From: &crossref.RefSource{
			Local:    true,
			PeerName: "localhost",
			Class:    schema.ClassName(className),
			Property: schema.PropertyName("toRef"),
			TargetID: strfmt.UUID(sourceID),
		},
		To: &crossref.Ref{
			Local:    true,
			PeerName: "localhost",
			Class:    className,
			TargetID: strfmt.UUID(targetID),
		},
		UpdateTime: updateTime,
	}
}

// TestReindexColocatedWrite_TimestampBumpDoesNotDropScannedProp covers the
// co-located-property leg of the batch-references / delta-merge
// mirror-coverage hole (weaviate/0-weaviate-issues#318, flagged on the
// weaviate/weaviate#11996 review): the batch-reference and merge write paths
// bump an object's LastUpdateTimeUnix WITHOUT firing the double-write
// callbacks for its unchanged co-located properties. Under the pre-fix
// timestamp-cutoff scan (weaviate/weaviate#11692) the bump pushed the object
// past the watermark, so it was neither scanned nor mirrored — its searchable
// title silently vanished from the migrated index.
//
// The skew-immune scan closes this leg by construction: the backfill analyzes
// every object it sees regardless of the bumped timestamp, so the unchanged
// co-located value is always captured from the on-disk row. (Mutations of the
// REF property's own buckets by post-cursor batch-ref writes remain outside
// the double-write tee, but no migration can target ref-property buckets:
// repair-filterable rejects non-primitive data types —
// TestValidateRebuildFilterableDataType. weaviate/weaviate#12210 adds the
// full-prop-set mirror on main.)
//
// Each case seeds a "victim" whose searchable title is unique, starts a
// MapToBlockmax reindex of title, performs a concurrent timestamp-bumping
// write against the victim, drives the reindex to swap, and asserts the
// victim's title posting survived.
func TestReindexColocatedWrite_TimestampBumpDoesNotDropScannedProp(t *testing.T) {
	const (
		pastTS         = int64(1_000)
		victimToken    = "victimtoken"
		controlToken   = "controltoken"
		numControlObjs = 3
	)

	cases := []struct {
		name string
		// preloadRef adds a reference before the reindex starts so the
		// concurrent batch write bumps ref-count 1->2, exercising the
		// batch-ref delete leg alongside the add leg.
		preloadRef bool
		mutate     func(t *testing.T, ctx context.Context, shard *Shard, className, victimID string, updateTime int64)
	}{
		{
			name: "batch_ref_add_first_ref",
			mutate: func(t *testing.T, ctx context.Context, shard *Shard, className, victimID string, updateTime int64) {
				errs := shard.AddReferencesBatch(ctx, objects.BatchReferences{
					colocatedBatchRefTo(className, victimID, uuid.NewString(), updateTime),
				})
				for _, err := range errs {
					require.NoError(t, err)
				}
			},
		},
		{
			name:       "batch_ref_add_second_ref_delete_leg",
			preloadRef: true,
			mutate: func(t *testing.T, ctx context.Context, shard *Shard, className, victimID string, updateTime int64) {
				errs := shard.AddReferencesBatch(ctx, objects.BatchReferences{
					colocatedBatchRefTo(className, victimID, uuid.NewString(), updateTime),
				})
				for _, err := range errs {
					require.NoError(t, err)
				}
			},
		},
		{
			name: "single_reference_merge",
			mutate: func(t *testing.T, ctx context.Context, shard *Shard, className, victimID string, updateTime int64) {
				require.NoError(t, shard.MergeObject(ctx, objects.MergeDocument{
					Class:      className,
					ID:         strfmt.UUID(victimID),
					UpdateTime: updateTime,
					References: objects.BatchReferences{
						colocatedBatchRefTo(className, victimID, uuid.NewString(), updateTime),
					},
				}))
			},
		},
		{
			name: "single_patch_unrelated_prop",
			mutate: func(t *testing.T, ctx context.Context, shard *Shard, className, victimID string, updateTime int64) {
				require.NoError(t, shard.MergeObject(ctx, objects.MergeDocument{
					Class:           className,
					ID:              strfmt.UUID(victimID),
					UpdateTime:      updateTime,
					PrimitiveSchema: map[string]interface{}{"extra": "changed"},
				}))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "ColocSkew_" + uuid.NewString()[:8]
			class := colocatedSkewClass(className)

			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)

			for i := 0; i < numControlObjs; i++ {
				require.NoError(t, shard.PutObject(ctx,
					makeColocatedSkewObject(className, uuid.NewString(), controlToken, "c", pastTS)))
			}

			victimID := uuid.NewString()
			require.NoError(t, shard.PutObject(ctx,
				makeColocatedSkewObject(className, victimID, victimToken, "orig", pastTS)))

			if tc.preloadRef {
				errs := shard.AddReferencesBatch(ctx, objects.BatchReferences{
					colocatedBatchRefTo(className, victimID, uuid.NewString(), pastTS),
				})
				for _, err := range errs {
					require.NoError(t, err)
				}
			}

			// Start the reindex: registers the double-write callbacks and
			// stamps reindexStarted = now (>> pastTS).
			strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
			task := newTestTask(idx.logger, strategy)
			require.NoError(t, task.OnAfterLsmInit(ctx, shard))

			// The concurrent write bumps the victim's LastUpdateTimeUnix an
			// hour past the watermark without firing the callbacks for the
			// unchanged title. Pre-fix, the backfill skipped the victim and
			// the title was lost.
			futureTS := time.Now().UnixMilli() + int64(time.Hour/time.Millisecond)
			tc.mutate(t, ctx, shard, className, victimID, futureTS)

			for {
				rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
				require.NoError(t, err)
				if rerunAt.IsZero() {
					break
				}
			}
			require.True(t, strategy.migrationCompleted)

			bucket := shard.store.Bucket(helpers.BucketSearchableFromPropNameLSM("title"))
			require.NotNil(t, bucket)
			require.Equal(t, lsmkv.StrategyInverted, bucket.Strategy())

			fp := fingerprintInvertedBucket(t, bucket)
			require.Lenf(t, fp[controlToken], numControlObjs,
				"control objects (never concurrently written) must survive the reindex — harness sanity")
			require.NotEmptyf(t, fp[victimToken],
				"co-located searchable 'title' for the concurrently-written object was dropped "+
					"from the new index generation (case %q)", tc.name)
		})
	}
}
