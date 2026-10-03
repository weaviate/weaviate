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

// colocatedSkewClass has a searchable reindex-target prop plus a
// filterable prop and a self-reference, standing in for co-located data a
// concurrent write touches during reindexing.
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

// TestReindexColocatedWrite_TimestampBumpDoesNotDropScannedProp pins that a
// batch-ref/merge write bumping LastUpdateTimeUnix without touching a
// co-located searchable prop must not drop that prop's posting from the
// migrated index (weaviate/0-weaviate-issues#318, weaviate/weaviate#11692).
func TestReindexColocatedWrite_TimestampBumpDoesNotDropScannedProp(t *testing.T) {
	const (
		pastTS         = int64(1_000)
		victimToken    = "victimtoken"
		controlToken   = "controltoken"
		numControlObjs = 3
	)

	cases := []struct {
		name string
		// preloadRef exercises the batch-ref delete leg by bumping ref-count 1->2.
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

			strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
			task := newTestTask(idx.logger, strategy)
			require.NoError(t, task.OnAfterLsmInit(ctx, shard))

			// Bumps LastUpdateTimeUnix an hour past the watermark without
			// mirroring the unchanged title.
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
