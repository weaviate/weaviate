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
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/additional"
	entcfg "github.com/weaviate/weaviate/entities/config"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/logrusext"
	"github.com/weaviate/weaviate/usecases/objects"
)

type missingBucketWrite struct {
	name            string
	create, deletes bool
	write           func(ctx context.Context, s *Shard, obj *storobj.Object) error
}

var missingBucketWrites = []missingBucketWrite{
	{name: "put", write: func(ctx context.Context, s *Shard, obj *storobj.Object) error {
		return s.PutObject(ctx, obj)
	}},
	{name: "merge", write: func(ctx context.Context, s *Shard, obj *storobj.Object) error {
		return s.MergeObject(ctx, objects.MergeDocument{
			Class: obj.Class().String(), ID: obj.ID(), UpdateTime: obj.LastUpdateTimeUnix(),
			PrimitiveSchema: obj.Properties().(map[string]interface{}),
		})
	}},
	{name: "create", create: true, write: func(ctx context.Context, s *Shard, obj *storobj.Object) error {
		return s.PutObject(ctx, obj)
	}},
	{name: "delete", deletes: true, write: func(ctx context.Context, s *Shard, obj *storobj.Object) error {
		return s.DeleteObject(ctx, obj.ID(), time.Time{})
	}},
	{name: "batch delete", deletes: true, write: func(ctx context.Context, s *Shard, obj *storobj.Object) error {
		return s.DeleteObjectBatch(ctx, []strfmt.UUID{obj.ID()}, time.Time{}, false)[0].Err
	}},
}

func missingBucketClass(className string) *models.Class {
	class := newTestClassWithProps(className, []string{"title", "other"})
	class.InvertedIndexConfig.IndexTimestamps = true
	class.Properties = append(class.Properties,
		&models.Property{Name: "num", DataType: schema.DataTypeInt.PropString(), IndexRangeFilters: boolPtr(true)},
		&models.Property{
			Name: "nested", DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{{
				Name: "name", DataType: schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWord, IndexFilterable: boolPtr(true),
			}},
		})
	return class
}

func missingBucketObject(className string, id strfmt.UUID, word string, n float64) *storobj.Object {
	return &storobj.Object{MarshallerVersion: 1, Object: models.Object{
		ID: id, Class: className, LastUpdateTimeUnix: time.Now().UnixMilli(),
		Properties: map[string]interface{}{
			"title": "title " + word, "other": word, "num": n,
			"nested": map[string]interface{}{"name": word},
		},
	}}
}

func filterableHas(t *testing.T, s *Shard, prop, value string, docID uint64) bool {
	t.Helper()
	bm, release, err := s.store.Bucket(helpers.BucketFromPropNameLSM(prop)).RoaringSetGet(testCtx(), []byte(value))
	require.NoError(t, err)
	defer release()
	return bm.Contains(docID)
}

// A write needing a missing bucket must fail before changing anything.
func TestFailedWriteChangesNothing(t *testing.T) {
	t.Setenv(entcfg.EnvNestedFilteringPreview, "true")
	type row struct {
		write   missingBucketWrite
		missing string // empty: every bucket is present
	}
	var rows []row
	for _, w := range missingBucketWrites {
		rows = append(rows, row{w, ""}, row{w, helpers.BucketFromPropNameLengthLSM("title")})
		if w.name != "put" && w.name != "delete" {
			continue
		}
		for _, missing := range []string{
			helpers.BucketFromPropNameLSM("title"),
			helpers.BucketSearchableFromPropNameLSM("title"),
			helpers.BucketFromPropNameNullLSM("title"),
			helpers.BucketRangeableFromPropNameLSM("num"),
			helpers.BucketNestedFromPropNameLSM("nested"),
		} {
			rows = append(rows, row{w, missing})
		}
	}

	for _, r := range rows {
		t.Run(fmt.Sprintf("%s, missing %q", r.write.name, r.missing), func(t *testing.T) {
			ctx := testCtx()
			className := "MissingBucket_" + uuid.NewString()[:8]
			shd, _ := testShardWithSettings(t, ctx, missingBucketClass(className), enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)

			id := strfmt.UUID(uuid.NewString())
			if !r.write.create {
				require.NoError(t, shard.PutObject(ctx, missingBucketObject(className, id, "alpha", 1)))
			}
			if r.missing != "" {
				require.NoError(t, shard.store.ShutdownBucket(ctx, r.missing))
			}
			logger, hook := logrustest.NewNullLogger()
			shard.refusedWriteSampler = logrusext.NewSampler(logger, maxReportedErrors, time.Minute)

			err := r.write.write(ctx, shard, missingBucketObject(className, id, "beta", 2))
			stored, lookupErr := shard.ObjectByID(ctx, id, nil, additional.Properties{})
			require.NoError(t, lookupErr)

			if r.missing == "" {
				require.NoError(t, err)
				if r.write.deletes {
					require.Nil(t, stored)
					return
				}
				require.NotNil(t, stored)
				assert.True(t, filterableHas(t, shard, "other", "beta", stored.DocID))
				return
			}

			require.Error(t, err)
			defer func() {
				assert.ErrorContains(t, err, r.missing)
				if assert.NotNil(t, hook.LastEntry(), "a refused write must be logged") {
					assert.Equal(t, r.missing, hook.LastEntry().Data["bucket"])
				}
			}()
			if r.write.create {
				assert.Nil(t, stored, "a refused create must not store the object")
				return
			}
			require.NotNil(t, stored, "a refused %s must not remove the object", r.write.name)
			value := stored.Properties().(map[string]interface{})["other"].(string)
			assert.Equal(t, "alpha", value, "a refused %s must not change the stored object", r.write.name)
			assert.True(t, filterableHas(t, shard, "other", value, stored.DocID),
				"the stored value must stay findable by an Equal filter")
			assert.True(t, r.missing == helpers.BucketFromPropNameLSM("title") ||
				filterableHas(t, shard, "title", "alpha", stored.DocID),
				"the other properties' entries must stay intact")
		})
	}
}

// A reference batch refuses only references to a property with a missing bucket.
func TestReferenceBatchRefusesOnlyThePropertyWithAMissingBucket(t *testing.T) {
	ctx := testCtx()
	className := "MissingRefBucket_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"title"})
	for _, name := range []string{"refA", "refB"} {
		class.Properties = append(class.Properties, &models.Property{Name: name, DataType: []string{className}})
	}
	class.Properties = append(class.Properties, &models.Property{
		Name: "unindexed", DataType: []string{className},
		IndexFilterable: boolPtr(false), IndexSearchable: boolPtr(false),
	})
	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	ids := make([]strfmt.UUID, 4)
	for i := range ids {
		obj := createTestObjectWithText(className, "t")
		ids[i] = obj.ID()
		require.NoError(t, shard.PutObject(ctx, obj))
	}
	require.NoError(t, shard.store.ShutdownBucket(ctx, helpers.BucketFromPropNameLSM("refA")))

	ref := func(from strfmt.UUID, prop string) objects.BatchReference {
		return objects.BatchReference{
			From: crossref.NewSource(schema.ClassName(className), schema.PropertyName(prop), from),
			To:   crossref.New("localhost", className, ids[3]),
		}
	}
	props := []string{"refA", "refB", "unindexed"}
	errs := shard.AddReferencesBatch(ctx, objects.BatchReferences{
		ref(ids[0], props[0]), ref(ids[1], props[1]), ref(ids[2], props[2]),
	})
	for i, prop := range props {
		stored, err := shard.ObjectByID(ctx, ids[i], nil, additional.Properties{})
		require.NoError(t, err)
		_, has := stored.Properties().(map[string]interface{})[prop]
		assert.Equal(t, prop != "refA", has, "the reference to %s", prop)
	}
	require.Len(t, errs, 3)
	assert.ErrorContains(t, errs[0], helpers.BucketFromPropNameLSM("refA"))
	assert.NoError(t, errs[1])
	assert.NoError(t, errs[2])
}

func TestRangeableDemotionLogsOncePerChange(t *testing.T) {
	tests := []struct {
		name    string
		prior   []bool
		ready   bool
		wantLog bool
	}{
		{"unset to not ready", nil, false, true},
		{"ready to not ready", []bool{true}, false, true},
		{"not ready again", []bool{false}, false, false},
		{"not ready to ready", []bool{false}, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()
			shard := &Shard{name: "s1", index: &Index{logger: logger}}
			for _, ready := range tt.prior {
				shard.setRangeableLocallyReady("num", ready)
			}
			hook.Reset()
			shard.setRangeableLocallyReady("num", tt.ready)
			require.Equal(t, tt.wantLog, hook.LastEntry() != nil)
			if tt.wantLog {
				require.Equal(t, "num", hook.LastEntry().Data["property"])
			}
		})
	}
}

// A delete cannot read which postings an undecodable object has, so it removes the
// object and leaves those postings behind instead of refusing.
func TestDeleteRemovesAnObjectThatNoLongerDecodes(t *testing.T) {
	for _, w := range missingBucketWrites {
		if !w.deletes {
			continue
		}
		t.Run(w.name, func(t *testing.T) {
			ctx := testCtx()
			className := "UndecodableDelete_" + uuid.NewString()[:8]
			shd, _ := testShardWithSettings(t, ctx, missingBucketClass(className), enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)

			obj := missingBucketObject(className, strfmt.UUID(uuid.NewString()), "alpha", 1)
			require.NoError(t, shard.PutObject(ctx, obj))
			idBytes, err := uuid.MustParse(obj.ID().String()).MarshalBinary()
			require.NoError(t, err)
			objects := shard.store.Bucket(helpers.ObjectsBucketLSM)
			stored, err := objects.Get(idBytes)
			require.NoError(t, err)
			docID, _, err := storobj.DocIDAndTimeFromBinary(stored)
			require.NoError(t, err)
			require.NoError(t, shard.upsertObjectDataLSM(objects, idBytes, stored[:storobj.MarshallerV1HeaderLen+2], docID))

			require.ErrorContains(t, w.write(ctx, shard, obj), "unmarshal previous object")
			remaining, err := objects.Get(idBytes)
			require.NoError(t, err)
			require.Nil(t, remaining)
		})
	}
}
