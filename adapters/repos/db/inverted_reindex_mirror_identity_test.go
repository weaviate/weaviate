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

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestMirrorStragglerNeverWritesIntoLiveData(t *testing.T) {
	const (
		propName  = "title"
		multiWord = "zulu romeo sierra"
		oneWord   = "solo"
	)

	tests := []struct {
		name   string
		probe  string
		replay func(t *testing.T, shard *Shard, st *propValueIndexState, obj *storobj.Object, docID uint64)
	}{
		{
			name:  "the add leg would write a target-form term into the live bucket",
			probe: "zulu",
			replay: func(t *testing.T, shard *Shard, st *propValueIndexState, obj *storobj.Object, docID uint64) {
				require.NoError(t, shard.migrationDoubleWrite(st, obj, nil,
					objectInsertStatus{docID: docID}))
			},
		},
		{
			name:  "the delete leg would remove a posting the live bucket owns",
			probe: oneWord,
			replay: func(t *testing.T, shard *Shard, st *propValueIndexState, obj *storobj.Object, docID uint64) {
				require.NoError(t, shard.migrationDoubleWriteDelete(st, obj, docID))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			className := "MirrorStraggler_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{propName})

			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)

			byProbe := map[string]*storobj.Object{
				"zulu":  createTestObjectWithText(className, multiWord),
				oneWord: createTestObjectWithText(className, oneWord),
			}
			for _, obj := range append(makeConvergenceTestObjects(t, 25, className),
				byProbe["zulu"], byProbe[oneWord]) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}

			task, _ := newFilterableRetokenizeTask(t, idx, className, propName,
				models.PropertyTokenizationField)
			require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))

			st := shard.loadPropValueIndexState()
			require.NotEmpty(t, st.scope.props, "precondition: the mirror must be armed")

			canonical := shard.store.Bucket(helpers.BucketFromPropNameLSM(propName))
			require.NotNil(t, canonical, "precondition: the live filterable bucket must exist")
			before := fingerprintRoaringSetBucket(t, canonical)
			require.Len(t, before[tt.probe], 1,
				"precondition: %q must name exactly one planted object in the live bucket", tt.probe)
			require.NotContains(t, before, multiWord,
				"precondition: the live bucket is word-tokenized, so it holds no whole-value term")

			key := task.migrationRecordKey()
			shard.DisarmMigrationMirror(key, propName)
			require.NoError(t, shard.ShutdownStagedBuckets(ctx, key, propName))
			require.Nil(t, shard.store.Bucket(task.ingestBucketName(propName)),
				"precondition: the staged name must stop resolving")

			tt.replay(t, shard, st, byProbe[tt.probe], before[tt.probe][0])

			require.Equal(t, before, fingerprintRoaringSetBucket(t,
				shard.store.Bucket(helpers.BucketFromPropNameLSM(propName))),
				"a mirror whose staged bucket is gone must write nowhere: the canonical name "+
					"denotes live source-form data, not this migration's copy")
		})
	}
}

func TestTheMirrorRefusesAStagedNameItDidNotArmOn(t *testing.T) {
	ctx := testCtx()
	className := "MirrorStagedIdentity_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"title"})
	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	const staged = "property_title_searchable__retokenize_ingest_1"
	require.NoError(t, shard.store.CreateOrLoadBucket(ctx, staged,
		shard.makeDefaultBucketOptions(lsmkv.StrategyInverted)...))
	live := shard.store.Bucket(staged)
	require.NotNil(t, live)

	namer := func(string) string { return staged }
	prop := &inverted.Property{Name: "title"}
	armed := armedMirror{
		props:   map[string]struct{}{"title": {}},
		buckets: map[string]*lsmkv.Bucket{"title": live},
	}

	got, _, skip := resolveScopedDoubleWriteBucket(shard, prop, armed,
		namer, helpers.BucketSearchableFromPropNameLSM)
	require.False(t, skip, "a mirror still aimed at the bucket it armed on writes into it")
	require.Same(t, live, got)

	armed.buckets["title"] = &lsmkv.Bucket{}
	_, _, skip = resolveScopedDoubleWriteBucket(shard, prop, armed,
		namer, helpers.BucketSearchableFromPropNameLSM)
	require.True(t, skip,
		"the staged name now denotes another migration's bucket, and this mirror's rows do not belong in it")
}
