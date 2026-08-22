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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestMirrorStragglerNeverWritesIntoLiveData drives the gap the mirror's
// canonical fallback leaves open. A writer snapshots the callback state, then
// the migration is torn down — the staged bucket is shut down, so its name
// stops resolving — and the write applies afterwards. Following the canonical
// name there reaches live source-form data and writes the migration's target
// form into it: new terms on the add leg, removed postings on the delete leg.
//
// Word to field is what makes both visible. Word tokenization indexes each
// word, field tokenization indexes the whole value, so a multi-word object's
// target form is a term the live bucket could never hold, and a one-word
// object's target form is exactly one the live bucket does hold.
func TestMirrorStragglerNeverWritesIntoLiveData(t *testing.T) {
	const (
		propName = "title"
		// "zulu" is outside the corpus dictionary, so it names its object.
		multiWord = "zulu romeo sierra"
		oneWord   = "solo"
	)

	tests := []struct {
		name string
		// probe is a word-form term carried by exactly one planted object:
		// the one this leg replays, and the docID it replays under.
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

			// The straggler's snapshot: taken while the mirror is armed, and
			// applied long after the state below has moved on.
			st := shard.loadPropValueIndexState()
			require.NotEmpty(t, st.scope.props, "precondition: the mirror must be armed")

			canonical := shard.store.Bucket(helpers.BucketFromPropNameLSM(propName))
			require.NotNil(t, canonical, "precondition: the live filterable bucket must exist")
			before := fingerprintRoaringSetBucket(t, canonical)
			require.Len(t, before[tt.probe], 1,
				"precondition: %q must name exactly one planted object in the live bucket", tt.probe)
			require.NotContains(t, before, multiWord,
				"precondition: the live bucket is word-tokenized, so it holds no whole-value term")

			// What the cancel edge does, in its order: disarm, then shut the
			// staged buckets down. The straggler already holds its snapshot.
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
