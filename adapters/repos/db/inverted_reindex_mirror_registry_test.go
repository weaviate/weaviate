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
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func mirrorKeyForGen(version uint64) MigrationRecordKey {
	return MigrationRecordKey{
		TaskVersion:  version,
		StrategyCode: StrategyCodeSearchableRetokenize,
		UnitID:       "shard-1__node-0",
	}
}

func TestMigrationMirrorRegistry(t *testing.T) {
	gen10, gen20 := mirrorKeyForGen(10), mirrorKeyForGen(20)

	tests := []struct {
		name      string
		exercise  func(r *migrationMirrorRegistry, arm func(MigrationRecordKey, string))
		wantFired map[string]int
		wantArmed int
	}{
		{
			name: "disarming an armed pair runs its handle",
			exercise: func(r *migrationMirrorRegistry, arm func(MigrationRecordKey, string)) {
				arm(gen10, "title")
				r.DisarmMigrationMirror(gen10, "title")
			},
			wantFired: map[string]int{"10/title": 1},
		},
		{
			name: "disarming twice runs it once: every edge that disarms is re-derived at each load",
			exercise: func(r *migrationMirrorRegistry, arm func(MigrationRecordKey, string)) {
				arm(gen10, "title")
				r.DisarmMigrationMirror(gen10, "title")
				r.DisarmMigrationMirror(gen10, "title")
			},
			wantFired: map[string]int{"10/title": 1},
		},
		{
			name: "disarming a pair that was never armed is a no-op",
			exercise: func(r *migrationMirrorRegistry, arm func(MigrationRecordKey, string)) {
				r.DisarmMigrationMirror(gen10, "title")
			},
			wantFired: map[string]int{},
		},
		{
			name: "properties of one record are separable",
			exercise: func(r *migrationMirrorRegistry, arm func(MigrationRecordKey, string)) {
				arm(gen10, "title")
				arm(gen10, "body")
				r.DisarmMigrationMirror(gen10, "title")
			},
			wantFired: map[string]int{"10/title": 1},
			wantArmed: 1,
		},
		{
			name: "two generations on one property stay separable while both are armed",
			exercise: func(r *migrationMirrorRegistry, arm func(MigrationRecordKey, string)) {
				arm(gen10, "title")
				arm(gen20, "title")
				r.DisarmMigrationMirror(gen10, "title")
			},
			wantFired: map[string]int{"10/title": 1},
			wantArmed: 1,
		},
		{
			name: "re-arming a pair disarms the handle it replaces",
			exercise: func(r *migrationMirrorRegistry, arm func(MigrationRecordKey, string)) {
				arm(gen10, "title")
				arm(gen10, "title")
			},
			wantFired: map[string]int{"10/title": 1},
			wantArmed: 1,
		},
		{
			name: "a whole record disarms at once without touching another one",
			exercise: func(r *migrationMirrorRegistry, arm func(MigrationRecordKey, string)) {
				arm(gen10, "title")
				arm(gen10, "body")
				arm(gen20, "title")
				r.DisarmMigrationMirrors(gen10)
			},
			wantFired: map[string]int{"10/title": 1, "10/body": 1},
			wantArmed: 1,
		},
		{
			name: "disarming a record that has nothing armed is a no-op",
			exercise: func(r *migrationMirrorRegistry, arm func(MigrationRecordKey, string)) {
				arm(gen20, "title")
				r.DisarmMigrationMirrors(gen10)
			},
			wantFired: map[string]int{},
			wantArmed: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// The zero value has to be usable: a shard holds one as a plain
			// field, with no constructor to run.
			var registry migrationMirrorRegistry

			fired := map[string]int{}
			arm := func(key MigrationRecordKey, prop string) {
				label := fmt.Sprintf("%d/%s", key.TaskVersion, prop)
				registry.ArmMigrationMirror(key, prop, func() { fired[label]++ })
			}

			tt.exercise(&registry, arm)

			require.Equal(t, tt.wantFired, fired)
			require.Equal(t, tt.wantArmed, registry.ArmedMigrationMirrors())
		})
	}
}

func TestMigrationMirrorRegistryConcurrentAccess(t *testing.T) {
	var registry migrationMirrorRegistry

	const actors = 8
	var wg sync.WaitGroup
	for i := range actors {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := mirrorKeyForGen(uint64(i + 1))
			for round := range 64 {
				registry.ArmMigrationMirror(key, "title", func() {})
				registry.ArmMigrationMirror(key, "body", func() {})
				_ = registry.ArmedMigrationMirrors()
				if round%2 == 0 {
					registry.DisarmMigrationMirror(key, "title")
				} else {
					registry.DisarmMigrationMirrors(key)
				}
			}
			registry.DisarmMigrationMirrors(key)
		}()
	}
	wg.Wait()

	require.Zero(t, registry.ArmedMigrationMirrors())
}

// TestMigrationMirrorDisarmIsPerProperty exercises the shape production arms:
// one registration over the migration's whole property set, published as one
// handle per property. Disarming a property has to stop exactly that
// property's mirror. Leaving it armed would write predecessor-form rows into
// the successor's live bucket the moment the staged one is shut down, and
// disarming the whole scope would stop mirroring properties no successor took
// over.
func TestMigrationMirrorDisarmIsPerProperty(t *testing.T) {
	const (
		retired = "title"
		kept    = "body"
	)
	ctx := testCtx()
	className := "MirrorDisarmPerProp_" + uuid.NewString()[:8]
	class := newEnableFilterableTestClass(className, retired, kept)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(context.Background())

	task, _ := newEnableFilterableTask(t, idx, className, retired, kept)
	require.NoError(t, task.OnAfterLsmInit(ctx, shard))

	registry := shard.migrationMirrorRegistry()
	require.Equal(t, 2, registry.ArmedMigrationMirrors(),
		"one handle per property, or no actor can disarm just the property it took over")

	registry.DisarmMigrationMirror(task.migrationRecordKey(), retired)
	require.Equal(t, 1, registry.ArmedMigrationMirrors())

	require.NoError(t, shard.PutObject(ctx, &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:                 strfmt.UUID(uuid.NewString()),
			Class:              className,
			Properties:         map[string]interface{}{retired: "alpha", kept: "bravo"},
			CreationTimeUnix:   time.Now().UnixMilli(),
			LastUpdateTimeUnix: time.Now().UnixMilli(),
		},
	}))

	mirrored := func(propName, term string) []uint64 {
		t.Helper()
		bucket := shard.store.Bucket(task.ingestBucketName(propName))
		require.NotNil(t, bucket, "staged bucket for %q", propName)
		bm, release, err := bucket.RoaringSetGet(ctx, []byte(term))
		require.NoError(t, err)
		defer release()
		if bm == nil {
			return nil
		}
		return bm.ToArray()
	}

	require.Empty(t, mirrored(retired, "alpha"),
		"a disarmed property must stop mirroring")
	require.Len(t, mirrored(kept, "bravo"), 1,
		"a property nobody disarmed must keep mirroring")
}

// TestOverlappingMirrorsOnOneProperty pins the steady state supersession
// creates: two records mirroring one property at once. Each arming is owned by
// its own record, so one record's disarm must leave the other's mirror copying
// — and must not un-suppress the inline write path, which analyzes under the
// source schema and would land source-tokenized rows in the survivor's staged
// bucket, the one a flip is about to make canonical.
func TestOverlappingMirrorsOnOneProperty(t *testing.T) {
	const (
		propName = "title"
		text     = "alpha bravo"
	)

	tests := []struct {
		name string
		// journey disarms or re-arms, then makes the user writes whose mirror
		// the assertions inspect.
		journey func(t *testing.T, ctx context.Context, shard *Shard, className string,
			predecessor, successor *ShardReindexTaskGeneric)
		survivor func(predecessor, successor *ShardReindexTaskGeneric) *ShardReindexTaskGeneric
		// wantDocs maps a term in the survivor's staged bucket to the number
		// of documents that must be posted under it.
		wantDocs map[string]int
	}{
		{
			name: "the predecessor's disarm leaves the successor's mirror copying",
			journey: func(t *testing.T, ctx context.Context, shard *Shard, className string,
				predecessor, _ *ShardReindexTaskGeneric,
			) {
				shard.migrationMirrorRegistry().DisarmMigrationMirror(predecessor.migrationRecordKey(), propName)
				require.NoError(t, shard.PutObject(ctx, createTestObjectWithText(className, text)))
			},
			survivor: func(_, successor *ShardReindexTaskGeneric) *ShardReindexTaskGeneric { return successor },
			wantDocs: map[string]int{"alpha bravo": 1, "alpha": 0, "bravo": 0},
		},
		{
			name: "the successor's disarm leaves the predecessor's mirror copying",
			journey: func(t *testing.T, ctx context.Context, shard *Shard, className string,
				_, successor *ShardReindexTaskGeneric,
			) {
				shard.migrationMirrorRegistry().DisarmMigrationMirror(successor.migrationRecordKey(), propName)
				require.NoError(t, shard.PutObject(ctx, createTestObjectWithText(className, text)))
			},
			survivor: func(predecessor, _ *ShardReindexTaskGeneric) *ShardReindexTaskGeneric { return predecessor },
			wantDocs: map[string]int{"alpha bravo": 1, "alpha": 0, "bravo": 0},
		},
		{
			name: "re-arming one record's property leaves its new mirror copying",
			journey: func(t *testing.T, ctx context.Context, shard *Shard, className string,
				_, successor *ShardReindexTaskGeneric,
			) {
				require.NoError(t, successor.OnAfterLsmInit(ctx, shard))
				require.NoError(t, shard.PutObject(ctx, createTestObjectWithText(className, text)))
			},
			survivor: func(_, successor *ShardReindexTaskGeneric) *ShardReindexTaskGeneric { return successor },
			wantDocs: map[string]int{"alpha bravo": 1, "alpha": 0, "bravo": 0},
		},
		{
			name: "the predecessor's disarm leaves the successor's mirror deleting",
			journey: func(t *testing.T, ctx context.Context, shard *Shard, className string,
				predecessor, successor *ShardReindexTaskGeneric,
			) {
				obj := createTestObjectWithText(className, text)
				require.NoError(t, shard.PutObject(ctx, obj))
				require.Len(t, fingerprintInvertedBucket(t,
					shard.store.Bucket(successor.ingestBucketName(propName)))[text], 1,
					"both mirrors are armed for this write, so the term has to be there to be removed")

				shard.migrationMirrorRegistry().DisarmMigrationMirror(predecessor.migrationRecordKey(), propName)
				require.NoError(t, shard.DeleteObject(ctx, obj.ID(), time.Now()))
			},
			survivor: func(_, successor *ShardReindexTaskGeneric) *ShardReindexTaskGeneric { return successor },
			wantDocs: map[string]int{"alpha bravo": 0, "alpha": 0, "bravo": 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			className := "MirrorOverlap_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{propName})

			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(context.Background())

			// FIELD against the class's WORD source: the mirror's own analysis
			// and the inline path's differ term for term, which is what makes
			// a lost suppression visible rather than merely theoretical.
			bucketStrategy := shard.store.Bucket(helpers.BucketSearchableFromPropNameLSM(propName)).Strategy()
			predecessor, _ := newSearchableRetokenizeTaskAtGeneration(t, idx, className, propName,
				models.PropertyTokenizationField, bucketStrategy, 1)
			require.NoError(t, predecessor.OnAfterLsmInit(ctx, shard))
			successor, _ := newSearchableRetokenizeTaskAtGeneration(t, idx, className, propName,
				models.PropertyTokenizationField, bucketStrategy, 2)
			require.NoError(t, successor.OnAfterLsmInit(ctx, shard))
			require.Equal(t, 2, shard.migrationMirrorRegistry().ArmedMigrationMirrors(),
				"two generations on one property is the steady state under test")

			tt.journey(t, ctx, shard, className, predecessor, successor)

			survivor := tt.survivor(predecessor, successor)
			staged := shard.store.Bucket(survivor.ingestBucketName(propName))
			require.NotNil(t, staged, "the survivor's staged bucket")
			fingerprint := fingerprintInvertedBucket(t, staged)
			for term, want := range tt.wantDocs {
				assert.Lenf(t, fingerprint[term], want,
					"documents posted under %q in the survivor's staged bucket", term)
			}
		})
	}
}
