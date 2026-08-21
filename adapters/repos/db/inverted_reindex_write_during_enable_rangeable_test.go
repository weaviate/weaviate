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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/filters"
	entinverted "github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// newNoLiveIndexRangeableTestClass forces IndexFilterable false, so the
// property has no live index and only the double-write can cover it.
func newNoLiveIndexRangeableTestClass(className string) *models.Class {
	c := newFilterableToRangeableTestClass(className)
	noIndex := false
	c.Properties[0].IndexFilterable = &noIndex
	return c
}

// newNoNullStateRangeableTestClass is newNoLiveIndexRangeableTestClass
// without null-state and property-length indexing. A property with no live
// index has neither bucket, and nothing creates them when a migration turns
// its index on, so a write once the property is indexed fails on the missing
// bucket. That gap is tracked separately; tests about the write window use
// this variant so they fail on their own subject.
//
// A separate variant, not a change to the shared fixture: the shared one
// also backs the swap-window double-write guards, which need the buckets.
func newNoNullStateRangeableTestClass(className string) *models.Class {
	c := newNoLiveIndexRangeableTestClass(className)
	c.InvertedIndexConfig.IndexNullState = false
	c.InvertedIndexConfig.IndexPropertyLength = false
	return c
}

// readRangeableIDs returns docIDs for one int64 value in a RoaringSetRange
// bucket; sibling of filterableToRangeableFingerprint for a single value.
func readRangeableIDs(t *testing.T, b *lsmkv.Bucket, v int64) []uint64 {
	t.Helper()
	require.Equal(t, lsmkv.StrategyRoaringSetRange, b.Strategy(),
		"readRangeableIDs requires a RoaringSetRange bucket")
	reader := b.ReaderRoaringSetRange()
	defer reader.Close()
	lex, err := entinverted.LexicographicallySortableInt64(v)
	require.NoError(t, err)
	key := binary.BigEndian.Uint64(lex)
	bm, release, err := reader.Read(context.Background(), key, filters.OperatorEqual)
	require.NoError(t, err)
	if release != nil {
		defer release()
	}
	if bm == nil {
		return nil
	}
	return bm.ToArray()
}

// TestReindex_ConcurrentWriteDuringEnableRangeable_NotLost pins
// weaviate/0-weaviate-issues#298: a write to a no-live-index property during
// an enable-rangeable migration must survive the swap via the double-write.
func TestReindex_ConcurrentWriteDuringEnableRangeable_NotLost(t *testing.T) {
	ctx := testCtx()
	const propName = filterableToRangeablePropName
	const numObjects = 25
	// Outside the corpus so its posting list is unambiguously this write.
	const concurrentValue = int64(4242)

	className := "EnableRangeableConc_" + uuid.NewString()[:8]
	class := newNoLiveIndexRangeableTestClass(className)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeFilterableToRangeableTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	task, _ := newFilterableToRangeableTask(t, idx, className, propName)

	// Drive to reindexed-but-not-swapped: iterator done, double-write live.
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))

	// The concurrent write: lands in the FINALIZING window, before swap.
	require.NoError(t, shard.PutObject(ctx, &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:         strfmt.UUID(uuid.NewString()),
			Class:      className,
			Properties: map[string]interface{}{propName: concurrentValue},
		},
	}))

	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))

	rangeBucket := shard.store.Bucket(helpers.BucketRangeableFromPropNameLSM(propName))
	require.NotNil(t, rangeBucket, "post-swap rangeable bucket must exist")

	require.NotEmptyf(t, readRangeableIDs(t, rangeBucket, 0),
		"positive control: iterator-backfilled corpus (value 0) must be present in the rangeable bucket; "+
			"got none — the migration never populated it and the assertion below would prove nothing")

	ids := readRangeableIDs(t, rangeBucket, concurrentValue)
	assert.NotEmptyf(t, ids,
		"#298 enable-rangeable: object written during the reindex window is NOT under the target rangeable value "+
			"%d — its ForceRangeable double-write was lost, so a range query misses it after the swap", concurrentValue)
}

// TestReindex_WriteAfterEnableRangeableSwap_NotLost pins that writes
// between this shard's swap and the cluster-wide schema flip must still
// reach the rangeable bucket, not be silently dropped.
func TestReindex_WriteAfterEnableRangeableSwap_NotLost(t *testing.T) {
	const propName = filterableToRangeablePropName
	const numObjects = 25
	// Outside the corpus so its posting list is unambiguously this write.
	const postSwapValue = int64(5150)

	for _, tc := range []struct {
		name         string
		cancelAfter  bool
		wantIndexed  bool
		explainEmpty string
	}{
		{
			name:        "flip pending: the write reaches the rangeable bucket",
			wantIndexed: true,
			explainEmpty: "object written after the swap is NOT under rangeable value %d — " +
				"the double-write is gone and the schema flip has not landed, so the write was silently dropped from the index",
		},
		{
			name:        "task cancelled after the swap: the write is dropped",
			cancelAfter: true,
			wantIndexed: false,
			explainEmpty: "a cancelled migration must stop filling the bucket, but value %d is still there; " +
				"the terminal transition did not take effect and writes keep feeding an index nothing will read",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "EnableRangeablePostSwap_" + uuid.NewString()[:8]
			class := newNoNullStateRangeableTestClass(className)

			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)

			for _, obj := range makeFilterableToRangeableTestObjects(t, numObjects, className) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}

			// The production strategy, not the shared wrapper: this test
			// turns on OnMigrationComplete's readiness side effect, which
			// the wrapper stubs out.
			task := newFilterableToRangeableTaskWithStrategy(t, idx, className, propName,
				&FilterableToRangeableStrategy{propNames: []string{propName}, generation: 1})

			require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
			require.NoError(t, task.RunPrepareOnShard(ctx, shard))
			require.NoError(t, task.RunSwapOnShard(ctx, shard))

			// The live schema still says IndexRangeFilters=false here: the
			// flip is the provider's job and has not run.
			require.False(t, inverted.HasRangeableIndex(class.Properties[0]),
				"the window this test covers only exists while the flag is still false")

			if tc.cancelAfter {
				// What autoCleanupAfterTerminal does once the drain
				// finishes, reduced to its one effect on this shard.
				shard.setRangeableLocallyReady(propName, false)
			}

			require.NoError(t, shard.PutObject(ctx, &storobj.Object{
				MarshallerVersion: 1,
				Object: models.Object{
					ID:         strfmt.UUID(uuid.NewString()),
					Class:      className,
					Properties: map[string]interface{}{propName: postSwapValue},
				},
			}))

			rangeBucket := shard.store.Bucket(helpers.BucketRangeableFromPropNameLSM(propName))
			require.NotNil(t, rangeBucket, "post-swap rangeable bucket must exist")
			require.NotEmptyf(t, readRangeableIDs(t, rangeBucket, 0),
				"positive control: the backfilled corpus (value 0) must be present, else the assertion below proves nothing")

			ids := readRangeableIDs(t, rangeBucket, postSwapValue)
			if tc.wantIndexed {
				assert.NotEmptyf(t, ids, tc.explainEmpty, postSwapValue)
			} else {
				assert.Emptyf(t, ids, tc.explainEmpty, postSwapValue)
			}
		})
	}
}

// TestForcedRangeableProps_StateWalk walks every state a property can be in
// with respect to an enable-rangeable migration, and pins which of them force
// an ordinary write into the rangeable bucket.
//
// The write side is derived from the readiness the query side already keeps,
// so the states are (live flag, readiness entry) pairs. Each row names the
// journey that produces the pair; readiness is only ever written by
// PreReindexHook (false), OnMigrationComplete and the post-restart seed
// (true), and the terminal transition (false).
//
// Several journeys land on the same pair, deliberately: what separates a
// restart inside the window from a cancel after the swap is whether the
// seed runs, not what the predicate then sees. That separation is pinned
// in TestSeedRangeableReadinessAfterRestart.
func TestForcedRangeableProps_StateWalk(t *testing.T) {
	const propName = "price"
	trueVal, falseVal := true, false

	for _, tc := range []struct {
		name string
		// flag is the live schema's IndexRangeFilters.
		flag *bool
		// ready is the shard's readiness entry; nil means no entry at all.
		ready *bool
		want  bool
		why   string
	}{
		{
			name: "steady state, property was never rangeable",
			why:  "no migration ever ran, so nothing may divert the write",
		},
		{
			name: "backfill running", ready: &falseVal,
			why: "PreReindexHook marked it not ready; the double-write callbacks cover this window",
		},
		{
			name: "swapped, flip pending", ready: &trueVal, want: true,
			why: "the window this whole mechanism exists for: the callbacks are down and the flag is not up yet",
		},
		{
			name: "swapped, flip pending, after a restart", ready: &trueVal, want: true,
			why: "the seed re-marks readiness from the live task, so a restart no longer loses the window",
		},
		{
			name: "swapped, flip pending, after a tenant COLD to HOT", ready: &trueVal, want: true,
			why: "activation runs the same shard init and the same seed",
		},
		{
			name: "recovery path completed the swap", ready: &trueVal, want: true,
			why: "the recovery branch marks readiness exactly like the live swap",
		},
		{
			name: "cancelled before the swap", ready: &falseVal,
			why: "no bucket pointer was ever flipped",
		},
		{
			name: "cancelled after the swap", ready: &falseVal,
			why: "the terminal transition un-readies the property once the local task has drained",
		},
		{
			name: "cancelled after the swap, then a restart",
			why:  "the promoted directory survives, but no live task vouches for it, so the seed never fires and it stays dormant debris",
		},
		{
			name: "crash mid-swap, then a restart", ready: &trueVal, want: true,
			why: "the seed covers this the same way, from the live task",
		},
		{
			name: "flip landed", flag: &trueVal, ready: &trueVal,
			why: "the analyzer emits the property from the live flag; forcing it again would be redundant work on every write",
		},
		{
			name: "flip landed, then repair-rangeable ran", flag: &trueVal, ready: &trueVal,
			why: "repair arrives with the flag already true, so it can never reach the predicate",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			props := []*models.Property{{
				Name:              propName,
				DataType:          schema.DataTypeInt.PropString(),
				IndexRangeFilters: tc.flag,
			}}
			s := &Shard{}
			if tc.ready != nil {
				s.setRangeableLocallyReady(propName, *tc.ready)
			}

			forced := s.forcedRangeableProps(props)
			_, got := forced[propName]
			assert.Equalf(t, tc.want, got, "forcedRangeableProps: %s", tc.why)

			overlay := s.writeAnalyzerOverlay(props)
			if tc.want {
				assert.Equalf(t, map[string]inverted.PropertyOverlay{
					propName: {ForceRangeable: true},
				}, overlay, "writeAnalyzerOverlay must project the forced property: %s", tc.why)
			} else {
				assert.Nilf(t, overlay,
					"writeAnalyzerOverlay must take its fast path and project nothing: %s", tc.why)
			}
		})
	}
}

// TestForcedRangeableProps_IgnoresBucketExistence pins that a loaded
// rangeable bucket alone cannot force writes. IsRangeableLocallyReady
// defaults to "ready" when a bucket exists and no entry does, which is the
// right answer for queries and the wrong one here: a bucket left behind by a
// cancelled migration would otherwise keep collecting writes forever.
func TestForcedRangeableProps_IgnoresBucketExistence(t *testing.T) {
	ctx := testCtx()
	const propName = filterableToRangeablePropName

	className := "ForcedRangeableDefault_" + uuid.NewString()[:8]
	class := newNoNullStateRangeableTestClass(className)
	trueVal := true
	class.Properties[0].IndexRangeFilters = &trueVal

	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	require.NotNil(t, shard.store.Bucket(helpers.BucketRangeableFromPropNameLSM(propName)),
		"the bucket must exist, else this test proves nothing")
	require.True(t, shard.IsRangeableLocallyReady(propName),
		"the query side must answer ready from bucket existence alone")

	// The state a cancelled migration leaves: bucket on disk, no entry, flag
	// back to what it was before the migration.
	falseVal := false
	props := []*models.Property{{
		Name:              propName,
		DataType:          schema.DataTypeInt.PropString(),
		IndexRangeFilters: &falseVal,
	}}
	assert.Empty(t, shard.forcedRangeableProps(props),
		"a bucket with no explicit readiness entry must not force writes")
}

// TestSeedRangeableReadinessAfterRestart pins the seed that survives a
// restart inside the swap window. On disk the window looks exactly like a
// migration cancelled after its swap, so the seed runs only where a live
// task is in hand, and refuses every state that is not that window.
func TestSeedRangeableReadinessAfterRestart(t *testing.T) {
	const propName = filterableToRangeablePropName
	trueVal := true

	for _, tc := range []struct {
		name          string
		migrationType ReindexMigrationType
		flagAlreadyOn bool
		promotedDir   bool
		properties    []string
		wantReady     bool
		why           string
	}{
		{
			name:          "restart inside the swap window",
			migrationType: ReindexTypeEnableRangeable,
			promotedDir:   true,
			properties:    []string{propName},
			wantReady:     true,
			why:           "the swapped bucket must keep taking writes until the cluster-wide flip lands",
		},
		{
			name:          "this shard never reached its swap",
			migrationType: ReindexTypeEnableRangeable,
			properties:    []string{propName},
			why:           "no promoted directory, so there is no swapped bucket to keep filling",
		},
		{
			name:          "the flip already landed",
			migrationType: ReindexTypeEnableRangeable,
			flagAlreadyOn: true,
			promotedDir:   true,
			properties:    []string{propName},
			why:           "shard init loads the bucket off the live flag and the analyzer emits the property on its own",
		},
		{
			name:          "repair-rangeable",
			migrationType: ReindexTypeRepairRangeable,
			promotedDir:   true,
			properties:    []string{propName},
			why:           "repair runs with the flag already true, so it has no window to cover",
		},
		{
			name:          "the property was dropped from the schema",
			migrationType: ReindexTypeEnableRangeable,
			promotedDir:   true,
			properties:    []string{"gone"},
			why:           "a property that no longer exists needs no index",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "SeedRangeable_" + uuid.NewString()[:8]
			class := newNoNullStateRangeableTestClass(className)
			if tc.flagAlreadyOn {
				class.Properties[0].IndexRangeFilters = &trueVal
			}

			shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)

			bucketName := helpers.BucketRangeableFromPropNameLSM(propName)
			if !tc.flagAlreadyOn {
				require.Nil(t, shard.store.Bucket(bucketName),
					"with the flag off, shard init must not have loaded the bucket")
			}
			if tc.promotedDir && !tc.flagAlreadyOn {
				require.NoError(t, os.MkdirAll(filepath.Join(shard.pathLSM(), bucketName), 0o755))
			}
			// A restart empties the readiness map; the seed is what refills it.
			shard.rangeableLocalReadyMu.Lock()
			shard.rangeableLocalReady = nil
			shard.rangeableLocalReadyMu.Unlock()

			logger, _ := logrustest.NewNullLogger()
			seedRangeableReadinessAfterRestart(ctx, &ReindexTaskPayload{
				MigrationType: tc.migrationType,
				Collection:    className,
				Properties:    tc.properties,
			}, shard, logger)

			shard.rangeableLocalReadyMu.RLock()
			ready, set := shard.rangeableLocalReady[propName]
			shard.rangeableLocalReadyMu.RUnlock()

			assert.Equalf(t, tc.wantReady, set && ready, "readiness: %s", tc.why)
			if tc.wantReady {
				assert.NotNilf(t, shard.store.Bucket(bucketName),
					"the seed must load the bucket too, or the next write hard-fails on it: %s", tc.why)
			}
		})
	}
}

// TestUnreadyRangeableAfterTerminal_RunsBehindTheDrain pins the ordering
// that closes the resurrection window: a cancelled task un-readies the
// property only once the local worker has drained. Un-readying while a swap
// is still running lets its OnMigrationComplete set the property ready
// again, permanently, because nothing runs after it.
func TestUnreadyRangeableAfterTerminal_RunsBehindTheDrain(t *testing.T) {
	const propName = filterableToRangeablePropName

	for _, tc := range []struct {
		name       string
		workerHung bool
		wantReady  bool
	}{
		{name: "worker drained", wantReady: false},
		{name: "worker still running", workerHung: true, wantReady: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "TerminalUnready_" + uuid.NewString()[:8]
			shd, idx := testShardWithSettings(t, ctx, newNoNullStateRangeableTestClass(className),
				enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)
			shard.setRangeableLocallyReady(propName, true)

			payload, err := json.Marshal(ReindexTaskPayload{
				MigrationType: ReindexTypeEnableRangeable,
				Collection:    className,
				Properties:    []string{propName},
				UnitToShard:   map[string]string{"u1": shard.Name()},
			})
			require.NoError(t, err)

			// A short server context so a hung worker ends the drain on the
			// deadline rather than the much longer cleanup timeout.
			serverCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			logger, _ := logrustest.NewNullLogger()
			p := NewReindexProvider(
				&DB{indices: map[string]*Index{indexID(schema.ClassName(className)): idx}},
				nil, nil, logger, "n1", nil, serverCtx)

			desc := distributedtask.TaskDescriptor{ID: "T_unready", Version: 1}
			if tc.workerHung {
				structuralInvariantInjectHandle(p, desc)
			}

			require.NoError(t, p.OnTaskCompleted(&distributedtask.Task{
				Namespace:      ReindexNamespace,
				TaskDescriptor: desc,
				Status:         distributedtask.TaskStatusCancelled,
				Payload:        payload,
			}))

			shard.rangeableLocalReadyMu.RLock()
			ready := shard.rangeableLocalReady[propName]
			shard.rangeableLocalReadyMu.RUnlock()
			assert.Equal(t, tc.wantReady, ready,
				"the transition must sit behind WaitForLocalTaskDrain, never in front of it")
		})
	}
}

// TestResolveUnitForPhase_SeedsRangeableReadinessOnTheSkipPath pins where
// the seed is wired. After a restart, startup finalize has promoted the
// bucket and deleted the tracker, so the unit resolves to no work and the
// phase callback skips it. That skip is the last place the live task and
// this shard are both in hand, so it is where the readiness lost to the
// restart has to come back.
func TestResolveUnitForPhase_SeedsRangeableReadinessOnTheSkipPath(t *testing.T) {
	ctx := testCtx()
	const propName = filterableToRangeablePropName
	className := "SkipPathSeed_" + uuid.NewString()[:8]

	shd, idx := testShardWithSettings(t, ctx, newNoNullStateRangeableTestClass(className),
		enthnsw.UserConfig{Skip: true}, false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	// The state a restart inside the window leaves behind: the promoted
	// bucket directory, no tracker, and an empty readiness map.
	bucketName := helpers.BucketRangeableFromPropNameLSM(propName)
	require.NoError(t, os.MkdirAll(filepath.Join(shard.pathLSM(), bucketName), 0o755))
	require.Nil(t, shard.store.Bucket(bucketName))

	logger, _ := logrustest.NewNullLogger()
	p := NewReindexProvider(
		&DB{indices: map[string]*Index{indexID(schema.ClassName(className)): idx}},
		nil, nil, logger, "n1", nil, ctx)

	payload := &ReindexTaskPayload{
		MigrationType: ReindexTypeEnableRangeable,
		Collection:    className,
		Properties:    []string{propName},
		UnitToShard:   map[string]string{"u1": shard.Name()},
	}
	res := p.resolveUnitForPhase(ctx, &distributedtask.Task{
		Namespace:      ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_skip_seed", Version: 1},
		Status:         distributedtask.TaskStatusSwapping,
	}, payload, "u1", idx, logger)

	require.True(t, res.Skip, "no tracker on disk, so the unit must resolve to no work")
	assert.True(t, shard.IsRangeableLocallyReady(propName),
		"the skip must not drop the window on the floor: writes until the flip belong in the rangeable bucket")
	assert.NotNil(t, shard.store.Bucket(bucketName),
		"the seed must load the bucket, or the next write hard-fails on it")
}
