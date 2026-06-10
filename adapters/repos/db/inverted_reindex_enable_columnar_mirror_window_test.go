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

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	entinverted "github.com/weaviate/weaviate/entities/inverted"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// -----------------------------------------------------------------------------
// Mirror-lifetime regression tests for the enable-columnar migration end
// (runtimeSwap): the double-write mirror must stay correct across the
// SwapBucketPointer rename window and must not be disabled while a write
// that analyzed under the pre-flip schema is still in flight.
//
// Black-box companion: TestEnableColumnar_ConcurrentWritesDuringMigration
// in test/acceptance/columnar_index (stale vint sum under concurrent
// PATCH load racing the migration end).
// -----------------------------------------------------------------------------

// makeColumnarMirrorProp builds the inverted.Property shape the live write
// path produces for an int property that was ANALYZED UNDER THE PRE-FLIP
// SCHEMA: HasColumnarIndex=false (the RAFT flag is still off), value
// encoded as the analyzer's 8-byte lexicographically sortable payload.
func makeColumnarMirrorProp(t *testing.T, propName string, v int64) inverted.Property {
	t.Helper()
	data, err := entinverted.LexicographicallySortableInt64(v)
	require.NoError(t, err)
	return inverted.Property{
		Name:  propName,
		Items: []inverted.Countable{{Data: data}},
		// All Has*Index flags false: pre-flip analysis of a property with
		// no other inverted index — the mirror callback is the only route
		// into the columnar bucket.
	}
}

// TestReindex_DoubleWriteRegistrationRequiresResolvableBuckets pins the
// fail-loud invariant on [ShardReindexTaskGeneric.registerDoubleWriteCallbacks]:
// mirror callbacks MUST NOT activate while any bucket they resolve
// (shard.Store().Bucket(bucketNamer(prop)) — the per-invocation primary
// lookup in every strategy's MakeAddCallback/MakeDeleteCallback) is not
// resolvable. Pre-guard, registering against an unloaded ingest bucket
// silently succeeded and put the shard in the weaviate/weaviate#11678
// client-visible failure shape: every concurrent PATCH 500s with
// "columnar double-write: bucket 'property_<p>_columnar__columnar_ingest_1'
// not found" AFTER its object-store half committed, while the row goes
// permanently missing from the columnar bucket (iterator-skip +
// failed-mirror loss quadrant). The guard turns that ordering regression
// into a deterministic error at migration start.
func TestReindex_DoubleWriteRegistrationRequiresResolvableBuckets(t *testing.T) {
	const numObjects = 10
	propName := enableColumnarPropName

	ctx := testCtx()
	shard, idx, _, className := setupEnableColumnarShard(t, ctx, "EnableColumnarRegGuard", numObjects)

	t.Run("ingest mirror refuses unresolvable bucket", func(t *testing.T) {
		task, _ := newEnableColumnarTask(t, idx, className, propName)

		// The broken ordering under test: register BEFORE loadIngestBuckets.
		disable, err := task.registerDoubleWriteCallbacks(
			shard, []string{propName}, task.ingestBucketName, true)
		require.Error(t, err,
			"registration must refuse to activate mirror callbacks whose target bucket is unresolvable")
		require.Contains(t, err.Error(), task.ingestBucketName(propName))
		require.Nil(t, disable)

		// No callback may have been left behind: a write through the
		// property-value-index path must NOT produce the 500-after-
		// object-commit signature.
		prop := makeColumnarMirrorProp(t, propName, 4242)
		require.NoError(t, shard.addToPropertyValueIndex(uint64(1<<21), prop),
			"a refused registration must leave no active mirror that errors on writes")
	})

	t.Run("backup mirror refuses unresolvable bucket", func(t *testing.T) {
		task, _ := newEnableColumnarTask(t, idx, className, propName)

		disable, err := task.registerDoubleWriteCallbacks(
			shard, []string{propName}, task.backupBucketName, false)
		require.Error(t, err,
			"the backup mirror has strict resolution (no canonical fallback) and must refuse just the same")
		require.Contains(t, err.Error(), task.backupBucketName(propName))
		require.Nil(t, disable)
	})

	t.Run("production ordering registers and mirrors", func(t *testing.T) {
		task, _ := newEnableColumnarTask(t, idx, className, propName)

		// OnAfterLsmInit loads the ingest buckets strictly before
		// registering — the guard must be invisible on the happy path,
		// and a write right after must reach the ingest bucket.
		require.NoError(t, task.OnAfterLsmInit(ctx, shard))
		t.Cleanup(task.disableCallbacks)

		const docID = uint64(1 << 22)
		const value = int64(31337)
		prop := makeColumnarMirrorProp(t, propName, value)
		require.NoError(t, shard.addToPropertyValueIndex(docID, prop))

		ingest := shard.store.Bucket(task.ingestBucketName(propName))
		require.NotNil(t, ingest)
		got, ok, err := ingest.ColumnarLookupInt64(docID, 0)
		require.NoError(t, err)
		require.True(t, ok, "the mirror write must land in the ingest bucket")
		require.Equal(t, value, got)
	})
}

// TestEnableColumnar_MirrorWriteInSwapWindowReachesSwappedBucket pins the
// [Phase 2a, disableCallbacks) window: SwapBucketPointer removes the
// ingest NAME from the store's map (the bucket itself stays live under
// the canonical name), but the mirror callbacks stay registered and
// resolve their target by ingest name per invocation. A write firing in
// that window must neither error ("columnar double-write: bucket not
// found" → PATCH 5xx) nor lose the value — it must land in the swapped
// bucket, which is reachable under the canonical name.
func TestEnableColumnar_MirrorWriteInSwapWindowReachesSwappedBucket(t *testing.T) {
	const (
		numObjects  = 25
		windowDocID = uint64(1 << 20) // distinct from any backfilled docID
		windowValue = int64(999)
	)
	propName := enableColumnarPropName

	ctx := testCtx()
	shard, idx, _, className := setupEnableColumnarShard(t, ctx, "EnableColumnarSwapWindow", numObjects)

	task, wrapped := newEnableColumnarTask(t, idx, className, propName)

	var (
		windowFired        bool
		ingestGoneInWindow bool
		windowWriteErr     error
	)
	orig := task.processOneSwapPropFn
	task.processOneSwapPropFn = func(ctx context.Context, store *lsmkv.Store, rt reindexTracker, propIdx int, propName string) (*lsmkv.Bucket, error) {
		b, err := orig(ctx, store, rt, propIdx, propName)
		if err != nil || windowFired {
			return b, err
		}
		windowFired = true
		// We are now between SwapBucketPointer and disableCallbacks.
		ingestGoneInWindow = store.Bucket(task.ingestBucketName(propName)) == nil
		// Simulate a live write that analyzed under the pre-flip schema
		// executing its property-value-index update right now.
		prop := makeColumnarMirrorProp(t, propName, windowValue)
		windowWriteErr = shard.addToPropertyValueIndex(windowDocID, prop)
		return b, err
	}

	runEnableColumnarTask(t, ctx, task, shard)
	require.True(t, wrapped.migrationCompleted, "migration must complete")
	require.True(t, windowFired, "swap-window injection must have fired")
	require.True(t, ingestGoneInWindow,
		"precondition: SwapBucketPointer must have removed the ingest name — "+
			"this is the exact window under test")

	require.NoError(t, windowWriteErr,
		"a mirror write in the [SwapBucketPointer, disableCallbacks) window must not "+
			"error — the swapped bucket is alive under the canonical name")

	postBucket := shard.store.Bucket(helpers.BucketColumnarFromPropNameLSM(propName))
	require.NotNil(t, postBucket)
	fp := enableColumnarFingerprint(t, postBucket)
	require.Equalf(t, windowValue, fp[windowDocID],
		"the swap-window mirror write must be served by the post-migration columnar bucket")
}

// TestEnableColumnar_DisabledMirrorIsSilentNoOp documents the silent
// no-op mechanism of a disabled mirror: once disableCallbacks has run, a
// write that analyzed under the pre-flip schema (HasColumnarIndex=false)
// reports SUCCESS while writing nothing to the columnar bucket. This is
// by design for the disabled state (errors after disable would fail
// unrelated writes); the correctness burden is therefore on runtimeSwap
// to not disable the mirror while such writes are still in flight — see
// TestEnableColumnar_PreFlipAnalyzedWriteCompletesBeforeMirrorDisable.
func TestEnableColumnar_DisabledMirrorIsSilentNoOp(t *testing.T) {
	const (
		numObjects = 10
		lateDocID  = uint64(1 << 20)
		lateValue  = int64(555)
	)
	propName := enableColumnarPropName

	ctx := testCtx()
	shard, idx, _, className := setupEnableColumnarShard(t, ctx, "EnableColumnarDisabledMirror", numObjects)

	task, _ := newEnableColumnarTask(t, idx, className, propName)
	// Mid-migration state: ingest buckets loaded, mirror callbacks
	// registered, backfill not yet run.
	require.NoError(t, task.OnAfterLsmInit(ctx, shard))

	task.disableCallbacks()

	prop := makeColumnarMirrorProp(t, propName, lateValue)
	require.NoError(t, shard.addToPropertyValueIndex(lateDocID, prop),
		"a disabled mirror must not turn writes into errors")

	for _, bucketName := range []string{
		task.ingestBucketName(propName),
		helpers.BucketColumnarFromPropNameLSM(propName),
	} {
		b := shard.store.Bucket(bucketName)
		if b == nil {
			continue
		}
		_, ok, err := b.ColumnarLookupInt64(lateDocID, 0)
		require.NoError(t, err)
		require.Falsef(t, ok,
			"disabled mirror must write nothing (bucket %q)", bucketName)
	}
}

// TestEnableColumnar_PreFlipAnalyzedWriteCompletesBeforeMirrorDisable pins
// the flip→disable TOCTOU at the migration end: a write that ran
// AnalyzeObject BEFORE OnMigrationComplete flipped the schema flag
// carries HasColumnarIndex=false, so the double-write mirror is its only
// route into the columnar bucket. runtimeSwap must therefore keep the
// mirror enabled until every such in-flight write has completed; if the
// mirror is disabled first, the write reports success while the columnar
// bucket permanently serves the stale backfill value (or misses the row)
// — the residual stale-sum failure of the concurrent-writes acceptance
// test.
func TestEnableColumnar_PreFlipAnalyzedWriteCompletesBeforeMirrorDisable(t *testing.T) {
	const (
		numObjects = 25
		lateDocID  = uint64(1 << 20)
		lateValue  = int64(888)
	)
	propName := enableColumnarPropName

	ctx := testCtx()
	className := "EnableColumnarPreFlipWrite_" + uuid.NewString()[:8]
	class := newEnableColumnarTestClass(className)
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	t.Cleanup(func() { shard.Shutdown(context.Background()) })

	for _, obj := range makeEnableColumnarTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	task, wrapped := newEnableColumnarTask(t, idx, className, propName)

	var lateWriteErr error
	done := make(chan struct{})
	task.onAfterMigrationComplete = func() {
		// Simulate a write that analyzed under the pre-flip schema and is
		// still executing its inverted-index updates when the migration
		// reaches its end: it is inside the inverted-write critical
		// section (updateInvertedIndexLSM holds this gate around
		// analyze→apply), and its property-value-index update lands a
		// little later — after runtimeSwap would (pre-fix) already have
		// disabled the mirror.
		// The RLock is taken synchronously inside the hook (i.e. strictly
		// before the barrier can start draining), modelling a write whose
		// analyze step has already happened; the sleep then places the
		// property-value-index update well after the point where the
		// pre-fix code had already disabled the mirror.
		shard.invertedWriteGate.RLock()
		enterrors.GoWrapper(func() {
			defer close(done)
			defer shard.invertedWriteGate.RUnlock()
			time.Sleep(150 * time.Millisecond)
			prop := makeColumnarMirrorProp(t, propName, lateValue)
			lateWriteErr = shard.addToPropertyValueIndex(lateDocID, prop)
		}, idx.logger)
	}

	runEnableColumnarTask(t, ctx, task, shard)
	require.True(t, wrapped.migrationCompleted, "migration must complete")

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("late writer did not finish")
	}
	require.NoError(t, lateWriteErr,
		"the pre-flip-analyzed write must succeed")

	postBucket := shard.store.Bucket(helpers.BucketColumnarFromPropNameLSM(propName))
	require.NotNil(t, postBucket)
	fp := enableColumnarFingerprint(t, postBucket)
	require.Equalf(t, lateValue, fp[lateDocID],
		"the pre-flip-analyzed write must be served by the post-migration columnar "+
			"bucket — a missing/stale row means the mirror was disabled while the "+
			"write was still in flight")
}
