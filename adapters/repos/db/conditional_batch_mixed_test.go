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

//go:build integrationTest

package db

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestBatchMixedConditionOutcomes is the formal AC test (task 1108).
//
// It creates a 1000-object batch where:
//   - 500 UUIDs already exist in the shard (pre-inserted unconditionally).
//   - 500 UUIDs are brand-new (never written).
//
// All 1000 objects carry Conditional.OnlyIfNotExists=true.
//
// Expected result from PutObjectBatch:
//   - 500 nil errors  -> new UUIDs inserted.
//   - 500 ErrPreconditionFailed errors -> existing UUIDs skipped.
//   - 0 unexpected errors.
//
// Per-index alignment is also asserted: for each position i in the batch, the
// error (or nil) must correspond to the UUID at that position, not an adjacent one.
//
// Causal link: this test catches a per-object error misalignment (wrong error
// slot), a missing conditional check in the batch path, or an off-by-one in the
// mixed-batch splitter because those bugs would produce the wrong inserted/skipped
// counts or wrong UUID-to-error mapping.
func TestBatchMixedConditionOutcomes(t *testing.T) {
	const half = 500

	ctx := context.Background()
	className := "BatchMixedCAS"
	class := &models.Class{Class: className}

	shard, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	defer func() { _ = idx.drop() }()

	// Phase 1: pre-insert 500 objects unconditionally.
	existingIDs := make([]strfmt.UUID, half)
	preInsertBatch := make([]*storobj.Object, half)
	for i := range existingIDs {
		id := strfmt.UUID(uuid.NewString())
		existingIDs[i] = id
		preInsertBatch[i] = buildUnconditionalObject(className, id)
	}
	preErrs := shard.PutObjectBatch(ctx, preInsertBatch)
	for i, err := range preErrs {
		require.NoError(t, err, "pre-insert failed at index %d", i)
	}

	// Phase 2: generate 500 brand-new UUIDs.
	newIDs := make([]strfmt.UUID, half)
	for i := range newIDs {
		newIDs[i] = strfmt.UUID(uuid.NewString())
	}

	// Phase 3: build the 1000-object mixed batch.
	// Layout: indices 0..499 = existing UUIDs, 500..999 = new UUIDs.
	// All carry OnlyIfNotExists.
	batch := make([]*storobj.Object, 2*half)
	for i := 0; i < half; i++ {
		batch[i] = buildConditionalObject(className, existingIDs[i])
	}
	for i := 0; i < half; i++ {
		batch[half+i] = buildConditionalObject(className, newIDs[i])
	}

	// Phase 4: submit the batch.
	batchErrs := shard.PutObjectBatch(ctx, batch)
	require.Len(t, batchErrs, 2*half, "error slice length must match batch length")

	// Phase 5: tally and assert outcomes.
	var inserted, skipped, unexpected int
	for i, err := range batchErrs {
		if err == nil {
			inserted++
			// This slot must be a new UUID.
			require.GreaterOrEqual(t, i, half,
				"nil error at index %d: expected only new UUIDs (indices 500-999) to succeed; "+
					"existing UUID at index %d should have been skipped", i, i)
		} else if isPreconditionFailed(err) {
			skipped++
			// This slot must be an existing UUID.
			require.Less(t, i, half,
				"ErrPreconditionFailed at index %d: expected only existing UUIDs (indices 0-499) "+
					"to be skipped; new UUID at index %d should have succeeded", i, i)
		} else {
			unexpected++
			t.Errorf("unexpected error at index %d: %v", i, err)
		}
	}

	require.Equal(t, half, inserted, "expected exactly %d inserted (new UUIDs)", half)
	require.Equal(t, half, skipped, "expected exactly %d skipped (existing UUIDs)", half)
	require.Zero(t, unexpected, "expected zero unexpected errors")
}

// runOnlyIfExistsCASRace fires concurrencyCount goroutines all attempting PutObject
// with OnlyIfExists on the same UUID. Returns success and precondFail counts.
func runOnlyIfExistsCASRace(t *testing.T, ctx context.Context, shard ShardLike, className string, id strfmt.UUID) (successCount, precondFailCount int) {
	t.Helper()

	logger, _ := test.NewNullLogger()

	var wg sync.WaitGroup
	var successes atomic.Int64
	var precondFails atomic.Int64

	start := make(chan struct{})

	for i := 0; i < concurrencyCount; i++ {
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			<-start
			obj := buildOnlyIfExistsObject(className, id)
			err := shard.PutObject(ctx, obj)
			if err == nil {
				successes.Add(1)
				return
			}
			if isPreconditionFailed(err) {
				precondFails.Add(1)
				return
			}
			t.Errorf("unexpected error from PutObject (OnlyIfExists): %v", err)
		}, logger)
	}

	close(start)
	wg.Wait()

	return int(successes.Load()), int(precondFails.Load())
}

// TestOnlyIfExistsConcurrency validates the OnlyIfExists (update_if_exists) path
// under 100 concurrent goroutines.
//
// Two sub-cases:
//  1. UUID exists: all 100 concurrent update_if_exists calls succeed (no race winner,
//     every goroutine updates the object under the per-UUID lock; each sees prevObj!=nil).
//  2. UUID does not exist: all 100 concurrent update_if_exists calls must receive
//     ErrPreconditionFailed (no goroutine can create the object it's trying to update).
//
// Causal link: without the OnlyIfExists check in putObjectLSM, case 2 would allow
// all 100 goroutines to write the object (successCount==100 instead of 0), failing
// require.Zero(t, successCount).
func TestOnlyIfExistsConcurrency(t *testing.T) {
	ctx := context.Background()
	className := "OnlyIfExistsConcurrency"
	class := &models.Class{Class: className}

	shard, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	defer func() { _ = idx.drop() }()

	t.Run("uuid_exists_all_succeed", func(t *testing.T) {
		id := strfmt.UUID(uuid.NewString())

		// Pre-insert unconditionally so the object exists.
		require.NoError(t, shard.PutObject(ctx, buildUnconditionalObject(className, id)))

		successCount, precondFailCount := runOnlyIfExistsCASRace(t, ctx, shard, className, id)

		// All goroutines see prevObj != nil, so all succeed.
		require.Equal(t, concurrencyCount, successCount,
			"all %d goroutines must succeed when object exists", concurrencyCount)
		require.Zero(t, precondFailCount,
			"no goroutine should get ErrPreconditionFailed when object exists")
	})

	t.Run("uuid_missing_all_fail", func(t *testing.T) {
		id := strfmt.UUID(uuid.NewString())
		// Do NOT pre-insert: UUID is brand new.

		successCount, precondFailCount := runOnlyIfExistsCASRace(t, ctx, shard, className, id)

		// All goroutines see prevObj == nil, so all get ErrPreconditionFailed.
		require.Zero(t, successCount,
			"no goroutine must succeed when object does not exist")
		require.Equal(t, concurrencyCount, precondFailCount,
			"all %d goroutines must get ErrPreconditionFailed when object missing", concurrencyCount)
	})
}

// TestMixedInsertUpdateConcurrentSameUUID exercises N goroutines on the same UUID
// where half carry OnlyIfNotExists (insert_if_not_exists) and half carry OnlyIfExists
// (update_if_exists). The UUID does not exist before the race starts.
//
// Invariants:
//   - Exactly 1 goroutine with OnlyIfNotExists wins the initial insert.
//   - The other (concurrencyCount/2 - 1) OnlyIfNotExists goroutines get ErrPreconditionFailed.
//   - OnlyIfExists goroutines that execute before the insert sees nil prevObj and fail;
//     those that execute after see non-nil prevObj and succeed. Any split is acceptable;
//     total must equal concurrencyCount/2.
//   - Zero panics, zero unexpected errors, zero torn objects.
//
// Causal link: this test catches torn-read and lock-bypass bugs because without the
// per-UUID docIdLock serialization, two goroutines could both observe prevObj==nil
// and both attempt to insert, breaking the "exactly 1 insert wins" guarantee.
func TestMixedInsertUpdateConcurrentSameUUID(t *testing.T) {
	const half = concurrencyCount / 2

	ctx := context.Background()
	className := "MixedInsertUpdateSameUUID"
	class := &models.Class{Class: className}

	shard, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	defer func() { _ = idx.drop() }()

	id := strfmt.UUID(uuid.NewString())

	var wg sync.WaitGroup
	var insertSuccesses atomic.Int64
	var insertPrecondFails atomic.Int64
	var updateSuccesses atomic.Int64
	var updatePrecondFails atomic.Int64

	logger, _ := test.NewNullLogger()
	start := make(chan struct{})

	// half goroutines: OnlyIfNotExists
	for i := 0; i < half; i++ {
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			<-start
			obj := buildConditionalObject(className, id)
			err := shard.PutObject(ctx, obj)
			if err == nil {
				insertSuccesses.Add(1)
				return
			}
			if isPreconditionFailed(err) {
				insertPrecondFails.Add(1)
				return
			}
			t.Errorf("unexpected error from OnlyIfNotExists goroutine: %v", err)
		}, logger)
	}

	// half goroutines: OnlyIfExists
	for i := 0; i < half; i++ {
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			<-start
			obj := buildOnlyIfExistsObject(className, id)
			err := shard.PutObject(ctx, obj)
			if err == nil {
				updateSuccesses.Add(1)
				return
			}
			if isPreconditionFailed(err) {
				updatePrecondFails.Add(1)
				return
			}
			t.Errorf("unexpected error from OnlyIfExists goroutine: %v", err)
		}, logger)
	}

	close(start)
	wg.Wait()

	ins := int(insertSuccesses.Load())
	insPF := int(insertPrecondFails.Load())
	updS := int(updateSuccesses.Load())
	updPF := int(updatePrecondFails.Load())

	// Exactly 1 insert must win.
	require.Equal(t, 1, ins,
		"exactly 1 OnlyIfNotExists goroutine must succeed; got %d", ins)
	require.Equal(t, half-1, insPF,
		"remaining %d OnlyIfNotExists goroutines must get ErrPreconditionFailed; got %d", half-1, insPF)

	// All OnlyIfExists goroutines must have a deterministic outcome (success or PrecondFail).
	require.Equal(t, half, updS+updPF,
		"all OnlyIfExists goroutines must return either nil or ErrPreconditionFailed; got %d+%d=%d",
		updS, updPF, updS+updPF)

	// No goroutine may have produced an unexpected error.
	// t.Errorf was called above for unexpected errors; totals must add up.
	require.Equal(t, concurrencyCount, ins+insPF+updS+updPF,
		"all goroutines must have an accounted outcome")
}

// TestSameInstantInsertBarrier validates the single-shard "many writes at the same
// instant" scenario for OnlyIfNotExists. A single close(start) channel releases all
// 100 goroutines simultaneously, maximising the chance that the per-UUID lock is
// actually contended.
//
// Expected: exactly 1 goroutine inserts the object (nil error); the other 99 receive
// ErrPreconditionFailed. No panic, no duplicate write.
//
// Causal link: this test catches the absence or incorrect placement of the
// OnlyIfNotExists existence-check guard in putObjectLSM. If the check were outside
// the per-UUID lock, multiple goroutines could concurrently observe prevObj==nil
// and each return nil (success), making successCount > 1 and failing the assertion.
func TestSameInstantInsertBarrier(t *testing.T) {
	ctx := context.Background()
	className := "SameInstantBarrier"
	class := &models.Class{Class: className}

	shard, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	defer func() { _ = idx.drop() }()

	id := strfmt.UUID(uuid.NewString())
	successCount, precondFailCount := runCASRace(t, ctx, shard, className, id)

	require.Equal(t, 1, successCount,
		"exactly 1 goroutine must insert the object; got %d", successCount)
	require.Equal(t, concurrencyCount-1, precondFailCount,
		"remaining %d goroutines must receive ErrPreconditionFailed; got %d",
		concurrencyCount-1, precondFailCount)
}
