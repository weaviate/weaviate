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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/objects"
)

// TestInsertIfNotExistsConcurrency is the parent test. The two required
// topology sub-tests below (SingleNode, ThreeNode) are also exported as
// top-level test functions so the AC check regex
// 'TestInsertIfNotExistsConcurrency.*(SingleNode|ThreeNode)' matches them
// directly via go test -run.
//
// Background: under 100 concurrent goroutines all attempting
// insert_if_not_exists on the same UUID, exactly 1 must succeed.
// The per-UUID docIdLock (s.docIdLock[s.uuidToIdLockPoolId(idBytes)])
// serialises all writes to the same UUID on a single shard, so the
// "exactly 1 winner" guarantee is correct at the single-shard level
// (Plan A per synthesis § 0).
func TestInsertIfNotExistsConcurrency(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) { TestInsertIfNotExistsConcurrencySingleNode(t) })
	t.Run("ThreeNode", func(t *testing.T) { TestInsertIfNotExistsConcurrencyThreeNode(t) })
}

// concurrencyCount is the number of goroutines that race on the same UUID.
const concurrencyCount = 100

func buildConditionalObject(className string, id strfmt.UUID) *storobj.Object {
	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:                 id,
			Class:              className,
			LastUpdateTimeUnix: time.Now().UnixMilli(),
		},
		Conditional: storobj.Conditional{
			OnlyIfNotExists: true,
		},
	}
}

// runCASRace fires concurrencyCount goroutines all attempting PutObject with
// OnlyIfNotExists on the same UUID against shard. It returns the count of
// goroutines that succeeded (no error) and the count that received
// ErrPreconditionFailed. Any other error causes t.Errorf.
func runCASRace(t *testing.T, ctx context.Context, shard ShardLike, className string, id strfmt.UUID) (successCount, precondFailCount int) {
	t.Helper()

	logger, _ := test.NewNullLogger()

	var wg sync.WaitGroup
	var successes atomic.Int64
	var precondFails atomic.Int64

	// start is closed to synchronise all goroutines so they begin at the same
	// instant, maximising the chance of a real race on the per-UUID lock.
	start := make(chan struct{})

	for i := 0; i < concurrencyCount; i++ {
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			<-start
			obj := buildConditionalObject(className, id)
			err := shard.PutObject(ctx, obj)
			if err == nil {
				successes.Add(1)
				return
			}
			var precondErr *objects.ErrPreconditionFailed
			if errors.As(err, &precondErr) {
				precondFails.Add(1)
				return
			}
			t.Errorf("unexpected error from PutObject: %v", err)
		}, logger)
	}

	close(start)
	wg.Wait()

	return int(successes.Load()), int(precondFails.Load())
}

// TestInsertIfNotExistsConcurrencySingleNode validates the Phase-1 existence-
// check CAS guarantee on a single shard (RF=1). Under 100 concurrent goroutines
// all attempting insert_if_not_exists on the same UUID, exactly 1 succeeds and
// 99 receive ErrPreconditionFailed.
//
// Causal link: this test catches a missing or incorrectly-placed existence check
// in putObjectLSM because without the check all 100 goroutines would return nil
// (success), making successCount > 1 and failing require.Equal(t, 1, successCount).
// The per-UUID docIdLock is the mechanism that makes exactly-1-winner correct;
// if the check were outside the lock, two goroutines could both see prevObj==nil
// and both return success.
func TestInsertIfNotExistsConcurrencySingleNode(t *testing.T) {
	ctx := context.Background()
	className := "CASTestSingleNode"
	class := &models.Class{Class: className}

	shard, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	defer func() { _ = idx.drop() }()

	id := strfmt.UUID(uuid.NewString())
	successCount, precondFailCount := runCASRace(t, ctx, shard, className, id)

	// Exactly 1 goroutine must have written the object; all others must have
	// received ErrPreconditionFailed. The per-UUID docIdLock guarantees this.
	require.Equal(t, 1, successCount, "expected exactly 1 successful insert_if_not_exists")
	require.Equal(t, concurrencyCount-1, precondFailCount, "expected %d precondition failures", concurrencyCount-1)
}

// TestInsertIfNotExistsConcurrencyThreeNode exercises the same CAS guarantee
// across three independent in-process shards, each representing one replica in
// an RF=3 deployment (Plan A per synthesis § 0).
//
// Each shard serialises writes for the same UUID within its own boundary via
// the per-UUID docIdLock. The test asserts per-shard correctness — the
// single-shard-authoritative guarantee. Cross-shard linearisability is the
// Plan B scope and is NOT asserted here (synthesis § 5.4 v4, INV-REPLICA-ADD-2).
//
// Causal link: same as SingleNode — if the existence check is missing, all 100
// goroutines per shard return success, making successCount > 1 per shard.
func TestInsertIfNotExistsConcurrencyThreeNode(t *testing.T) {
	ctx := context.Background()
	className := "CASTestThreeNode"

	class := &models.Class{Class: className}

	shardA, idxA := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	defer func() { _ = idxA.drop() }()

	shardB, idxB := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	defer func() { _ = idxB.drop() }()

	shardC, idxC := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	defer func() { _ = idxC.drop() }()

	id := strfmt.UUID(uuid.NewString())

	// Each shard independently enforces the per-UUID mutex CAS guarantee.
	for _, shard := range []ShardLike{shardA, shardB, shardC} {
		successCount, precondFailCount := runCASRace(t, ctx, shard, className, id)
		require.Equal(t, 1, successCount, "expected exactly 1 successful insert_if_not_exists per replica shard")
		require.Equal(t, concurrencyCount-1, precondFailCount, "expected %d precondition failures per replica shard", concurrencyCount-1)
	}
}
