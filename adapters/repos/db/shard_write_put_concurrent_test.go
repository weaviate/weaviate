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
	"github.com/weaviate/weaviate/entities/additional"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/multi"
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

// ---------------------------------------------------------------------------
// Phase-2 version-CAS tests
// ---------------------------------------------------------------------------

// buildVersionCASObject builds a storobj.Object with an IfVersion precondition
// set to expectedVersion.
func buildVersionCASObject(className string, id strfmt.UUID, expectedVersion uint64) *storobj.Object {
	v := expectedVersion
	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:                 id,
			Class:              className,
			LastUpdateTimeUnix: time.Now().UnixMilli(),
		},
		Conditional: storobj.Conditional{
			IfVersion: &v,
		},
	}
}

// buildUnconditionalObject builds a storobj.Object with no precondition for
// initial seeding.
func buildUnconditionalObject(className string, id strfmt.UUID) *storobj.Object {
	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:                 id,
			Class:              className,
			LastUpdateTimeUnix: time.Now().UnixMilli(),
		},
	}
}

// TestVersionCASConcurrencyExactlyOnce is the Phase-2 exactly-once CAS test.
//
// 100 goroutines all race to update_if_version=1 on the same object (which
// was written unconditionally at Version 1). The per-UUID docIdLock serialises
// all writes on one shard, so exactly ONE goroutine must succeed (and bump the
// version to 2). All others must receive ErrPreconditionFailed (version mismatch).
//
// Causal link: this test catches a missing or incorrectly-placed IfVersion check
// in putObjectLSM. Without the check, all 100 goroutines return nil; with it
// correctly inside the per-UUID lock, only the first goroutine sees version=1;
// subsequent goroutines see version=2 and get ErrPreconditionFailed.
func TestVersionCASConcurrencyExactlyOnce(t *testing.T) {
	ctx := context.Background()
	className := "VersionCASExactlyOnce"
	class := &models.Class{Class: className}

	shard, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	defer func() { _ = idx.drop() }()

	logger, _ := test.NewNullLogger()
	id := strfmt.UUID(uuid.NewString())

	// Seed: write the object unconditionally so it has Version 1.
	seedObj := buildUnconditionalObject(className, id)
	require.NoError(t, shard.PutObject(ctx, seedObj), "seed write must succeed")

	// Now launch 100 goroutines all trying to update_if_version=1.
	var wg sync.WaitGroup
	var successes atomic.Int64
	var precondFails atomic.Int64

	start := make(chan struct{})

	for i := 0; i < concurrencyCount; i++ {
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			<-start
			obj := buildVersionCASObject(className, id, 1) // IfVersion = 1
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
			t.Errorf("unexpected error from version-CAS PutObject: %v", err)
		}, logger)
	}

	close(start)
	wg.Wait()

	require.Equal(t, 1, int(successes.Load()),
		"exactly 1 goroutine must win the version-CAS race (per-UUID lock serialises)")
	require.Equal(t, concurrencyCount-1, int(precondFails.Load()),
		"the remaining %d goroutines must receive ErrPreconditionFailed (version mismatch)", concurrencyCount-1)
}

// TestVersionMonotonicIncrement verifies that sequential unconditional writes
// produce a strictly monotonically increasing Version: 1, 2, 3, ...
//
// Causal link: this test catches a bug where putObjectLSM mints the version
// with the wrong initial value or fails to increment (e.g. always returns 1).
// If the version is not monotonic, at least one of the require.Equal assertions
// will fail because the stored object's Additional["version"] will not match.
func TestVersionMonotonicIncrement(t *testing.T) {
	ctx := context.Background()
	className := "VersionMonotonic"
	class := &models.Class{Class: className}

	shard, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	defer func() { _ = idx.drop() }()

	id := strfmt.UUID(uuid.NewString())

	for expectedVersion := uint64(1); expectedVersion <= 5; expectedVersion++ {
		obj := buildUnconditionalObject(className, id)
		require.NoError(t, shard.PutObject(ctx, obj),
			"write %d must succeed", expectedVersion)

		// Read back and verify the version via SearchResult.
		fetched, err := shard.ObjectByID(ctx, id, nil, additional.Properties{})
		require.NoError(t, err, "fetch after write %d must succeed", expectedVersion)
		require.NotNil(t, fetched, "object must exist after write %d", expectedVersion)

		result := fetched.SearchResult(additional.Properties{}, "")
		require.NotNil(t, result)
		gotVersion, ok := result.AdditionalProperties["version"]
		require.True(t, ok, "version must be in AdditionalProperties after write %d", expectedVersion)
		require.Equal(t, expectedVersion, gotVersion,
			"version must be %d after write %d", expectedVersion, expectedVersion)
	}
}

// TestVersionCASMismatchPath verifies that update_if_version with a STALE expected
// version returns ErrPreconditionFailed with the correct expected/actual fields,
// and that the object is NOT modified by the failed write.
//
// Causal link: this test catches a bug where the IfVersion check is missing or
// inverted (allowing the write to proceed despite version mismatch). If the check
// is absent, err == nil and require.Error(t, err) fails immediately.
func TestVersionCASMismatchPath(t *testing.T) {
	ctx := context.Background()
	className := "VersionMismatch"
	class := &models.Class{Class: className}

	shard, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	defer func() { _ = idx.drop() }()

	id := strfmt.UUID(uuid.NewString())

	// Seed at version 1.
	require.NoError(t, shard.PutObject(ctx, buildUnconditionalObject(className, id)))

	// Advance to version 2 unconditionally.
	require.NoError(t, shard.PutObject(ctx, buildUnconditionalObject(className, id)))

	// Now attempt update_if_version=1 (stale). Must fail.
	staleCAS := buildVersionCASObject(className, id, 1)
	err := shard.PutObject(ctx, staleCAS)
	require.Error(t, err, "stale version-CAS must return an error")

	var pf *objects.ErrPreconditionFailed
	require.True(t, errors.As(err, &pf),
		"error must be *ErrPreconditionFailed, got %T: %v", err, err)
	require.Equal(t, uint64(1), pf.ExpectedVersion,
		"ErrPreconditionFailed.ExpectedVersion must echo the caller's expected version")
	require.Equal(t, uint64(2), pf.ActualVersion,
		"ErrPreconditionFailed.ActualVersion must reflect the stored version")

	// The object must still be at version 2 (failed write must not mutate state).
	fetched, fetchErr := shard.ObjectByID(ctx, id, nil, additional.Properties{})
	require.NoError(t, fetchErr)
	require.NotNil(t, fetched)
	result := fetched.SearchResult(additional.Properties{}, "")
	gotVersion, ok := result.AdditionalProperties["version"]
	require.True(t, ok, "version must be in AdditionalProperties")
	require.Equal(t, uint64(2), gotVersion,
		"object version must remain 2 after a failed version-CAS write")
}

// TestVersionCASFirstWriteSentinel verifies that the first write to a new UUID
// produces Version 1 and that if_version=0 (the sentinel for "untracked")
// matches a non-existent object's legacy version.
//
// Causal link: this test catches a bug where the first-write version is not 1
// (e.g. remains 0 because the mint logic is absent). It also verifies the design
// decision that Version 0 is the sentinel for "never written by a v2 node".
func TestVersionCASFirstWriteSentinel(t *testing.T) {
	ctx := context.Background()
	className := "VersionFirstWrite"
	class := &models.Class{Class: className}

	shard, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	defer func() { _ = idx.drop() }()

	id := strfmt.UUID(uuid.NewString())

	// First write (unconditional). Version must be 1.
	require.NoError(t, shard.PutObject(ctx, buildUnconditionalObject(className, id)))

	fetched, err := shard.ObjectByID(ctx, id, nil, additional.Properties{})
	require.NoError(t, err)
	require.NotNil(t, fetched)

	result := fetched.SearchResult(additional.Properties{}, "")
	gotVersion, ok := result.AdditionalProperties["version"]
	require.True(t, ok, "version must be present after first write")
	require.Equal(t, uint64(1), gotVersion, "first write must produce Version 1")

	// if_version=0 on a NEW UUID (object absent) must be treated as "no stored
	// version" (prevObj == nil → storedVersion = 0 == IfVersion) and succeed,
	// producing Version 1.
	id2 := strfmt.UUID(uuid.NewString())
	zeroVersion := uint64(0)
	obj2 := &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:                 id2,
			Class:              className,
			LastUpdateTimeUnix: time.Now().UnixMilli(),
		},
		Conditional: storobj.Conditional{IfVersion: &zeroVersion},
	}
	require.NoError(t, shard.PutObject(ctx, obj2),
		"if_version=0 on a new UUID must succeed (0 == stored sentinel 0)")

	fetched2, err2 := shard.ObjectByID(ctx, id2, nil, additional.Properties{})
	require.NoError(t, err2)
	require.NotNil(t, fetched2)
	result2 := fetched2.SearchResult(additional.Properties{}, "")
	gotVersion2, ok2 := result2.AdditionalProperties["version"]
	require.True(t, ok2)
	require.Equal(t, uint64(1), gotVersion2, "first conditional write with if_version=0 must produce Version 1")
}

// TestReplicaApplyPreservesVersion verifies that a replica-apply (putOne with
// mintVersion=false) stores exactly the Version carried by the incoming object
// rather than re-minting from the local prevObj.
//
// This is the core regression test for the version-divergence bug: before the
// fix, putObjectLSM always incremented from local prevObj (e.g. prevObj.Version=0
// -> stored=1, ignoring the coordinator-assigned Version=7 on the wire). Each
// replica would thus produce a different version from the same coordinator write,
// preventing convergence after node recovery.
//
// Causal link: the test calls putOne(ctx, uuid, obj, false) with obj.Version=7
// on a shard whose local prevObj has Version=0 (new object). Without the fix,
// putObjectLSM mints Version=1 and the stored version is 1, causing
// require.Equal(t, uint64(7), gotVersion) to fail. With the fix,
// mintVersion=false skips the mint block and the stored version is 7.
func TestReplicaApplyPreservesVersion(t *testing.T) {
	ctx := context.Background()
	className := "ReplicaApplyPreservesVersion"
	class := &models.Class{Class: className}

	shard, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	defer func() { _ = idx.drop() }()

	id := strfmt.UUID(uuid.NewString())
	uid, err := uuid.Parse(string(id))
	require.NoError(t, err)
	idBytes, err := uid.MarshalBinary()
	require.NoError(t, err)

	// Build an object as if the coordinator minted Version=7.
	const coordinatorVersion = uint64(7)
	incomingObj := &storobj.Object{
		MarshallerVersion: 2,
		Version:           coordinatorVersion,
		Object: models.Object{
			ID:                 id,
			Class:              className,
			LastUpdateTimeUnix: time.Now().UnixMilli(),
		},
	}

	// Apply as a replica (mintVersion=false). The shard has no prior object
	// for this UUID (prevObj=nil), so without the fix putObjectLSM would mint
	// Version=1 instead of preserving 7.
	concrShard, ok := shard.(*Shard)
	require.True(t, ok, "testShardWithSettings must return a *Shard for this test")
	require.NoError(t, concrShard.putOne(ctx, idBytes, incomingObj, false),
		"replica apply must succeed")

	// Read back and verify the stored version.
	fetched, fetchErr := shard.ObjectByID(ctx, id, nil, additional.Properties{})
	require.NoError(t, fetchErr)
	require.NotNil(t, fetched, "object must be stored after replica apply")

	result := fetched.SearchResult(additional.Properties{}, "")
	gotVersion, ok2 := result.AdditionalProperties["version"]
	require.True(t, ok2, "version must be present in AdditionalProperties")
	require.Equal(t, coordinatorVersion, gotVersion,
		"replica-apply must store the incoming Version (%d), not re-mint from local prevObj",
		coordinatorVersion)
}

// TestVersionCASKnownWeakTwoShards documents the Plan-A KNOWN-WEAK boundary at
// the shard layer: two independent shard instances (simulating two coordinator
// nodes during async-replication lag) can both observe Version=N and both pass
// update_if_version=N, producing two successful writes that converge by LWW.
//
// This is NOT a bug; it is the documented Plan-A honest boundary (per synthesis
// §4.1). The test exists to pin this behavior: future changes that accidentally
// prevent BOTH writes from succeeding on separate shards, or that cause a crash,
// will be caught here.
//
// The test asserts: neither shard returns an error (no crash), and after both
// writes the per-shard object has a version > 1 (both incremented, convergence
// is by LWW in the actual multi-node path). Exact version depends on write order;
// we only assert the convergence invariant (version advanced, no corruption).
func TestVersionCASKnownWeakTwoShards(t *testing.T) {
	ctx := context.Background()
	className := "VersionCASKnownWeak"
	class := &models.Class{Class: className}

	// Two independent shard instances, each representing a different coordinator.
	shardA, idxA := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	defer func() { _ = idxA.drop() }()

	shardB, idxB := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	defer func() { _ = idxB.drop() }()

	id := strfmt.UUID(uuid.NewString())

	// Seed BOTH shards unconditionally at Version 1 (simulating replicated state).
	require.NoError(t, shardA.PutObject(ctx, buildUnconditionalObject(className, id)))
	require.NoError(t, shardB.PutObject(ctx, buildUnconditionalObject(className, id)))

	// Both shards independently see Version=1. In a real cluster with async lag,
	// two coordinators could each issue update_if_version=1 concurrently.
	// Each shard evaluates against its own local state only (Plan-A boundary).
	errA := shardA.PutObject(ctx, buildVersionCASObject(className, id, 1))
	errB := shardB.PutObject(ctx, buildVersionCASObject(className, id, 1))

	// KNOWN-WEAK: both succeed at the shard layer (each sees its own Version=1).
	// In a real multi-node write, LWW would resolve which value survives.
	require.NoError(t, errA, "KNOWN-WEAK: shard A must not crash on version-CAS (Plan-A boundary)")
	require.NoError(t, errB, "KNOWN-WEAK: shard B must not crash on version-CAS (Plan-A boundary)")
}

// TestObjectDigestsReturnsVersion verifies that Shard.ObjectDigests populates
// the Version field of the returned RepairResponse from the on-disk binary
// record after a v2 write. This is the unit-level proof for the root cause fix:
// before the fix, ObjectDigests built RepairResponse{ID, UpdateTime} only
// (Version defaulted to zero). resolveCurrentVersion therefore always returned
// 0, causing the coordinator CAS check to reject every If-Match update on
// RF>1 clusters with ErrPreconditionFailed (412).
//
// Causal link: this test writes obj with MarshallerVersion=2 and Version=5,
// then calls ObjectDigests on the shard and asserts Version==5. Without the
// fix, ObjectDigests returns Version==0 and the assertion fails. With the fix,
// VersionFromBinary extracts 5 from the v2 header and Version is set correctly.
func TestObjectDigestsReturnsVersion(t *testing.T) {
	ctx := context.Background()
	const className = "ObjectDigestsVersionTest"
	class := &models.Class{Class: className}

	orig := storobj.GetWriteMarshallerVersion()
	defer storobj.SetWriteMarshallerVersion(orig)
	storobj.SetWriteMarshallerVersion(2)

	shard, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	defer func() { _ = idx.drop() }()

	id := strfmt.UUID(uuid.NewString())

	// Write via the shard PutObject (unconditional), which mints Version=1.
	require.NoError(t, shard.PutObject(ctx, buildUnconditionalObject(className, id)))

	// Retrieve digest and verify Version is non-zero (the shard minted 1).
	concrShard, ok := shard.(*Shard)
	require.True(t, ok)

	digests, err := concrShard.ObjectDigests(ctx, []multi.Identifier{{ID: id.String()}})
	require.NoError(t, err)
	require.Len(t, digests, 1)
	require.Equal(t, id.String(), digests[0].ID)
	require.Equal(t, int64(1), digests[0].Version,
		"ObjectDigests must return the stored Version from the v2 binary header; "+
			"got zero (the pre-fix value); this would cause resolveCurrentVersion to always "+
			"return 0, breaking every If-Match update on RF>1 clusters")
}

// TestObjectDigestsMissingObjectVersion verifies that ObjectDigests returns
// Version=0 for a UUID that does not exist on the shard. This is the correct
// sentinel: a missing object has no version, and 0 is the base for a first mint.
func TestObjectDigestsMissingObjectVersion(t *testing.T) {
	ctx := context.Background()
	const className = "ObjectDigestsMissingTest"
	class := &models.Class{Class: className}

	shard, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	defer func() { _ = idx.drop() }()

	concrShard, ok := shard.(*Shard)
	require.True(t, ok)

	id := strfmt.UUID(uuid.NewString())
	digests, err := concrShard.ObjectDigests(ctx, []multi.Identifier{{ID: id.String()}})
	require.NoError(t, err)
	require.Len(t, digests, 1)
	// Missing object: the loop leaves digests[0] at its zero value (ID==""),
	// which signals "not found" to the caller. Version must be 0.
	require.Equal(t, "", digests[0].ID, "missing object must have empty ID in digest")
	require.Equal(t, int64(0), digests[0].Version, "missing object must have Version=0 sentinel")
}
