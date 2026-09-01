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
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Exhaustive single-process recovery-convergence tests for the v2
// inverted-index reindex pipeline. See weaviate/0-weaviate-issues#240.

// fingerprintInvertedBucket returns a (term → sorted []docID) snapshot
// of an inverted/MapCollection searchable bucket. Frequency is dropped;
// per-doc inclusion is enough to catch posting-list divergence.
func fingerprintInvertedBucket(t *testing.T, b *lsmkv.Bucket) map[string][]uint64 {
	t.Helper()
	out := map[string][]uint64{}
	if b == nil {
		return out
	}
	c, err := b.MapCursor()
	require.NoError(t, err)
	defer c.Close()
	for k, pairs := c.First(context.Background()); k != nil; k, pairs = c.Next(context.Background()) {
		term := string(append([]byte(nil), k...))
		ids := make([]uint64, 0, len(pairs))
		for _, p := range pairs {
			require.Lenf(t, p.Key, 8,
				"unexpected pair key length on term %q: want 8 bytes (big-endian docID), got %d",
				term, len(p.Key))
			id := uint64(p.Key[0])<<56 |
				uint64(p.Key[1])<<48 |
				uint64(p.Key[2])<<40 |
				uint64(p.Key[3])<<32 |
				uint64(p.Key[4])<<24 |
				uint64(p.Key[5])<<16 |
				uint64(p.Key[6])<<8 |
				uint64(p.Key[7])
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		out[term] = ids
	}
	return out
}

// newSearchableRetokenizeTask wraps the production
// SearchableRetokenizeStrategy in test scaffolding. Semantic
// migration: swap is driven via RunReindexOnly/RunPrepare/RunSwap on
// each shard, not the inline runtimeSwap used by MapToBlockmax.
func newSearchableRetokenizeTask(t *testing.T, idx *Index, className, propName, targetTokenization, bucketStrategy string) (*ShardReindexTaskGeneric, *testSearchableRetokenizeStrategyWrapper) {
	t.Helper()
	wrapped := &testSearchableRetokenizeStrategyWrapper{
		SearchableRetokenizeStrategy: SearchableRetokenizeStrategy{
			propName:           propName,
			targetTokenization: targetTokenization,
			className:          className,
			bucketStrategy:     bucketStrategy,
			generation:         1,
		},
	}
	task := NewShardReindexTaskGeneric(
		"SearchableRetokenize", idx.logger, wrapped,
		reindexTaskConfig{
			concurrency:                   2,
			memtableOptFactor:             4,
			backupMemtableOptFactor:       1,
			processingDuration:            10 * time.Minute,
			pauseDuration:                 1 * time.Second,
			checkProcessingEveryNoObjects: 1000,
		},
		&UuidKeyParser{}, uuidObjectsIteratorAsync,
	)
	return task, wrapped
}

// testSearchableRetokenizeStrategyWrapper stubs OnMigrationComplete
// (the real strategy is a no-op for searchable — the schema flip is
// done by FilterableRetokenize when it runs second; we don't run that
// here). Same pattern as testMigrationStrategy for MapToBlockmax.
type testSearchableRetokenizeStrategyWrapper struct {
	SearchableRetokenizeStrategy
	migrationCompleted bool
}

func (s *testSearchableRetokenizeStrategyWrapper) OnMigrationComplete(_ context.Context, _ ShardLike) error {
	s.migrationCompleted = true
	return nil
}

// makeConvergenceTestObjects builds n objects whose `title` cycles
// through a 25-token dictionary so each token appears in multiple docs.
func makeConvergenceTestObjects(t *testing.T, n int, className string) []*storobj.Object {
	t.Helper()
	tokens := []string{
		"alpha", "bravo", "charlie", "delta", "echo",
		"foxtrot", "golf", "hotel", "india", "juliett",
		"kilo", "lima", "mike", "november", "oscar",
		"papa", "quebec", "romeo", "sierra", "tango",
		"uniform", "victor", "whiskey", "xray", "yankee",
	}
	out := make([]*storobj.Object, n)
	for i := 0; i < n; i++ {
		text := tokens[i%len(tokens)] + " " + tokens[(i+1)%len(tokens)] + " " + tokens[(i+2)%len(tokens)]
		out[i] = createTestObjectWithText(className, text)
	}
	return out
}

// TestRecoveryConvergence_Baseline fingerprints a clean MapToBlockmax
// migration; other recovery-convergence tests compare against it.
func TestRecoveryConvergence_Baseline(t *testing.T) {
	ctx := testCtx()
	const propName = "title"
	const numObjects = 25

	className := "ConvergenceBaseline_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{propName})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	objects := makeConvergenceTestObjects(t, numObjects, className)
	for _, obj := range objects {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
	preBucket := shard.store.Bucket(bucketName)
	require.NotNil(t, preBucket, "pre-migration searchable bucket must exist")
	require.Equal(t, lsmkv.StrategyMapCollection, preBucket.Strategy())

	strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task := newTestTask(idx.logger, strategy)
	require.NoError(t, task.RunOnShard(ctx, shard))
	require.True(t, strategy.migrationCompleted)

	postBucket := shard.store.Bucket(bucketName)
	require.NotNil(t, postBucket, "post-migration searchable bucket must exist")
	require.Equal(t, lsmkv.StrategyInverted, postBucket.Strategy())

	rt := NewFileReindexTracker(shard.pathLSM(), MigrationDirSearchableMapToBlockmax+genSuffix(1), &UuidKeyParser{})
	require.True(t, rt.IsReindexed())
	require.True(t, rt.IsPrepended())
	require.True(t, rt.IsMerged())
	require.True(t, rt.IsSwapped())
	require.True(t, rt.IsTidied())

	fp := fingerprintInvertedBucket(t, postBucket)
	require.NotEmpty(t, fp, "baseline fingerprint must have at least one term")

	expectedTokens := []string{
		"alpha", "bravo", "charlie", "delta", "echo",
		"foxtrot", "golf", "hotel", "india", "juliett",
		"kilo", "lima", "mike", "november", "oscar",
		"papa", "quebec", "romeo", "sierra", "tango",
		"uniform", "victor", "whiskey", "xray", "yankee",
	}
	for _, tok := range expectedTokens {
		docIDs, ok := fp[tok]
		require.Truef(t, ok, "baseline fingerprint missing token %q", tok)
		require.NotEmptyf(t, docIDs, "baseline fingerprint token %q has empty posting list", tok)
	}
}

// computeBaselineFingerprint runs a clean migration on a throw-away
// shard and returns its post-migration fingerprint. Recovery-from-state
// cases compare against this as ground truth.
func computeBaselineFingerprint(t *testing.T, propName string, numObjects int) map[string][]uint64 {
	t.Helper()
	ctx := testCtx()
	className := "ConvergenceBaselineRef_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{propName})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task := newTestTask(idx.logger, strategy)
	require.NoError(t, task.RunOnShard(ctx, shard))
	require.True(t, strategy.migrationCompleted)

	bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
	return fingerprintInvertedBucket(t, shard.store.Bucket(bucketName))
}

// reindexSentinelState names one of the five on-disk states a migration
// can be interrupted at.
type reindexSentinelState string

const (
	sentinelStateReindexed reindexSentinelState = "IsReindexed"
	sentinelStatePrepended reindexSentinelState = "IsPrepended"
	sentinelStateMerged    reindexSentinelState = "IsMerged"
	sentinelStateSwapped   reindexSentinelState = "IsSwapped"
	sentinelStateTidied    reindexSentinelState = "IsTidied"
)

// allReindexSentinelStates is the canonical iteration order, so failure
// output reads left-to-right along the state machine.
var allReindexSentinelStates = []reindexSentinelState{
	sentinelStateReindexed,
	sentinelStatePrepended,
	sentinelStateMerged,
	sentinelStateSwapped,
	sentinelStateTidied,
}

// expectedSentinelsAt is the sentinel snapshot a shard driven to state s
// must carry. Asserted before the code under test runs, so a broken
// drive-to cannot let a pass mask a missed setup.
func expectedSentinelsAt(s reindexSentinelState) map[string]bool {
	switch s {
	case sentinelStateReindexed:
		return map[string]bool{"reindexed": true, "prepended": false, "merged": false, "swapped": false, "tidied": false}
	case sentinelStatePrepended:
		return map[string]bool{"reindexed": true, "prepended": true, "merged": false, "swapped": false, "tidied": false}
	case sentinelStateMerged:
		return map[string]bool{"reindexed": true, "prepended": true, "merged": true, "swapped": false, "tidied": false}
	case sentinelStateSwapped:
		return map[string]bool{"reindexed": true, "prepended": true, "merged": true, "swapped": true, "tidied": false}
	case sentinelStateTidied:
		return map[string]bool{"reindexed": true, "prepended": true, "merged": true, "swapped": true, "tidied": true}
	}
	return nil
}

// driveToSentinelState drives a fresh shard to state s through the
// production entry points.
//
// IsPrepended and IsSwapped are synthesized rather than driven: each
// lives inside an atomic method (runtimePrepare writes markPrepended and
// markMerged together, runtimeSwap writes markSwapped and markTidied
// together), so the only way to land between the two writes is to drive
// past them and remove the later sentinel. Same scheme PR #11415 uses
// for MapToBlockmax.
func driveToSentinelState(t *testing.T, ctx context.Context, shard *Shard,
	task *ShardReindexTaskGeneric, s reindexSentinelState,
) {
	t.Helper()
	switch s {
	case sentinelStateReindexed:
		require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	case sentinelStateMerged:
		require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
		require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	case sentinelStatePrepended:
		driveToSentinelState(t, ctx, shard, task, sentinelStateMerged)
		removeSentinel(t, task, shard, mergedSentinelFile)
	case sentinelStateTidied:
		require.NoError(t, task.RunOnShard(ctx, shard))
	case sentinelStateSwapped:
		driveToSentinelState(t, ctx, shard, task, sentinelStateTidied)
		removeSentinel(t, task, shard, tidiedSentinelFile)
	default:
		t.Fatalf("unknown sentinel state %q", s)
	}
}

// readReindexSentinels snapshots all five sentinels in one call so an
// assertion failure shows the full state, not a single missed flag.
func readReindexSentinels(rt reindexTracker) map[string]bool {
	return map[string]bool{
		"reindexed": rt.IsReindexed(),
		"prepended": rt.IsPrepended(),
		"merged":    rt.IsMerged(),
		"swapped":   rt.IsSwapped(),
		"tidied":    rt.IsTidied(),
	}
}

// removeSentinel deletes one of the tracker's sentinel files to
// synthesize a crash between two writes that an atomic method makes
// together. Pick the sentinel with [mergedSentinelFile] / [tidiedSentinelFile]
// so the file name stays owned by the tracker.
func removeSentinel(t *testing.T, task *ShardReindexTaskGeneric, shard *Shard,
	pick func(*fileReindexTracker) string,
) {
	t.Helper()
	rt, err := task.newReindexTracker(shard.pathLSM())
	require.NoError(t, err)
	ftr := rt.(*fileReindexTracker)
	name := pick(ftr)
	require.NoError(t, os.Remove(filepath.Join(ftr.config.migrationPath, name)),
		"removing %s to synthesize an interrupted state", name)
}

func mergedSentinelFile(ftr *fileReindexTracker) string { return ftr.config.filenameMerged }

func tidiedSentinelFile(ftr *fileReindexTracker) string { return ftr.config.filenameTidied }

// recoveryConvergenceMatrix is the shared body of the per-strategy
// "recover from each interrupted state" matrices. Only the fixture and
// the target bucket differ per strategy; the restart-and-converge
// procedure is the same for all of them.
//
// K is the fingerprint's key type: a term for the inverted and
// roaring-set buckets, a lexicographic value key for the rangeable one.
type recoveryConvergenceMatrix[K comparable] struct {
	// namePrefix seeds the throw-away collection names.
	namePrefix string
	// buildClass and seedObjects build the pre-migration fixture.
	buildClass  func(className string) *models.Class
	seedObjects func(t *testing.T, ctx context.Context, shard *Shard, className string)
	// buildTask returns a fresh task plus a reader for its strategy
	// wrapper's OnMigrationComplete flag.
	buildTask func(t *testing.T, idx *Index, className string) (*ShardReindexTaskGeneric, func() bool)
	// bucketName is the migration's target bucket and wantStrategy the
	// strategy it must still carry after recovery.
	bucketName   string
	wantStrategy string
	fingerprint  func(t *testing.T, b *lsmkv.Bucket) map[K][]uint64
}

// run computes the clean-migration baseline once, then recovers from
// each of the five interrupted states against it as a subtest.
func (m recoveryConvergenceMatrix[K]) run(t *testing.T) {
	baseline := m.baseline(t)
	require.NotEmpty(t, baseline, "baseline fingerprint must be non-empty")
	for _, state := range allReindexSentinelStates {
		t.Run(m.namePrefix+"_"+string(state), func(t *testing.T) {
			m.runCase(t, state, baseline)
		})
	}
}

// baseline runs one uninterrupted migration on a throw-away shard. Every
// case asserts bit-equal convergence against its fingerprint.
func (m recoveryConvergenceMatrix[K]) baseline(t *testing.T) map[K][]uint64 {
	t.Helper()
	ctx := testCtx()
	className := m.namePrefix + "BaselineRef_" + uuid.NewString()[:8]

	shard, idx := m.newFixture(t, ctx, className, m.buildClass(className))
	defer shard.Shutdown(ctx)

	task, completed := m.buildTask(t, idx, className)
	require.NoError(t, task.RunOnShard(ctx, shard))
	require.True(t, completed(), "baseline migration must run OnMigrationComplete")

	return m.fingerprint(t, shard.store.Bucket(m.bucketName))
}

func (m recoveryConvergenceMatrix[K]) newFixture(
	t *testing.T, ctx context.Context, className string, class *models.Class,
) (*Shard, *Index) {
	t.Helper()
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	m.seedObjects(t, ctx, shard, className)
	return shard, idx
}

func (m recoveryConvergenceMatrix[K]) runCase(
	t *testing.T, state reindexSentinelState, baseline map[K][]uint64,
) {
	ctx := testCtx()
	className := m.namePrefix + "Case_" + uuid.NewString()[:8]
	class := m.buildClass(className)

	shard, idx := m.newFixture(t, ctx, className, class)
	defer shard.Shutdown(ctx)

	task, _ := m.buildTask(t, idx, className)
	driveToSentinelState(t, ctx, shard, task, state)

	// Verify the drive actually landed at the intended on-disk state.
	// Without this guard a broken drive would let recovery from a
	// different state appear to "converge".
	rt, err := task.newReindexTracker(shard.pathLSM())
	require.NoError(t, err)
	drivenTo := readReindexSentinels(rt)
	for name, want := range expectedSentinelsAt(state) {
		assert.Equalf(t, want, drivenTo[name],
			"after driving to %s, sentinel %q: full state %v", state, name, drivenTo)
	}

	// Simulated restart: graceful shutdown, fresh task, then idx.initShard
	// re-runs FinalizeCompletedMigrations → LSM init → OnAfterLsmInit.
	shardName := shard.Name()
	require.NoError(t, shard.Shutdown(ctx))

	task2, completed2 := m.buildTask(t, idx, className)
	idx.shardReindexer = &testShardReindexer{task: task2}

	shd2, err := idx.initShard(ctx, shardName, class, nil, true, true)
	require.NoErrorf(t, err, "shard re-init must succeed (state %s)", state)
	shard2 := shd2.(*Shard)
	defer shard2.Shutdown(ctx)
	idx.shards.Store(shardName, shd2)

	// Relaunch the task the way the provider does after a restart: one
	// RunOnShard drives whatever the previous run left unfinished through
	// to the terminal state.
	require.NoErrorf(t, task2.RunOnShard(ctx, shard2),
		"recovery RunOnShard must not error (state %s)", state)

	rt2, err := task2.newReindexTracker(shard2.pathLSM())
	require.NoErrorf(t, err, "post-recovery tracker init (state %s)", state)
	require.Truef(t, rt2.IsTidied(),
		"recovery must reach the terminal tidied state (state %s)", state)
	require.Truef(t, completed2(),
		"recovery must run OnMigrationComplete (state %s)", state)

	bucket := shard2.store.Bucket(m.bucketName)
	require.NotNilf(t, bucket, "post-recovery bucket %q must exist (state %s)", m.bucketName, state)
	require.Equalf(t, m.wantStrategy, bucket.Strategy(),
		"post-recovery bucket %q must keep strategy %s (state %s)", m.bucketName, m.wantStrategy, state)

	// Compare per key so the failure output names which posting list
	// diverged rather than dumping the whole fingerprint.
	got := m.fingerprint(t, bucket)
	assert.Equalf(t, len(baseline), len(got),
		"post-recovery key count diverges from baseline (state %s)", state)
	for key, wantIDs := range baseline {
		gotIDs, ok := got[key]
		if !ok {
			assert.Failf(t, "missing key",
				"key %v present in baseline but missing post-recovery (state %s)", key, state)
			continue
		}
		assert.Equalf(t, wantIDs, gotIDs,
			"key %v post-recovery doc-id list diverges from baseline (state %s)\n  baseline (%d): %v\n  got      (%d): %v",
			key, state, len(wantIDs), wantIDs, len(gotIDs), gotIDs)
	}
}
