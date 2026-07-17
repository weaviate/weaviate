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
	"testing"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/filters"
	entinverted "github.com/weaviate/weaviate/entities/inverted"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Regression pin: a live write landing between Store.SwapBucketPointer
// (which deletes bucketsByName[ingestName]) and runtimeSwap's
// disableCallbacks resolved the ingest bucket to nil and panicked on
// bucket.Strategy() (weaviate/weaviate#12206). These tests arrange that
// exact swap-window state directly and fire the callback as a concurrent
// live write would.

// setupRangeableSwapRaceShard builds a throwaway shard for the swap-race
// tests below. Returns the shard and a cleanup func.
func setupRangeableSwapRaceShard(t *testing.T) (*Shard, context.Context) {
	t.Helper()
	ctx := testCtx()
	className := "RangeableSwapRace_" + uuid.NewString()[:8]
	class := newFilterableToRangeableTestClass(className)

	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	t.Cleanup(func() { shard.Shutdown(ctx) })
	return shard, ctx
}

// rangeQueryKey mirrors filterableToRangeableFingerprint's key encoding: the
// same LexicographicallySortableInt64 -> BigEndian-uint64 round trip
// WriteToReindexBucket / addToPropertyRangeBucket use for storage.
func rangeQueryKey(t *testing.T, value int64) (itemData []byte, lookupKey uint64) {
	t.Helper()
	lex, err := entinverted.LexicographicallySortableInt64(value)
	require.NoError(t, err)
	return lex, binary.BigEndian.Uint64(lex)
}

// readRangeableDocIDs queries a RoaringSetRange bucket for the exact-match
// posting list at lookupKey. Returns nil if there is no match.
func readRangeableDocIDs(t *testing.T, ctx context.Context, b *lsmkv.Bucket, lookupKey uint64) []uint64 {
	t.Helper()
	reader := b.ReaderRoaringSetRange()
	defer reader.Close()
	bm, release, err := reader.Read(ctx, lookupKey, filters.OperatorEqual)
	require.NoError(t, err)
	defer func() {
		if release != nil {
			release()
		}
	}()
	if bm == nil {
		return nil
	}
	return bm.ToArray()
}

// TestFilterableToRangeableStrategy_MakeAddCallback_SurvivesSwapWindow pins
// the primary fix: a live ADD write landing on a property whose
// SwapBucketPointer has already run (ingest name gone from the store) must
// not panic, and the posting must land in the bucket now reachable at the
// canonical name - not be silently dropped.
func TestFilterableToRangeableStrategy_MakeAddCallback_SurvivesSwapWindow(t *testing.T) {
	shard, ctx := setupRangeableSwapRaceShard(t)

	const propName = filterableToRangeablePropName
	strategy := &FilterableToRangeableStrategy{propNames: []string{propName}, generation: 1}
	mainName := strategy.SourceBucketName(propName)
	ingestName := mainName + strategy.IngestSuffix()

	// Arrange the exact post-Phase-2a state: both buckets exist, then
	// SwapBucketPointer runs (the real production call, not a mock) so
	// mainName resolves to what ingestName used to resolve to, and
	// ingestName is gone from the store's bucket map.
	rangeOpts := shard.makeDefaultBucketOptions(lsmkv.StrategyRoaringSetRange)
	require.NoError(t, shard.store.CreateOrLoadBucket(ctx, mainName, rangeOpts...))
	require.NoError(t, shard.store.CreateOrLoadBucket(ctx, ingestName, rangeOpts...))
	_, err := shard.store.SwapBucketPointer(ctx, mainName, ingestName)
	require.NoError(t, err)
	require.Nil(t, shard.store.Bucket(ingestName),
		"precondition: ingest name must be gone from the store after SwapBucketPointer")
	require.NotNil(t, shard.store.Bucket(mainName),
		"precondition: canonical name must still resolve after SwapBucketPointer")

	// bucketNamer mirrors ShardReindexTaskGeneric.ingestBucketName exactly:
	// SourceBucketName + IngestSuffix.
	bucketNamer := func(p string) string { return strategy.SourceBucketName(p) + strategy.IngestSuffix() }
	propsByName := map[string]struct{}{propName: {}}
	cb := strategy.MakeAddCallback(bucketNamer, propsByName, true)

	itemData, lookupKey := rangeQueryKey(t, 42)
	const docID = uint64(7)
	property := &inverted.Property{
		Name:  propName,
		Items: []inverted.Countable{{Data: itemData}},
	}

	var cbErr error
	assert.NotPanics(t, func() {
		cbErr = cb(shard, docID, property)
	}, "ADD callback must not panic when the ingest bucket name has already "+
		"been swapped away - it must fall back to the canonical bucket name")
	require.NoError(t, cbErr)

	// This is the causal check: the posting must be readable from the
	// bucket now reachable at the canonical name - not silently dropped.
	ids := readRangeableDocIDs(t, ctx, shard.store.Bucket(mainName), lookupKey)
	require.Contains(t, ids, docID,
		"the docID must land in the canonical bucket post-swap, not be lost")
}

// TestFilterableToRangeableStrategy_MakeDeleteCallback_SurvivesSwapWindow is
// the delete-side sibling of the ADD test above - same swap-window
// arrangement, same fallback expectation.
func TestFilterableToRangeableStrategy_MakeDeleteCallback_SurvivesSwapWindow(t *testing.T) {
	shard, ctx := setupRangeableSwapRaceShard(t)

	const propName = filterableToRangeablePropName
	strategy := &FilterableToRangeableStrategy{propNames: []string{propName}, generation: 1}
	mainName := strategy.SourceBucketName(propName)
	ingestName := mainName + strategy.IngestSuffix()

	rangeOpts := shard.makeDefaultBucketOptions(lsmkv.StrategyRoaringSetRange)
	require.NoError(t, shard.store.CreateOrLoadBucket(ctx, mainName, rangeOpts...))
	require.NoError(t, shard.store.CreateOrLoadBucket(ctx, ingestName, rangeOpts...))

	itemData, lookupKey := rangeQueryKey(t, 99)
	const docID = uint64(11)
	// Pre-seed the posting directly on what will become the canonical
	// bucket, exactly as if it were written during the backfill before
	// the swap ran.
	require.NoError(t, shard.store.Bucket(ingestName).RoaringSetRangeAdd(lookupKey, docID))

	_, err := shard.store.SwapBucketPointer(ctx, mainName, ingestName)
	require.NoError(t, err)
	require.Nil(t, shard.store.Bucket(ingestName))

	preIDs := readRangeableDocIDs(t, ctx, shard.store.Bucket(mainName), lookupKey)
	require.Contains(t, preIDs, docID, "precondition: docID must be present before delete")

	bucketNamer := func(p string) string { return strategy.SourceBucketName(p) + strategy.IngestSuffix() }
	propsByName := map[string]struct{}{propName: {}}
	cb := strategy.MakeDeleteCallback(bucketNamer, propsByName, true)

	property := &inverted.Property{
		Name:  propName,
		Items: []inverted.Countable{{Data: itemData}},
	}

	var cbErr error
	assert.NotPanics(t, func() {
		cbErr = cb(shard, docID, property)
	}, "DELETE callback must not panic when the ingest bucket name has "+
		"already been swapped away - it must fall back to the canonical "+
		"bucket name")
	require.NoError(t, cbErr)

	postIDs := readRangeableDocIDs(t, ctx, shard.store.Bucket(mainName), lookupKey)
	require.NotContains(t, postIDs, docID,
		"the docID must actually be removed from the canonical bucket, not silently no-op'd")
}

// TestFilterableToRangeableStrategy_MakeAddCallback_BothBucketsMissing pins
// that when neither the ingest nor canonical bucket resolves, the callback
// skips without panicking or erroring (resolveScopedDoubleWriteBucket's
// uniform skip contract), and resolveDoubleWriteBucket logs a Warn so the
// gap stays visible to operators.
func TestFilterableToRangeableStrategy_MakeAddCallback_BothBucketsMissing(t *testing.T) {
	shard, _ := setupRangeableSwapRaceShard(t)
	logger, ok := shard.index.logger.(*logrus.Logger)
	require.True(t, ok, "test helper wires a *logrus.Logger; if this changes, wire a hook differently")
	hook := test.NewLocal(logger)

	const propName = filterableToRangeablePropName
	strategy := &FilterableToRangeableStrategy{propNames: []string{propName}, generation: 1}
	// Deliberately create neither the ingest bucket nor the canonical
	// bucket - both name lookups inside the callback will miss.

	bucketNamer := func(p string) string { return strategy.SourceBucketName(p) + strategy.IngestSuffix() }
	propsByName := map[string]struct{}{propName: {}}
	cb := strategy.MakeAddCallback(bucketNamer, propsByName, true)

	itemData, _ := rangeQueryKey(t, 5)
	property := &inverted.Property{
		Name:  propName,
		Items: []inverted.Countable{{Data: itemData}},
	}

	var cbErr error
	assert.NotPanics(t, func() {
		cbErr = cb(shard, uint64(1), property)
	}, "ADD callback must not panic even when neither bucket name resolves")
	require.NoError(t, cbErr,
		"a genuinely missing bucket is skipped like any other double-write "+
			"miss (resolveScopedDoubleWriteBucket's uniform skip contract) - "+
			"it must not fail the live write")

	// Causal check: the skip must not be silent-and-invisible. This is
	// what makes the skip loss-free in practice - an operator can grep
	// logs for it even though the write itself succeeds.
	entry := hook.LastEntry()
	require.NotNil(t, entry, "resolveDoubleWriteBucket must log when neither bucket name resolves")
	assert.Equal(t, logrus.WarnLevel, entry.Level)
	assert.Contains(t, entry.Message, "double-write mirror skipped")
}
