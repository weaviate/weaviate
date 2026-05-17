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
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestShardCallbacks_AddToPropertyValueIndex(t *testing.T) {
	t.Run("callback fires", func(t *testing.T) {
		s := &Shard{}
		var called atomic.Bool
		s.registerAddToPropertyValueIndex(func(_ *Shard, _ uint64, _ *inverted.Property) error {
			called.Store(true)
			return nil
		})

		err := s.onAddToPropertyValueIndex(1, &inverted.Property{Name: "test"})
		require.NoError(t, err)
		assert.True(t, called.Load())
	})

	t.Run("deregister disables callback", func(t *testing.T) {
		s := &Shard{}
		var callCount atomic.Int32
		deregister := s.registerAddToPropertyValueIndex(func(_ *Shard, _ uint64, _ *inverted.Property) error {
			callCount.Add(1)
			return nil
		})

		// Fires before deregister.
		err := s.onAddToPropertyValueIndex(1, &inverted.Property{Name: "test"})
		require.NoError(t, err)
		assert.Equal(t, int32(1), callCount.Load())

		deregister()

		// Does not fire after deregister.
		err = s.onAddToPropertyValueIndex(1, &inverted.Property{Name: "test"})
		require.NoError(t, err)
		assert.Equal(t, int32(1), callCount.Load())
	})

	t.Run("deregister is idempotent", func(t *testing.T) {
		s := &Shard{}
		deregister := s.registerAddToPropertyValueIndex(func(_ *Shard, _ uint64, _ *inverted.Property) error {
			return nil
		})

		deregister()
		deregister() // Must not panic.
	})

	t.Run("deregister one leaves others active", func(t *testing.T) {
		s := &Shard{}
		var calls [3]atomic.Int32

		deregister0 := s.registerAddToPropertyValueIndex(func(_ *Shard, _ uint64, _ *inverted.Property) error {
			calls[0].Add(1)
			return nil
		})
		s.registerAddToPropertyValueIndex(func(_ *Shard, _ uint64, _ *inverted.Property) error {
			calls[1].Add(1)
			return nil
		})
		s.registerAddToPropertyValueIndex(func(_ *Shard, _ uint64, _ *inverted.Property) error {
			calls[2].Add(1)
			return nil
		})

		// All fire initially.
		err := s.onAddToPropertyValueIndex(1, &inverted.Property{Name: "test"})
		require.NoError(t, err)
		assert.Equal(t, int32(1), calls[0].Load())
		assert.Equal(t, int32(1), calls[1].Load())
		assert.Equal(t, int32(1), calls[2].Load())

		// Deregister the first callback only.
		deregister0()

		err = s.onAddToPropertyValueIndex(1, &inverted.Property{Name: "test"})
		require.NoError(t, err)
		assert.Equal(t, int32(1), calls[0].Load(), "deregistered callback must not fire again")
		assert.Equal(t, int32(2), calls[1].Load(), "other callback must still fire")
		assert.Equal(t, int32(2), calls[2].Load(), "other callback must still fire")
	})

	t.Run("callback error propagates", func(t *testing.T) {
		s := &Shard{}
		s.registerAddToPropertyValueIndex(func(_ *Shard, _ uint64, _ *inverted.Property) error {
			return fmt.Errorf("boom")
		})

		err := s.onAddToPropertyValueIndex(1, &inverted.Property{Name: "test"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "boom")
	})
}

func TestShardCallbacks_DeleteFromPropertyValueIndex(t *testing.T) {
	t.Run("callback fires", func(t *testing.T) {
		s := &Shard{}
		var called atomic.Bool
		s.registerDeleteFromPropertyValueIndex(func(_ *Shard, _ uint64, _ *inverted.Property) error {
			called.Store(true)
			return nil
		})

		err := s.onDeleteFromPropertyValueIndex(1, &inverted.Property{Name: "test"})
		require.NoError(t, err)
		assert.True(t, called.Load())
	})

	t.Run("deregister disables callback", func(t *testing.T) {
		s := &Shard{}
		var callCount atomic.Int32
		deregister := s.registerDeleteFromPropertyValueIndex(func(_ *Shard, _ uint64, _ *inverted.Property) error {
			callCount.Add(1)
			return nil
		})

		err := s.onDeleteFromPropertyValueIndex(1, &inverted.Property{Name: "test"})
		require.NoError(t, err)
		assert.Equal(t, int32(1), callCount.Load())

		deregister()

		err = s.onDeleteFromPropertyValueIndex(1, &inverted.Property{Name: "test"})
		require.NoError(t, err)
		assert.Equal(t, int32(1), callCount.Load())
	})

	t.Run("deregister one leaves others active", func(t *testing.T) {
		s := &Shard{}
		var calls [2]atomic.Int32

		deregister0 := s.registerDeleteFromPropertyValueIndex(func(_ *Shard, _ uint64, _ *inverted.Property) error {
			calls[0].Add(1)
			return nil
		})
		s.registerDeleteFromPropertyValueIndex(func(_ *Shard, _ uint64, _ *inverted.Property) error {
			calls[1].Add(1)
			return nil
		})

		err := s.onDeleteFromPropertyValueIndex(1, &inverted.Property{Name: "test"})
		require.NoError(t, err)
		assert.Equal(t, int32(1), calls[0].Load())
		assert.Equal(t, int32(1), calls[1].Load())

		deregister0()

		err = s.onDeleteFromPropertyValueIndex(1, &inverted.Property{Name: "test"})
		require.NoError(t, err)
		assert.Equal(t, int32(1), calls[0].Load())
		assert.Equal(t, int32(2), calls[1].Load())
	})
}

// TestShardCallbacks_DualWriteMigration simulates the lifecycle of a
// dual-write migration: writes go to a primary bucket, then a callback is
// registered so writes are duplicated to a secondary bucket, then the
// callback is deregistered. Only writes during the dual-write window should
// appear in the secondary bucket.
func TestShardCallbacks_DualWriteMigration(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	dirName := t.TempDir()
	store, err := lsmkv.New(dirName, dirName, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	defer store.Shutdown(ctx)

	const propName = "title"
	primaryBucket := helpers.BucketFromPropNameLSM(propName)
	secondaryBucket := primaryBucket + "_secondary"

	// Create both buckets with RoaringSet strategy (same as filterable index).
	err = store.CreateOrLoadBucket(ctx, primaryBucket,
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet))
	require.NoError(t, err)
	err = store.CreateOrLoadBucket(ctx, secondaryBucket,
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet))
	require.NoError(t, err)

	s := &Shard{store: store}

	// Helper: write a property with HasFilterableIndex to the primary bucket
	// (via addToPropertyValueIndex) which also triggers callbacks.
	writeDoc := func(t *testing.T, docID uint64, term string) {
		t.Helper()
		err := s.addToPropertyValueIndex(docID, inverted.Property{
			Name:               propName,
			Items:              []inverted.Countable{{Data: []byte(term)}},
			HasFilterableIndex: true,
		})
		require.NoError(t, err)
	}

	// Helper: check which docIDs are present in a bucket for a given term.
	readDocIDs := func(t *testing.T, bucketName, term string) []uint64 {
		t.Helper()
		b := store.Bucket(bucketName)
		require.NotNil(t, b, "bucket %q must exist", bucketName)
		bm, release, err := b.RoaringSetGet([]byte(term))
		require.NoError(t, err)
		defer release()
		if bm == nil {
			return nil
		}
		return bm.ToArray()
	}

	// --- Phase 1: before dual-write ---
	// docIDs 1-3 go only to the primary bucket.
	for docID := uint64(1); docID <= 3; docID++ {
		writeDoc(t, docID, "hello")
	}

	// --- Register dual-write callback ---
	// The callback mirrors filterable writes to the secondary bucket,
	// similar to what duplicateToBuckets does in the blockmax migrator.
	deregister := s.registerAddToPropertyValueIndex(
		func(shard *Shard, docID uint64, property *inverted.Property) error {
			if !property.HasFilterableIndex {
				return nil
			}
			secondary := shard.store.Bucket(helpers.BucketFromPropNameLSM(property.Name) + "_secondary")
			if secondary == nil {
				return nil
			}
			for _, item := range property.Items {
				if err := secondary.RoaringSetAddOne(item.Data, docID); err != nil {
					return err
				}
			}
			return nil
		})

	// --- Phase 2: during dual-write ---
	// docIDs 4-6 should appear in both buckets.
	for docID := uint64(4); docID <= 6; docID++ {
		writeDoc(t, docID, "hello")
	}

	// --- Deregister the callback ---
	deregister()

	// --- Phase 3: after dual-write ---
	// docIDs 7-9 go only to the primary bucket again.
	for docID := uint64(7); docID <= 9; docID++ {
		writeDoc(t, docID, "hello")
	}

	// --- Verify ---
	primaryIDs := readDocIDs(t, primaryBucket, "hello")
	secondaryIDs := readDocIDs(t, secondaryBucket, "hello")

	assert.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9}, primaryIDs,
		"primary bucket must contain all docIDs (phases 1-3)")
	assert.Equal(t, []uint64{4, 5, 6}, secondaryIDs,
		"secondary bucket must contain only phase 2 docIDs (during dual-write)")
}

// TestShardCallbacks_ConcurrentRegistrationAndWrites verifies that
// registering a callback while another goroutine is invoking callbacks
// does not race. Run with -race to verify.
func TestShardCallbacks_ConcurrentRegistrationAndWrites(t *testing.T) {
	s := &Shard{}

	// Pre-register a callback so the slice is non-empty from the start.
	var baseCount atomic.Int64
	s.registerAddToPropertyValueIndex(func(_ *Shard, _ uint64, _ *inverted.Property) error {
		baseCount.Add(1)
		return nil
	})

	const (
		numWriters       = 4
		writesPerWriter  = 500
		numRegistrations = 20
	)

	var wg sync.WaitGroup

	// Writers: continuously invoke callbacks.
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < writesPerWriter; i++ {
				_ = s.onAddToPropertyValueIndex(uint64(i), &inverted.Property{Name: "p"})
			}
		}()
	}

	// Registrations: add and deregister callbacks concurrently with writes.
	for r := 0; r < numRegistrations; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dereg := s.registerAddToPropertyValueIndex(
				func(_ *Shard, _ uint64, _ *inverted.Property) error {
					return nil
				})
			dereg()
		}()
	}

	wg.Wait()

	// The base callback must have been invoked at least once (sanity).
	assert.Greater(t, baseCount.Load(), int64(0))
}
