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

package hnsw

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
)

func TestCleanupCheckpoint_EncodeDecode(t *testing.T) {
	t.Run("basic encode/decode roundtrip", func(t *testing.T) {
		original := &CleanupCheckpoint{
			Version:      cleanupCheckpointVersion,
			GenerationID: 42,
			TombstoneIDs: []uint64{100, 200, 300, 500, 1000},
			TotalNodes:   1000000,
			Watermark:    500000,
			StartedAt:    time.Now().Truncate(time.Nanosecond),
			UpdatedAt:    time.Now().Truncate(time.Nanosecond),
		}

		data := original.encode()
		decoded, err := decodeCheckpoint(data)

		require.NoError(t, err)
		assert.Equal(t, original.Version, decoded.Version)
		assert.Equal(t, original.GenerationID, decoded.GenerationID)
		assert.Equal(t, original.TombstoneIDs, decoded.TombstoneIDs)
		assert.Equal(t, original.TotalNodes, decoded.TotalNodes)
		assert.Equal(t, original.Watermark, decoded.Watermark)
		assert.Equal(t, original.StartedAt.UnixNano(), decoded.StartedAt.UnixNano())
		assert.Equal(t, original.UpdatedAt.UnixNano(), decoded.UpdatedAt.UnixNano())
	})

	t.Run("empty tombstone list", func(t *testing.T) {
		original := &CleanupCheckpoint{
			Version:      cleanupCheckpointVersion,
			GenerationID: 1,
			TombstoneIDs: []uint64{},
			TotalNodes:   100,
			Watermark:    0,
			StartedAt:    time.Now(),
			UpdatedAt:    time.Now(),
		}

		data := original.encode()
		decoded, err := decodeCheckpoint(data)

		require.NoError(t, err)
		assert.Equal(t, 0, len(decoded.TombstoneIDs))
	})

	t.Run("large tombstone list", func(t *testing.T) {
		tombstones := make([]uint64, 100000)
		for i := range tombstones {
			tombstones[i] = uint64(i * 2)
		}

		original := &CleanupCheckpoint{
			Version:      cleanupCheckpointVersion,
			GenerationID: 999,
			TombstoneIDs: tombstones,
			TotalNodes:   10000000,
			Watermark:    5000000,
			StartedAt:    time.Now(),
			UpdatedAt:    time.Now(),
		}

		data := original.encode()
		decoded, err := decodeCheckpoint(data)

		require.NoError(t, err)
		assert.Equal(t, len(tombstones), len(decoded.TombstoneIDs))
		assert.Equal(t, tombstones, decoded.TombstoneIDs)
	})
}

func TestCleanupCheckpoint_ChecksumValidation(t *testing.T) {
	t.Run("valid checksum passes", func(t *testing.T) {
		cp := &CleanupCheckpoint{
			Version:      cleanupCheckpointVersion,
			GenerationID: 1,
			TombstoneIDs: []uint64{1, 2, 3},
			TotalNodes:   100,
			Watermark:    50,
			StartedAt:    time.Now(),
			UpdatedAt:    time.Now(),
		}

		data := cp.encode()
		_, err := decodeCheckpoint(data)
		require.NoError(t, err)
	})

	t.Run("corrupted data fails checksum", func(t *testing.T) {
		cp := &CleanupCheckpoint{
			Version:      cleanupCheckpointVersion,
			GenerationID: 1,
			TombstoneIDs: []uint64{1, 2, 3},
			TotalNodes:   100,
			Watermark:    50,
			StartedAt:    time.Now(),
			UpdatedAt:    time.Now(),
		}

		data := cp.encode()

		// Corrupt a byte in the middle
		data[len(data)/2] ^= 0xFF

		_, err := decodeCheckpoint(data)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "checksum mismatch")
	})

	t.Run("truncated data fails", func(t *testing.T) {
		cp := &CleanupCheckpoint{
			Version:      cleanupCheckpointVersion,
			GenerationID: 1,
			TombstoneIDs: []uint64{1, 2, 3},
			TotalNodes:   100,
			Watermark:    50,
			StartedAt:    time.Now(),
			UpdatedAt:    time.Now(),
		}

		data := cp.encode()

		// Truncate the data
		_, err := decodeCheckpoint(data[:len(data)-10])
		require.Error(t, err)
	})

	t.Run("invalid magic fails", func(t *testing.T) {
		cp := &CleanupCheckpoint{
			Version:      cleanupCheckpointVersion,
			GenerationID: 1,
			TombstoneIDs: []uint64{1, 2, 3},
			TotalNodes:   100,
			Watermark:    50,
			StartedAt:    time.Now(),
			UpdatedAt:    time.Now(),
		}

		data := cp.encode()

		// Corrupt the magic number
		data[0] = 0x00
		data[1] = 0x00
		data[2] = 0x00
		data[3] = 0x00

		_, err := decodeCheckpoint(data)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid checkpoint magic")
	})
}

func TestCleanupCheckpoint_FilePersistence(t *testing.T) {
	tmpDir := t.TempDir()

	t.Run("save and load checkpoint", func(t *testing.T) {
		path := filepath.Join(tmpDir, "test.checkpoint")

		original := &CleanupCheckpoint{
			Version:      cleanupCheckpointVersion,
			GenerationID: 123,
			TombstoneIDs: []uint64{10, 20, 30},
			TotalNodes:   1000,
			Watermark:    500,
			StartedAt:    time.Now().Truncate(time.Nanosecond),
			UpdatedAt:    time.Now().Truncate(time.Nanosecond),
		}

		// Use OS directly for file tests
		err := saveCleanupCheckpointOS(path, original)
		require.NoError(t, err)

		loaded, err := loadCleanupCheckpointOS(path)
		require.NoError(t, err)
		require.NotNil(t, loaded)

		assert.Equal(t, original.GenerationID, loaded.GenerationID)
		assert.Equal(t, original.TombstoneIDs, loaded.TombstoneIDs)
		assert.Equal(t, original.Watermark, loaded.Watermark)
	})

	t.Run("load missing checkpoint returns nil", func(t *testing.T) {
		path := filepath.Join(tmpDir, "nonexistent.checkpoint")

		loaded, err := loadCleanupCheckpointOS(path)
		require.NoError(t, err)
		assert.Nil(t, loaded)
	})

	t.Run("delete checkpoint", func(t *testing.T) {
		path := filepath.Join(tmpDir, "delete_test.checkpoint")

		cp := &CleanupCheckpoint{
			Version:      cleanupCheckpointVersion,
			GenerationID: 1,
			TombstoneIDs: []uint64{1},
			TotalNodes:   10,
			Watermark:    5,
			StartedAt:    time.Now(),
			UpdatedAt:    time.Now(),
		}

		err := saveCleanupCheckpointOS(path, cp)
		require.NoError(t, err)

		// Verify it exists
		_, err = os.Stat(path)
		require.NoError(t, err)

		// Delete it
		err = deleteCleanupCheckpointOS(path)
		require.NoError(t, err)

		// Verify it's gone
		_, err = os.Stat(path)
		assert.True(t, os.IsNotExist(err))
	})

	t.Run("delete nonexistent checkpoint succeeds", func(t *testing.T) {
		path := filepath.Join(tmpDir, "nonexistent_delete.checkpoint")

		err := deleteCleanupCheckpointOS(path)
		require.NoError(t, err)
	})
}

// Helper functions that use os directly for testing
func saveCleanupCheckpointOS(path string, cp *CleanupCheckpoint) error {
	cp.UpdatedAt = time.Now()
	data := cp.encode()
	tempPath := path + ".tmp"
	if err := os.WriteFile(tempPath, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tempPath, path)
}

func loadCleanupCheckpointOS(path string) (*CleanupCheckpoint, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return decodeCheckpoint(data)
}

func deleteCleanupCheckpointOS(path string) error {
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func TestProgressTracker_BasicOperations(t *testing.T) {
	t.Run("claim returns sequential IDs", func(t *testing.T) {
		tracker := newProgressTracker(0, 10)

		for i := uint64(0); i < 10; i++ {
			id, ok := tracker.claim()
			assert.True(t, ok)
			assert.Equal(t, i, id)
		}

		// No more work
		_, ok := tracker.claim()
		assert.False(t, ok)
	})

	t.Run("claim with startFrom", func(t *testing.T) {
		tracker := newProgressTracker(5, 10)

		id, ok := tracker.claim()
		assert.True(t, ok)
		assert.Equal(t, uint64(5), id)

		id, ok = tracker.claim()
		assert.True(t, ok)
		assert.Equal(t, uint64(6), id)
	})

	t.Run("complete removes from inFlight", func(t *testing.T) {
		tracker := newProgressTracker(0, 10)

		id, _ := tracker.claim()
		_, total, inFlight := tracker.getProgress()
		assert.Equal(t, uint64(10), total)
		assert.Equal(t, uint64(1), inFlight)

		tracker.complete(id)
		_, _, inFlight = tracker.getProgress()
		assert.Equal(t, uint64(0), inFlight)
	})
}

func TestProgressTracker_WatermarkCalculation(t *testing.T) {
	t.Run("empty tracker has watermark 0", func(t *testing.T) {
		tracker := newProgressTracker(0, 10)
		assert.Equal(t, uint64(0), tracker.getSafeWatermark())
	})

	t.Run("all complete gives highest watermark", func(t *testing.T) {
		tracker := newProgressTracker(0, 5)

		// Claim and complete all
		for i := 0; i < 5; i++ {
			id, _ := tracker.claim()
			tracker.complete(id)
		}

		assert.Equal(t, uint64(4), tracker.getSafeWatermark())
	})

	t.Run("watermark respects in-flight", func(t *testing.T) {
		tracker := newProgressTracker(0, 10)

		// Claim 0, 1, 2, 3, 4
		ids := make([]uint64, 5)
		for i := 0; i < 5; i++ {
			ids[i], _ = tracker.claim()
		}

		// Complete 0, 1, 3, 4 (skip 2)
		tracker.complete(0)
		tracker.complete(1)
		tracker.complete(3)
		tracker.complete(4)

		// Watermark should be 1 (2 is still in-flight)
		assert.Equal(t, uint64(1), tracker.getSafeWatermark())

		// Complete 2
		tracker.complete(2)

		// Now watermark should be 4
		assert.Equal(t, uint64(4), tracker.getSafeWatermark())
	})

	t.Run("first ID in-flight gives watermark 0", func(t *testing.T) {
		tracker := newProgressTracker(0, 10)

		// Claim 0, 1, 2
		tracker.claim() // 0
		tracker.claim() // 1
		tracker.claim() // 2

		// Complete 1, 2 but not 0
		tracker.complete(1)
		tracker.complete(2)

		// Watermark must be 0 (ID 0 is in-flight, can't go below 0)
		assert.Equal(t, uint64(0), tracker.getSafeWatermark())
	})
}

func TestProgressTracker_ConcurrentAccess(t *testing.T) {
	t.Run("concurrent claim is safe", func(t *testing.T) {
		tracker := newProgressTracker(0, 10000)

		var wg sync.WaitGroup
		claimed := make(chan uint64, 10000)

		// 10 workers claiming concurrently
		for w := 0; w < 10; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					id, ok := tracker.claim()
					if !ok {
						return
					}
					claimed <- id
				}
			}()
		}

		wg.Wait()
		close(claimed)

		// Verify all IDs were claimed exactly once
		seen := make(map[uint64]bool)
		for id := range claimed {
			assert.False(t, seen[id], "ID %d claimed multiple times", id)
			seen[id] = true
		}
		assert.Equal(t, 10000, len(seen))
	})

	t.Run("concurrent claim and complete is safe", func(t *testing.T) {
		tracker := newProgressTracker(0, 1000)

		var wg sync.WaitGroup

		// 5 workers
		for w := 0; w < 5; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					id, ok := tracker.claim()
					if !ok {
						return
					}
					// Simulate some work
					time.Sleep(time.Microsecond)
					tracker.complete(id)
				}
			}()
		}

		wg.Wait()

		// All work complete, watermark should be 999
		assert.Equal(t, uint64(999), tracker.getSafeWatermark())
	})

	t.Run("watermark never exceeds actual progress", func(t *testing.T) {
		tracker := newProgressTracker(0, 1000)

		var wg sync.WaitGroup
		stop := make(chan struct{})

		// Workers claiming and completing
		for w := 0; w < 4; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}

					id, ok := tracker.claim()
					if !ok {
						return
					}
					// Random delay
					time.Sleep(time.Duration(id%100) * time.Microsecond)
					tracker.complete(id)
				}
			}()
		}

		// Periodically check watermark
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				select {
				case <-stop:
					return
				default:
				}

				watermark := tracker.getSafeWatermark()
				claimed, _, _ := tracker.getProgress()

				// Watermark + 1 + inFlight should be <= claimed
				// (watermark is the highest contiguous completed ID)
				if watermark > 0 && claimed > 0 {
					// This is a sanity check - watermark should never exceed
					// the number of claimed IDs minus in-flight
					assert.LessOrEqual(t, watermark, claimed-1,
						"watermark %d exceeded claimed-1 %d", watermark, claimed-1)
				}

				time.Sleep(time.Millisecond)
			}
		}()

		// Let it run briefly then stop
		time.Sleep(50 * time.Millisecond)
		close(stop)
		wg.Wait()
	})
}

func TestProgressTracker_ResumeFromWatermark(t *testing.T) {
	t.Run("resume from watermark 500", func(t *testing.T) {
		// Simulate: previous run completed nodes 0-499, persisted watermark 499
		// Resume should start from 500
		tracker := newProgressTracker(500, 1000)

		id, ok := tracker.claim()
		assert.True(t, ok)
		assert.Equal(t, uint64(500), id)

		// Initial watermark for resumed tracker should be 499
		assert.Equal(t, uint64(499), tracker.lastPersistedWatermark)
	})
}

func TestCreateCheckpointFromDeleteList(t *testing.T) {
	t.Run("creates checkpoint with sorted tombstones", func(t *testing.T) {
		deleteList := helpers.NewAllowList(300, 100, 500, 200, 400)

		cp := createCheckpointFromDeleteList(42, deleteList, 10000)

		assert.Equal(t, uint64(42), cp.GenerationID)
		assert.Equal(t, uint64(10000), cp.TotalNodes)
		assert.Equal(t, uint64(0), cp.Watermark)
		assert.Equal(t, cleanupCheckpointVersion, cp.Version)

		// Tombstones should be sorted
		assert.Equal(t, []uint64{100, 200, 300, 400, 500}, cp.TombstoneIDs)
	})
}

func TestRebuildDeleteListFromCheckpoint(t *testing.T) {
	t.Run("rebuilds exact same list", func(t *testing.T) {
		original := helpers.NewAllowList(100, 200, 300, 400, 500)

		cp := createCheckpointFromDeleteList(1, original, 1000)
		rebuilt := rebuildDeleteListFromCheckpoint(cp)

		// Verify same contents
		assert.Equal(t, original.Len(), rebuilt.Len())
		for _, id := range original.Slice() {
			assert.True(t, rebuilt.Contains(id))
		}
	})
}

// TestGenerationIsolation proves the critical correctness property:
// When a cleanup generation starts with tombstones {x1, x2}, gets interrupted,
// and a new tombstone x3 is added before resume, ONLY {x1, x2} are removed
// after the resumed generation completes. x3 must remain pending for the
// next generation.
//
// This test simulates the checkpoint-based resume flow without running
// the full HNSW cleanup machinery.
func TestGenerationIsolation(t *testing.T) {
	t.Run("interrupt after partial scan, add tombstone, resume, verify only original batch removed", func(t *testing.T) {
		tmpDir := t.TempDir()
		checkpointPath := filepath.Join(tmpDir, "test.checkpoint")

		// === PHASE 1: Start generation with {x1=100, x2=200} ===
		initialTombstones := helpers.NewAllowList(100, 200)
		totalNodes := uint64(1000)

		checkpoint := createCheckpointFromDeleteList(1, initialTombstones, totalNodes)
		require.Equal(t, uint64(1), checkpoint.GenerationID)
		require.Equal(t, []uint64{100, 200}, checkpoint.TombstoneIDs)
		require.Equal(t, uint64(0), checkpoint.Watermark)

		// Simulate partial processing: process nodes 0-499
		tracker := newProgressTracker(0, totalNodes)
		for i := uint64(0); i < 500; i++ {
			id, ok := tracker.claim()
			require.True(t, ok)
			require.Equal(t, i, id)
			tracker.complete(id)
		}

		// Verify watermark is at 499 (all nodes 0-499 complete)
		watermark := tracker.getSafeWatermark()
		require.Equal(t, uint64(499), watermark)

		// === PHASE 2: Simulate interruption ===
		// Save checkpoint with watermark at 499
		checkpoint.Watermark = watermark
		err := saveCleanupCheckpointOS(checkpointPath, checkpoint)
		require.NoError(t, err)

		// === PHASE 3: Add new tombstone x3=300 (simulating concurrent deletion) ===
		// This tombstone is added AFTER the generation started, so it must NOT
		// be in the checkpoint's TombstoneIDs and must NOT be removed by this generation
		x3 := uint64(300)

		// The live tombstones map would now contain {100, 200, 300}
		// But the checkpoint only has {100, 200}
		liveTombstones := helpers.NewAllowList(100, 200, 300)

		// === PHASE 4: Simulate resume from checkpoint ===
		loadedCheckpoint, err := loadCleanupCheckpointOS(checkpointPath)
		require.NoError(t, err)
		require.NotNil(t, loadedCheckpoint)

		// Verify checkpoint has ONLY the original tombstones
		require.Equal(t, uint64(1), loadedCheckpoint.GenerationID)
		require.Equal(t, []uint64{100, 200}, loadedCheckpoint.TombstoneIDs)
		require.Equal(t, uint64(499), loadedCheckpoint.Watermark)

		// Rebuild deleteList from checkpoint (NOT from live tombstones!)
		deleteList := rebuildDeleteListFromCheckpoint(loadedCheckpoint)

		// Critical assertion: deleteList must NOT contain x3
		require.True(t, deleteList.Contains(100), "deleteList must contain x1")
		require.True(t, deleteList.Contains(200), "deleteList must contain x2")
		require.False(t, deleteList.Contains(x3), "deleteList must NOT contain x3 (added after generation started)")

		// Resume processing from watermark + 1
		startFrom := loadedCheckpoint.Watermark + 1
		require.Equal(t, uint64(500), startFrom)

		resumedTracker := newProgressTracker(startFrom, totalNodes)

		// Process remaining nodes 500-999
		for {
			id, ok := resumedTracker.claim()
			if !ok {
				break
			}
			resumedTracker.complete(id)
		}

		// Final watermark should be 999
		finalWatermark := resumedTracker.getSafeWatermark()
		require.Equal(t, uint64(999), finalWatermark)

		// === PHASE 5: Simulate generation completion ===
		// After generation completes, we would iterate over deleteList (from checkpoint)
		// and remove ONLY those tombstones

		// Simulate removal: for each id in deleteList, remove from liveTombstones
		it := deleteList.Iterator()
		removedCount := 0
		for id, ok := it.Next(); ok; id, ok = it.Next() {
			if liveTombstones.Contains(id) {
				// In real code: delete(h.tombstones, id)
				removedCount++
			}
		}
		it.Stop()

		// We should have removed exactly 2 tombstones (x1 and x2)
		require.Equal(t, 2, removedCount)

		// x3 should still be in liveTombstones (deferred to next generation)
		require.True(t, liveTombstones.Contains(x3),
			"x3 must remain in liveTombstones after this generation completes")

		// Delete checkpoint to mark generation complete
		err = deleteCleanupCheckpointOS(checkpointPath)
		require.NoError(t, err)

		// Verify checkpoint is deleted
		loaded, err := loadCleanupCheckpointOS(checkpointPath)
		require.NoError(t, err)
		require.Nil(t, loaded, "checkpoint should be deleted after generation completes")
	})

	t.Run("resume from checkpoint does not restart from node 0", func(t *testing.T) {
		tmpDir := t.TempDir()
		checkpointPath := filepath.Join(tmpDir, "test.checkpoint")

		// Create checkpoint with watermark at 5000
		checkpoint := &CleanupCheckpoint{
			Version:      cleanupCheckpointVersion,
			GenerationID: 1,
			TombstoneIDs: []uint64{100, 200},
			TotalNodes:   10000,
			Watermark:    5000,
			StartedAt:    time.Now(),
			UpdatedAt:    time.Now(),
		}
		err := saveCleanupCheckpointOS(checkpointPath, checkpoint)
		require.NoError(t, err)

		// Load and resume
		loaded, err := loadCleanupCheckpointOS(checkpointPath)
		require.NoError(t, err)

		// Resume should start from watermark + 1
		startFrom := loaded.Watermark + 1
		require.Equal(t, uint64(5001), startFrom, "resume must start from watermark+1, not 0")

		tracker := newProgressTracker(startFrom, loaded.TotalNodes)

		// First claim should be 5001, not 0
		firstClaim, ok := tracker.claim()
		require.True(t, ok)
		require.Equal(t, uint64(5001), firstClaim,
			"first claimed ID on resume must be watermark+1, got %d", firstClaim)
	})
}
