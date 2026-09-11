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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/geo"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

const reuseTestClass = "DocIDReuseTest"

func reuseTestShard(t *testing.T, ctx context.Context, asyncIndexing bool) *Shard {
	t.Helper()
	class := &models.Class{
		Class: reuseTestClass,
		Properties: []*models.Property{{
			Name:         "category",
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
		}},
	}
	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{}, false, asyncIndexing)
	s := shd.(*Shard)
	t.Cleanup(func() { s.Shutdown(context.Background()) })
	return s
}

func reusePutObject(t *testing.T, s *Shard, vec []float32) strfmt.UUID {
	t.Helper()
	obj := &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:         strfmt.UUID(uuid.NewString()),
			Class:      reuseTestClass,
			Properties: map[string]interface{}{"category": "keep"},
		},
		Vector: vec,
	}
	require.NoError(t, s.PutObject(context.Background(), obj))
	return obj.Object.ID
}

func reuseDocIDOf(t *testing.T, s *Shard, id strfmt.UUID) uint64 {
	t.Helper()
	obj, err := s.ObjectByID(context.Background(), id, nil, additionalProps())
	require.NoError(t, err)
	require.NotNil(t, obj)
	return obj.DocID
}

type tombstoneCleaner interface {
	CleanUpTombstonedNodes(cyclemanager.ShouldAbortCallback) error
}

// runTombstoneCleanups runs the HNSW tombstone cleanup for every vector and
// geo index of the shard, synchronously.
func runTombstoneCleanups(t *testing.T, s *Shard) {
	t.Helper()
	never := cyclemanager.ShouldAbortCallback(func() bool { return false })
	require.NoError(t, s.ForEachVectorIndex(func(_ string, idx VectorIndex) error {
		if c, ok := idx.(tombstoneCleaner); ok {
			return c.CleanUpTombstonedNodes(never)
		}
		return nil
	}))
	require.NoError(t, s.ForEachGeoIndex(func(_ string, gi *geo.Index) error {
		if c, ok := gi.UnderlyingVectorIndex().(tombstoneCleaner); ok {
			return c.CleanUpTombstonedNodes(never)
		}
		return nil
	}))
}

func runHarvest(s *Shard) bool {
	return s.freeList.harvestCycle(func() bool { return false })
}

// TestDocIDReuse_Lifecycle drives the full journey: delete → tombstone
// cleanup → harvest (persisted) → acquire → the next insert reuses the id.
func TestDocIDReuse_Lifecycle(t *testing.T) {
	t.Setenv("DOCID_REUSE_ENABLED", "true")
	ctx := context.Background()
	s := reuseTestShard(t, ctx, false)

	idA := reusePutObject(t, s, []float32{1, 0, 0})
	idB := reusePutObject(t, s, []float32{0, 1, 0})
	_ = idA
	docIDB := reuseDocIDOf(t, s, idB)

	require.NoError(t, s.DeleteObject(ctx, idB, time.Time{}))
	runTombstoneCleanups(t, s)

	require.True(t, runHarvest(s), "harvest must promote the cleaned id")
	require.FileExists(t, filepath.Join(s.path(), docIDFreeListFileName))

	idC := reusePutObject(t, s, []float32{0, 0, 1})
	require.Equal(t, docIDB, reuseDocIDOf(t, s, idC),
		"the next insert must reuse the cleaned docID")

	// and the object is fully functional under the reused id
	obj, err := s.ObjectByID(ctx, idC, nil, additionalProps())
	require.NoError(t, err)
	require.NotNil(t, obj)
}

// TestDocIDReuse_PoisonedEntryFailsHard pins the single verification point:
// an id planted in the free list while an object row still references it
// must fail the insert loudly, never silently degrade.
func TestDocIDReuse_PoisonedEntryFailsHard(t *testing.T) {
	t.Setenv("DOCID_REUSE_ENABLED", "true")
	ctx := context.Background()
	s := reuseTestShard(t, ctx, false)

	idA := reusePutObject(t, s, []float32{1, 0, 0})
	docIDA := reuseDocIDOf(t, s, idA) // LIVE object

	// poison the free list
	s.freeList.mu.Lock()
	s.freeList.free = append(s.freeList.free, docIDA)
	s.freeList.inFree[docIDA] = struct{}{}
	s.freeList.mu.Unlock()

	obj := &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:         strfmt.UUID(uuid.NewString()),
			Class:      reuseTestClass,
			Properties: map[string]interface{}{"category": "keep"},
		},
		Vector: []float32{0, 1, 0},
	}
	err := s.PutObject(ctx, obj)
	require.Error(t, err, "insert with a poisoned free-list entry must fail hard")
	require.Contains(t, err.Error(), "docID reuse invariant violation")
}

// TestDocIDReuse_PoisonedEntryIndexSideFailsHard is the index-side sibling of
// the row-side poison test: the object row is gone, but an index still holds
// per-id state (here: the pending tombstone; the same assertNoTraces →
// CleanForReuse path also rejects a stranded entrypoint, pinned at the hnsw
// level in TestCleanForReuse_EntrypointGuard). Acquire must fail the insert
// hard, never hand the id out.
func TestDocIDReuse_PoisonedEntryIndexSideFailsHard(t *testing.T) {
	t.Setenv("DOCID_REUSE_ENABLED", "true")
	ctx := context.Background()
	s := reuseTestShard(t, ctx, false)

	reusePutObject(t, s, []float32{1, 0, 0})
	idB := reusePutObject(t, s, []float32{0, 1, 0})
	docIDB := reuseDocIDOf(t, s, idB)

	// delete WITHOUT running tombstone cleanup: row gone, hnsw tombstone
	// still pending
	require.NoError(t, s.DeleteObject(ctx, idB, time.Time{}))

	// poison the free list with the not-yet-clean id
	s.freeList.mu.Lock()
	s.freeList.free = append(s.freeList.free, docIDB)
	s.freeList.inFree[docIDB] = struct{}{}
	s.freeList.mu.Unlock()

	obj := &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:         strfmt.UUID(uuid.NewString()),
			Class:      reuseTestClass,
			Properties: map[string]interface{}{"category": "keep"},
		},
		Vector: []float32{0, 0, 1},
	}
	err := s.PutObject(ctx, obj)
	require.Error(t, err, "insert must fail hard on index-side traces")
	require.Contains(t, err.Error(), "docID reuse invariant violation")
	require.Contains(t, err.Error(), "still holds a trace")
}

// TestDocIDReuse_CrashBeforePersistIsSafe pins the crash-safety direction: if
// the process dies after tombstone cleanup but before the harvest persisted
// the free list, the id must NOT be reusable after restart.
func TestDocIDReuse_CrashBeforePersistIsSafe(t *testing.T) {
	t.Setenv("DOCID_REUSE_ENABLED", "true")
	ctx := context.Background()
	s := reuseTestShard(t, ctx, false)

	idA := reusePutObject(t, s, []float32{1, 0, 0})
	docIDA := reuseDocIDOf(t, s, idA)
	require.NoError(t, s.DeleteObject(ctx, idA, time.Time{}))
	runTombstoneCleanups(t, s)
	// crash before harvest: no persist happened

	reloaded := newShardDocIDFreeList(s)
	reloaded.Load()
	id, ok, err := reloaded.Acquire()
	require.NoError(t, err)
	require.False(t, ok, "unpersisted id %d must not be reusable after restart (got %d)", docIDA, id)

	// now harvest (persist) and reload: the id survives the restart
	require.True(t, runHarvest(s))
	reloaded2 := newShardDocIDFreeList(s)
	reloaded2.Load()
	id, ok, err = reloaded2.Acquire()
	require.NoError(t, err)
	require.True(t, ok, "persisted free list must survive reload")
	require.Equal(t, docIDA, id)
}

// TestDocIDReuse_IntersectionWithGeo pins that an id only becomes reusable
// once EVERY index of the shard reports it clean — the geo index included.
func TestDocIDReuse_IntersectionWithGeo(t *testing.T) {
	t.Setenv("DOCID_REUSE_ENABLED", "true")
	ctx := context.Background()

	class := &models.Class{
		Class: reuseTestClass,
		Properties: []*models.Property{
			{
				Name:         "category",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
			{
				Name:     "location",
				DataType: []string{string(schema.DataTypeGeoCoordinates)},
			},
		},
	}
	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{}, false, false)
	s := shd.(*Shard)
	t.Cleanup(func() { s.Shutdown(context.Background()) })

	putGeo := func(lat, lon float32, vec []float32) *storobj.Object {
		obj := &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:    strfmt.UUID(uuid.NewString()),
				Class: reuseTestClass,
				Properties: map[string]interface{}{
					"category": "keep",
					"location": &models.GeoCoordinates{Latitude: &lat, Longitude: &lon},
				},
			},
			Vector: vec,
		}
		require.NoError(t, s.PutObject(ctx, obj))
		return obj
	}

	// Two geo nodes: deleting one must leave a REAL pending tombstone. (A
	// sole node would take hnsw's resetIfOnlyNode shortcut, which wipes the
	// graph including tombstones and leaves the index immediately clean.)
	putGeo(48.1, 11.5, []float32{1, 0, 0})
	obj := putGeo(52.5, 13.4, []float32{0, 1, 0})
	docID := reuseDocIDOf(t, s, obj.Object.ID)

	// sanity: the geo index exists and holds the vector
	geoCount := 0
	require.NoError(t, s.ForEachGeoIndex(func(name string, gi *geo.Index) error {
		geoCount++
		require.True(t, gi.UnderlyingVectorIndex().(VectorIndex).ContainsDoc(docID),
			"geo index %q must contain docID %d before the delete", name, docID)
		return nil
	}))
	require.Equal(t, 1, geoCount, "the location property must have a geo index")

	require.NoError(t, s.DeleteObject(ctx, obj.Object.ID, time.Time{}))

	// Clean ONLY the main vector index; the geo index still holds the
	// tombstone → the id must stay out of the free list.
	never := cyclemanager.ShouldAbortCallback(func() bool { return false })
	require.NoError(t, s.ForEachVectorIndex(func(_ string, idx VectorIndex) error {
		return idx.(tombstoneCleaner).CleanUpTombstonedNodes(never)
	}))

	require.False(t, runHarvest(s), "geo index not cleaned: nothing may be harvested")
	_, ok, err := s.freeList.Acquire()
	require.NoError(t, err)
	require.False(t, ok, "docID %d must be withheld while the geo index is dirty", docID)

	// Now clean the geo index too → the id becomes reusable.
	require.NoError(t, s.ForEachGeoIndex(func(_ string, gi *geo.Index) error {
		return gi.UnderlyingVectorIndex().(tombstoneCleaner).CleanUpTombstonedNodes(never)
	}))
	require.True(t, runHarvest(s), "all indexes clean: harvest must promote the id")
	id, ok, err := s.freeList.Acquire()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, docID, id)
}

// TestDocIDReuse_DrainGate pins the drain gate: an id whose indexes are all
// clean is still withheld while queue chunks captured at delete time remain
// pending.
func TestDocIDReuse_DrainGate(t *testing.T) {
	t.Setenv("DOCID_REUSE_ENABLED", "true")
	ctx := context.Background()
	s := reuseTestShard(t, ctx, false)

	idA := reusePutObject(t, s, []float32{1, 0, 0})
	docIDA := reuseDocIDOf(t, s, idA)
	require.NoError(t, s.DeleteObject(ctx, idA, time.Time{}))
	runTombstoneCleanups(t, s)

	// All indexes are clean now. Simulate an undrained queue by planting a
	// fake pending chunk name into the candidate's watermark and a matching
	// file into a queue dir — the gate must hold the id back until the chunk
	// disappears.
	var queueID, queueDir string
	require.NoError(t, s.ForEachVectorQueue(func(_ string, q *VectorIndexQueue) error {
		if q != nil && q.DiskQueue != nil {
			queueID = q.ID()
			queueDir = q.Dir()
		}
		return nil
	}))
	if queueID == "" {
		t.Skip("no disk queue on this shard configuration")
	}
	require.NoError(t, os.MkdirAll(queueDir, 0o755))
	fakeChunk := filepath.Join(queueDir, "chunk-999999.bin")
	require.NoError(t, os.WriteFile(fakeChunk, []byte("x"), 0o644))

	s.freeList.mu.Lock()
	cand, exists := s.freeList.pending[docIDA]
	require.True(t, exists, "candidate must be pending")
	cand.watermark[queueID] = map[string]struct{}{"chunk-999999.bin": {}}
	s.freeList.mu.Unlock()

	require.False(t, runHarvest(s), "undrained watermark: nothing may be harvested")
	_, ok, err := s.freeList.Acquire()
	require.NoError(t, err)
	require.False(t, ok, "id must be withheld while its watermark chunk is pending")

	// Drain: the captured chunk disappears → the id is released.
	require.NoError(t, os.Remove(fakeChunk))
	require.True(t, runHarvest(s))
	id, ok, err := s.freeList.Acquire()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, docIDA, id)
}

// TestDocIDReuse_MaintenancePausesReuse pins that maintenance operations
// disable reuse while running, and that the pause stacks/releases correctly.
func TestDocIDReuse_MaintenancePausesReuse(t *testing.T) {
	t.Setenv("DOCID_REUSE_ENABLED", "true")
	ctx := context.Background()
	s := reuseTestShard(t, ctx, false)

	idA := reusePutObject(t, s, []float32{1, 0, 0})
	docIDA := reuseDocIDOf(t, s, idA)
	require.NoError(t, s.DeleteObject(ctx, idA, time.Time{}))
	runTombstoneCleanups(t, s)
	require.True(t, runHarvest(s))

	// While paused (as RepairIndex/FillQueue/RequantizeIndex and a dynamic
	// upgrade do), Acquire must yield nothing and harvest must not run.
	s.freeList.Pause()
	_, ok, err := s.freeList.Acquire()
	require.NoError(t, err)
	require.False(t, ok, "paused: no id may be acquired")
	s.freeList.Resume()

	id, ok, err := s.freeList.Acquire()
	require.NoError(t, err)
	require.True(t, ok, "resumed: the id is available again")
	require.Equal(t, docIDA, id)
	s.freeList.Return(id)

	// RequantizeIndex runs even without async indexing and must hold the
	// pause for its duration; afterwards reuse works again. (FillQueue and
	// RepairIndex early-return without async indexing but share the same
	// Pause/defer-Resume pattern.)
	require.NoError(t, s.RequantizeIndex(ctx, ""))
	id, ok, err = s.freeList.Acquire()
	require.NoError(t, err)
	require.True(t, ok, "reuse must work again after maintenance finished")
	require.Equal(t, docIDA, id)
}

// TestDocIDReuse_DefaultNeverCleanScopesFlag pins the structural scope
// enforcement: a shard whose index does not implement the reuse surface
// (here: the noop index, standing in for HFresh and any future type) never
// frees ids, even with the flag on.
func TestDocIDReuse_DefaultNeverCleanScopesFlag(t *testing.T) {
	t.Setenv("DOCID_REUSE_ENABLED", "true")
	ctx := context.Background()
	class := &models.Class{
		Class: reuseTestClass,
		Properties: []*models.Property{{
			Name:         "category",
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
		}},
	}
	// Skip: true wires the noop vector index, which does NOT implement
	// common.ReuseCleanliness.
	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false)
	s := shd.(*Shard)
	t.Cleanup(func() { s.Shutdown(context.Background()) })

	idA := reusePutObject(t, s, []float32{1, 0, 0})
	require.NoError(t, s.DeleteObject(ctx, idA, time.Time{}))

	require.False(t, runHarvest(s), "an index without the reuse surface must never harvest")
	_, ok, err := s.freeList.Acquire()
	require.NoError(t, err)
	require.False(t, ok, "shard with a non-participating index must never reuse ids")
}

// TestDocIDReuse_PhysicalCopyDifferentLives is the replica scenario: a shard
// directory receives a physical copy from another node where the same docID
// leads a DIFFERENT life (alive there, freed here). A stale local free list
// must not poison the copied data — load-time validation drops every id the
// copied state disagrees with.
func TestDocIDReuse_PhysicalCopyDifferentLives(t *testing.T) {
	t.Setenv("DOCID_REUSE_ENABLED", "true")
	ctx := context.Background()

	// Source of the free list: a shard where docID 1 was deleted and freed.
	s1 := reuseTestShard(t, ctx, false)
	reusePutObject(t, s1, []float32{1, 0, 0})
	idB := reusePutObject(t, s1, []float32{0, 1, 0})
	docIDB := reuseDocIDOf(t, s1, idB)
	require.NoError(t, s1.DeleteObject(ctx, idB, time.Time{}))
	runTombstoneCleanups(t, s1)
	require.True(t, runHarvest(s1))

	// Target: a shard where the same docID is a LIVE object.
	s2 := reuseTestShard(t, ctx, false)
	reusePutObject(t, s2, []float32{1, 0, 0})
	idLive := reusePutObject(t, s2, []float32{0, 1, 0})
	require.Equal(t, docIDB, reuseDocIDOf(t, s2, idLive), "test setup: same docID, different life")

	// The stale free list lands in the target's directory (as a leftover of
	// a physical copy that replaced the data files but not this one, or the
	// reverse). Reloading must reject the id.
	src, err := os.ReadFile(filepath.Join(s1.path(), docIDFreeListFileName))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(s2.path(), docIDFreeListFileName), src, 0o644))

	reloaded := newShardDocIDFreeList(s2)
	reloaded.Load()
	_, ok, err := reloaded.Acquire()
	require.NoError(t, err)
	require.False(t, ok, "docID %d leads a live life here; the foreign free list must be rejected", docIDB)
}

// TestDocIDReuse_FlagOffIsInert pins that with the flag off (the default)
// nothing registers, nothing harvests, nothing is acquired and no state file
// is created — the monotonic counter remains the only allocator.
func TestDocIDReuse_FlagOffIsInert(t *testing.T) {
	ctx := context.Background()
	s := reuseTestShard(t, ctx, false)

	idA := reusePutObject(t, s, []float32{1, 0, 0})
	docIDA := reuseDocIDOf(t, s, idA)
	require.NoError(t, s.DeleteObject(ctx, idA, time.Time{}))
	runTombstoneCleanups(t, s)

	require.False(t, runHarvest(s))
	require.Empty(t, s.freeList.pending, "flag off: no candidates registered")
	_, ok, err := s.freeList.Acquire()
	require.NoError(t, err)
	require.False(t, ok)
	require.NoFileExists(t, filepath.Join(s.path(), docIDFreeListFileName))

	idB := reusePutObject(t, s, []float32{0, 1, 0})
	require.Equal(t, docIDA+1, reuseDocIDOf(t, s, idB),
		"flag off: the counter must keep allocating monotonically")
}

func additionalProps() additional.Properties { return additional.Properties{} }
