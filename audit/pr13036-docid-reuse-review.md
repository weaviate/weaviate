# Review: docID reuse (PR #13036 and its stack)

Reviewed at head f03e86a0fd (stack: #12894, #13029, #12886, #13036). File:line references are at that commit. Evidence levels:
- **REPRODUCED**: a failing test exists; I can share it on request.
- **TRACED**: followed through the code, not executed.

## TL;DR

- **Recommendation: don't merge in this form.**
- **The gate is narrower than the invariant.** Reuse is only safe if nothing still refers to the old life of docID X. The gate (queue drain + `CleanForReuse`) only proves that the vector indexes and their queues have released X. It doesn't cover:
  - in-flight queries;
  - objects-bucket tombstones;
  - an old-life vector insert that isn't queued yet;
  - leftovers from a crash mid-insert.
- **Reuse turns a harmless miss into a silent wrong answer.** Today a stale docID resolves to "not found". With reuse it resolves to a different object, and nothing reports an error.
- **Two critical bugs reproduce** (C1, C2).
- **"Flag off is behavior-identical" does not hold** (section 3).
- **Suggested direction: generation-tagged docIDs** (section 1).

## 1. Is reuse a good idea?

**What it buys.** DocIDs grow with every delete and every vector-changing update. Several structures are sized by the max docID rather than by the live count: the HNSW nodes array, the caches, visited lists and the filter universe. That is a real problem on high-churn shards.

**What it costs.** Every holder of a docID currently relies on "never reused". With reuse, each holder must be proven to have let go of X before X is freed.

| Holder of X | Covered by the gate? |
|---|---|
| Vector queues, HNSW/flat/geo state | Yes |
| In-flight query (allow list, HNSW candidates, BM25 hits, groupBy/aggregation second pass, delete-by-filter) | No (C2, H3) |
| Objects-bucket tombstone that keeps X as secondary key | No (C1) |
| Old-life vector insert not yet queued | No (H1) |
| Postings or queued vector left by a crash mid-insert | No (H2) |

There is no grace period, minimum age or reader epoch. The free list is LIFO, so the most recently deleted ids are reused first. For objects without a vector, X can be reused milliseconds after the delete.

**Alternatives:**
1. **Generation-tagged docIDs.** Split the id into a reusable slot and a generation. Arrays stay dense, every full id stays unique, and stale references fail cleanly. This removes C1, C2, H1 and H3 by construction.
2. **Sparse or paged id-indexed structures.** These fix memory without touching identity.
3. **Keep the docID on vector updates** where the index supports in-place replacement. Updates are the main source of churn.

If raw reuse stays, it needs reader quiescence, a minimum quarantine age, and a rule that objects-bucket tombstones never carry a reusable secondary key.

## 2. Critical and high findings (flag on)

**C1. A reused live object resolves as deleted. CRITICAL, REPRODUCED.**
- **How it happens:**
  - A delete writes a tombstone that still carries docID X as secondary key (shard_write_delete.go:315-323).
  - The objects bucket keeps tombstones forever (KeepTombstones).
  - Flush and compaction keep a tombstone's secondary keys.
- **What goes wrong:**
  - After reuse, one segment holds two entries for X: the old tombstone and the new row. The segment's secondary index has no dedupe (segmentindex/tree.go:458-475), and the lookup returns whichever it hits first.
  - On a tombstone hit, GetBySecondary returns nil for a live object.
- **Repro:** 100 ids reused. Across all three variants, every run lost 41–55 of 100 objects, and a plain flush is enough to trigger it. Each lost object:
  - can't be found by docID;
  - is dropped from search results;
  - is not returned by nearVector for its own vector.
- **Knock-on effects:**
  - HNSW treats the not-found as a deleted object and tombstones the node, so the vector is lost permanently while the object is still there by UUID.
  - The free list's own row check is defeated the same way.
- **Sibling, TRACED:** re-creating the old UUID in the same memtable clears the X → new-owner mapping (memtable.go:465-471).

**C2. A stale query re-tombstones a freed id. HIGH, REPRODUCED (deterministic).**
- **How it happens:** when a fetch returns not-found, `handleDeletedNode` only checks for an existing tombstone, never whether the node still exists (hnsw/search.go:904-917). A search that collected X before cleanup and rescores after harvest therefore tombstones a freed id.
- **Consequence:** either the next insert that acquires X fails hard (no fallback to the counter), or the new owner is hidden from search and removed by the next cleanup.
- **Repro:** both outcomes reproduced with a real compressed HNSW search; only the interleaving was forced.
- **Worse path:** `knnSearchByVector` (search.go:1058-1066) adds the tombstone unconditionally.

**H1. A late old-life vector insert lands on the new owner. HIGH, TRACED.**
- putOne queues the vector work after releasing docIdLock (shard_write_put.go:56-81), and batch put does the same.
- When a delete or update races on the same UUID, the drain watermark misses the later insert. X is freed and reused, and the old vector then attaches to the new object.

**H2. Restart re-frees ids that are in use. HIGH, TRACED.**
- Acquired ids stay in the persisted file until the next harvest.
- The insert path writes row, postings and queue before an un-fsynced WAL flush.
- After a crash, the load-time check only looks at the row and the vector indexes. X is freed again, and the next owner inherits orphan postings or a replayed vector.

**H3. A running query resolves X to a different object. HIGH, TRACED.**
- A query collects X, X is deleted, freed and reused, and then the query resolves X. Nothing compares UUID or generation. Consequences:
  - filtered search returns objects that fail the filter;
  - BM25 and hybrid attach the old object's score;
  - groupBy and aggregations read X twice;
  - delete-by-filter can delete an unrelated object.

**Copilot's comments, re-assessed:**
- **Pause vs. dynamic upgrade: not reachable as described.** The queue is paused with zero active tasks before Upgrade (vector_index_queue.go:302-311). The real dynamic gaps are in M4.
- **UnTrackProperty: confirmed, and worse than stated.** It decrements before checking that the property exists (new_prop_length_tracker.go:245-253), so retries double-decrement. Its error now aborts a delete before the row write, which makes the object undeletable. This also applies with the flag off.
- **Merge leaks: confirmed, and wider.** Batch put, read repair and change-log replay never register retired ids either, so reuse is effectively off for batch-imported data. These are leaks only, which is the safe direction.

## 3. Flag off is not behavior-identical (TRACED)

These run regardless of the flag:
- **Delete reordering** (inverted cleanup before the row delete). An error or crash in between leaves a live row with no postings: filters and BM25 can't find it, and NotEqual filters return it. While a delete is running, NotEqual filters already return the object.
- **Two-phase batch delete.** The lock is released between the phases. A failed batch leaves objects that re-running the same delete-by-filter can't find. Each deleted object also costs an extra read and two sha256 hashes.
- **Extra WAL flush on updates.** Every docID-changing update does a full-store WAL flush while holding docIdLock.
- **Filter universe no longer shrinks.** Negated filters re-resolve every deleted id on every query, and the inflated allow-list length skews the flat-vs-graph search decision.
- **Smaller changes.** A sync on a closed commit log now returns success, and the env flag is read on every insert and delete.

## 4. Medium

- **M1.** The candidate list grows without bound on shards with a non-participating index. Each candidate copies every queue's chunk-name set, and every delete reads the queue directory while holding locks.
- **M2.** The acquired-ids map never shrinks.
- **M3.** Harvest holds the free-list lock across write, fsync and rename, which stalls every insert.
- **M4.** A dynamic index fails inserts in the window between TryUpgrading and the start hook. Dynamic target vectors added at runtime never get the upgrade hooks.
- **M5.** Flat-only collections never start the cleanup cycle, so reuse is silently off for them (shard_init_vector.go:190-208).
- **M6.** The documented BM25 flush/compaction tombstone race becomes a wrong-result bug.
- **M7.** The harvest's index flush is not durable. The comment at docid_freelist.go:351 overstates it.
- **M8.** Filter-only result order (docID order) no longer reflects insertion order, so offset pagination drifts.

**Why the PR's own tests missed C1 and C2:**
- Test shards never compact the objects bucket.
- No test flushes the objects bucket after a reuse.
- The reuse test shard's HNSW config has no defaults, so nearVector can't find objects even with the flag off.

## 5. Before enabling the flag

- [ ] Tests:
  - reuse followed by flush and compaction;
  - reuse after the old UUID is re-created;
  - reuse racing a query (filtered search, BM25, groupBy, delete-by-filter, stale rescore);
  - concurrent writes to the same UUID;
  - a crash mid-insert, then reload;
  - delete/update failure and retry with the flag off.
- [ ] Reader quiescence plus a minimum age, or generation-tagged ids.
- [ ] A rule for objects-bucket tombstone secondary keys.
- [ ] Gate the reorderings behind the flag, or test their flag-off behavior.


## Appendix: C1/C2 reproduction tests

Drop both files into f03e86a0fd; they need no production changes. Run:
- C1: `go test -tags integrationTest -count 1 -run TestDocIDReuseRepro ./adapters/repos/db/ -v`
- C2: `go test -count 1 -run TestDocIDReuseRepro ./adapters/repos/db/vector/hnsw/ -v`

### C1: adapters/repos/db/docid_reuse_repro_integration_test.go

<details><summary>show</summary>

```go
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
	"encoding/binary"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

type reproObj struct {
	id    strfmt.UUID
	vec   []float32
	docID uint64
}

type reproLoss struct {
	secondaryNil    int
	secondaryWrong  int
	byDocIDMissing  int
	searchMissing   int
	rowCheckDefeats int
}

// reproShard is reuseTestShard with a defaulted HNSW config, so nearVector
// reliably finds an object by its own vector.
func reproShard(t *testing.T, ctx context.Context) *Shard {
	t.Helper()
	class := &models.Class{
		Class: reuseTestClass,
		Properties: []*models.Property{{
			Name:         "category",
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
		}},
	}
	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.NewDefaultUserConfig(), false, false)
	s := shd.(*Shard)
	t.Cleanup(func() { s.Shutdown(context.Background()) })
	return s
}

func reproObjectsBucketSegments(s *Shard) int {
	view := s.store.Bucket(helpers.ObjectsBucketLSM).GetConsistentView()
	defer view.ReleaseView()
	return len(view.Disk)
}

func reproFlushObjects(t *testing.T, s *Shard) {
	t.Helper()
	require.NoError(t, s.store.Bucket(helpers.ObjectsBucketLSM).FlushAndSwitch())
}

func reproCompactToOneSegment(t *testing.T, s *Shard) {
	t.Helper()
	never := cyclemanager.ShouldAbortCallback(func() bool { return false })
	for i := 0; i < 50 && reproObjectsBucketSegments(s) > 1; i++ {
		s.cycleCallbacks.compactionCallbacks.CycleCallback(never)
		if s.cycleCallbacks.compactionAuxCallbacks != nil {
			s.cycleCallbacks.compactionAuxCallbacks.CycleCallback(never)
		}
	}
	require.Equal(t, 1, reproObjectsBucketSegments(s), "objects bucket must be compacted into one segment")
}

func reproMeasureLoss(t *testing.T, s *Shard, objs []reproObj) reproLoss {
	t.Helper()
	ctx := context.Background()
	var loss reproLoss

	bucket, release, err := s.objectsBucket()
	require.NoError(t, err)
	docIDs := make([]uint64, len(objs))
	var unresolvable []uint64
	for i, o := range objs {
		docIDs[i] = o.docID
		key := make([]byte, 8)
		binary.LittleEndian.PutUint64(key, o.docID)
		row, err := bucket.GetBySecondary(ctx, helpers.ObjectsBucketLSMDocIDSecondaryIndex, key)
		require.NoError(t, err)
		if row == nil {
			loss.secondaryNil++
			unresolvable = append(unresolvable, o.docID)
			continue
		}
		got, err := storobj.FromBinaryUUIDOnlyDisk(row, reuseTestClass)
		require.NoError(t, err)
		if got.ID() != o.id {
			loss.secondaryWrong++
		}
	}
	resolved, err := storobj.ObjectsByDocIDWithEmpty(bucket, docIDs, additional.Properties{}, nil, s.index.logger)
	release()
	require.NoError(t, err)
	for i, o := range resolved {
		if o == nil || o.ID() != objs[i].id {
			loss.byDocIDMissing++
		}
	}

	for _, o := range objs {
		found, _, err := s.ObjectVectorSearch(ctx, []models.Vector{o.vec}, []string{""}, 0, 1,
			nil, nil, nil, additional.Properties{}, nil, nil)
		require.NoError(t, err)
		if len(found) == 0 || found[0].ID() != o.id {
			loss.searchMissing++
		}
	}

	for _, id := range unresolvable {
		err := s.freeList.assertNoTraces(id)
		if err == nil || !strings.Contains(err.Error(), "an object row still references it") {
			loss.rowCheckDefeats++
		}
	}
	return loss
}

// TestDocIDReuseRepro_DuplicateSecondaryKeyHidesReusedObject pins that a live
// object on a reused docID stays resolvable once the old life's tombstone
// (which keeps the docID secondary key) and the new life's row share a segment.
func TestDocIDReuseRepro_DuplicateSecondaryKeyHidesReusedObject(t *testing.T) {
	const (
		anchors = 5
		n       = 100
		dims    = 16
	)

	tests := []struct {
		name             string
		flushAfterPut    bool
		flushAfterDelete bool
	}{
		{name: "put, delete and reuse in one memtable, single flush"},
		{name: "put in own segment, delete and reuse in the next, then compaction", flushAfterPut: true},
		{name: "delete and reuse in separate segments, then compaction", flushAfterDelete: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("DOCID_REUSE_ENABLED", "true")
			ctx := context.Background()
			s := reproShard(t, ctx)
			require.NoError(t, s.cycleCallbacks.compactionCallbacksCtrl.Deactivate(ctx))
			require.NoError(t, s.cycleCallbacks.compactionAuxCallbacksCtrl.Deactivate(ctx))
			require.NoError(t, s.cycleCallbacks.flushCallbacksCtrl.Deactivate(ctx))

			r := rand.New(rand.NewSource(42))
			randVec := func() []float32 {
				v := make([]float32, dims)
				for i := range v {
					v[i] = r.Float32()
				}
				return v
			}

			for i := 0; i < anchors; i++ {
				reusePutObject(t, s, randVec())
			}
			oldDocIDs := map[uint64]struct{}{}
			old := make([]strfmt.UUID, n)
			for i := range old {
				old[i] = reusePutObject(t, s, randVec())
				oldDocIDs[reuseDocIDOf(t, s, old[i])] = struct{}{}
			}
			if tt.flushAfterPut {
				reproFlushObjects(t, s)
			}

			for _, id := range old {
				require.NoError(t, s.DeleteObject(ctx, id, time.Time{}))
			}
			runTombstoneCleanups(t, s)
			require.True(t, runHarvest(s))
			if tt.flushAfterDelete {
				reproFlushObjects(t, s)
			}

			fresh := make([]reproObj, n)
			for i := range fresh {
				vec := randVec()
				id := reusePutObject(t, s, vec)
				fresh[i] = reproObj{id: id, vec: vec, docID: reuseDocIDOf(t, s, id)}
				require.Contains(t, oldDocIDs, fresh[i].docID, "new object %d must reuse a freed docID", i)
			}

			before := reproMeasureLoss(t, s, fresh)
			require.Equal(t, reproLoss{}, before, "control: every reused object resolvable before the merge")

			reproFlushObjects(t, s)
			if reproObjectsBucketSegments(s) > 1 {
				reproCompactToOneSegment(t, s)
			}

			after := reproMeasureLoss(t, s, fresh)
			t.Logf("after merge into one segment: %+v", after)
			assert.Zero(t, after.secondaryNil+after.secondaryWrong,
				"%d/%d live objects on reused docIDs unresolvable via the docID secondary index", after.secondaryNil+after.secondaryWrong, n)
			assert.Zero(t, after.byDocIDMissing,
				"%d/%d live objects on reused docIDs dropped by ObjectsByDocID", after.byDocIDMissing, n)
			assert.Zero(t, after.searchMissing,
				"%d/%d live objects on reused docIDs not returned by nearVector for their own vector", after.searchMissing, n)
			assert.Zero(t, after.rowCheckDefeats,
				"%d live docIDs pass assertNoTraces' object-row check", after.rowCheckDefeats)
		})
	}
}
```

</details>

### C2: adapters/repos/db/vector/hnsw/docid_reuse_retombstone_repro_test.go

<details><summary>show</summary>

```go
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
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

type reproObjectStore struct {
	mu   sync.Mutex
	vecs map[uint64][]float32
}

func (s *reproObjectStore) get(id uint64) ([]float32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.vecs[id]
	if !ok {
		return nil, storobj.NewErrNotFoundf(id, "object row deleted")
	}
	return v, nil
}

func (s *reproObjectStore) set(id uint64, v []float32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.vecs[id] = v
}

func (s *reproObjectStore) del(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.vecs, id)
}

type reproView struct{}

func (reproView) ReleaseView() {}

// reproRescoreGate parks the next rescore at its view acquisition, i.e. after
// the query collected its candidates and before it fetches their vectors.
type reproRescoreGate struct {
	armed   atomic.Bool
	reached chan struct{}
	release chan struct{}
}

func (g *reproRescoreGate) view() common.BucketView {
	if g.armed.CompareAndSwap(true, false) {
		close(g.reached)
		<-g.release
	}
	return reproView{}
}

func newReproBQIndex(t *testing.T, store *reproObjectStore, gate *reproRescoreGate) (*hnsw, *test.Hook) {
	t.Helper()
	logger, hook := test.NewNullLogger()
	uc := ent.UserConfig{}
	uc.SetDefaults()
	uc.MaxConnections = 16
	uc.EFConstruction = 64
	uc.EF = 64
	uc.VectorCacheMaxObjects = 1_000_000
	uc.BQ = ent.BQConfig{Enabled: true}

	lsm := testinghelpers.NewDummyStore(t)
	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "docid-reuse-retombstone-repro",
		Logger:                logger,
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return store.get(id)
		},
		GetViewThunk: gate.view,
		TempVectorForIDWithViewThunk: func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
			v, err := store.get(id)
			if err != nil {
				return nil, err
			}
			copy(container.Slice, v)
			return container.Slice[:len(v)], nil
		},
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
		AllocChecker:      memwatch.NewDummyMonitor(),
	}, uc, cyclemanager.NewCallbackGroupNoop(), lsm)
	require.NoError(t, err)
	t.Cleanup(func() {
		index.Shutdown(context.Background())
		lsm.Shutdown(context.Background())
	})
	return index, hook
}

// reproStaleRescoreAcrossFree runs a real BQ knn search whose rescore is
// parked after candidate collection, while victim is deleted, cleaned up and
// (by CleanForReuse) proven free; the search then resumes against the freed id.
func reproStaleRescoreAcrossFree(t *testing.T, index *hnsw, hook *test.Hook, store *reproObjectStore,
	gate *reproRescoreGate, vecs [][]float32, victim uint64,
) {
	t.Helper()
	ctx := context.Background()
	never := cyclemanager.ShouldAbortCallback(func() bool { return false })

	gate.armed.Store(true)
	searchDone := make(chan []uint64, 1)
	enterrors.GoWrapper(func() {
		ids, _, err := index.SearchByVector(ctx, vecs[victim], 5, nil)
		assert.NoError(t, err)
		searchDone <- ids
	}, index.logger)

	select {
	case <-gate.reached:
	case <-time.After(30 * time.Second):
		t.Fatal("search never reached rescore")
	}

	store.del(victim)
	require.NoError(t, index.Delete(victim))
	require.NoError(t, index.CleanUpTombstonedNodes(never))
	require.False(t, index.ContainsDoc(victim))
	require.True(t, index.CleanForReuse(victim), "victim must be clean (harvestable / acquirable) after cleanup")

	close(gate.release)
	select {
	case <-searchDone:
	case <-time.After(30 * time.Second):
		t.Fatal("search never finished")
	}

	rescoreHitVictim := false
	for _, e := range hook.AllEntries() {
		if e.Data["action"] == "rescore" && strings.Contains(e.Message, fmt.Sprintf("node %d", victim)) {
			rescoreHitVictim = true
		}
	}
	require.True(t, rescoreHitVictim, "the stale query must have rescored victim %d", victim)
}

func reproVectors(n, dims int) [][]float32 {
	r := rand.New(rand.NewSource(7))
	vecs := make([][]float32, n)
	for i := range vecs {
		vecs[i] = make([]float32, dims)
		for d := range vecs[i] {
			vecs[i][d] = r.Float32()*2 - 1
		}
	}
	return vecs
}

// TestDocIDReuseRepro_StaleRescoreRetombstonesFreedID pins that a query which
// collected a docID before it was freed must not re-dirty that id afterwards.
func TestDocIDReuseRepro_StaleRescoreRetombstonesFreedID(t *testing.T) {
	const (
		n      = 50
		dims   = 64
		victim = uint64(7)
	)
	vecs := reproVectors(n+1, dims)

	setup := func(t *testing.T) (*hnsw, *test.Hook, *reproObjectStore, *reproRescoreGate) {
		store := &reproObjectStore{vecs: map[uint64][]float32{}}
		gate := &reproRescoreGate{reached: make(chan struct{}), release: make(chan struct{})}
		index, hook := newReproBQIndex(t, store, gate)
		for i := 0; i < n; i++ {
			store.set(uint64(i), vecs[i])
			require.NoError(t, index.Add(context.Background(), uint64(i), vecs[i]))
		}
		require.True(t, index.compressed.Load())
		require.True(t, index.shouldRescore())
		require.NotEqual(t, victim, index.Entrypoint())
		return index, hook, store, gate
	}

	t.Run("freed id stays clean, so the next Acquire does not fail the insert", func(t *testing.T) {
		index, hook, store, gate := setup(t)
		reproStaleRescoreAcrossFree(t, index, hook, store, gate, vecs, victim)

		assert.False(t, index.hasTombstone(victim), "stale rescore re-added a tombstone on freed docID %d", victim)
		assert.True(t, index.CleanForReuse(victim),
			"freed docID %d no longer clean: Acquire's assertNoTraces would fail the user's insert", victim)
	})

	t.Run("new owner of the reused id stays searchable and survives cleanup", func(t *testing.T) {
		index, hook, store, gate := setup(t)
		reproStaleRescoreAcrossFree(t, index, hook, store, gate, vecs, victim)

		newVec := vecs[n]
		store.set(victim, newVec)
		require.NoError(t, index.Add(context.Background(), victim, newVec))

		ids, _, err := index.SearchByVector(context.Background(), newVec, 1, nil)
		require.NoError(t, err)
		assert.Equal(t, []uint64{victim}, ids, "new owner of docID %d not returned for its own vector", victim)

		require.NoError(t, index.CleanUpTombstonedNodes(func() bool { return false }))
		assert.True(t, index.ContainsDoc(victim),
			"tombstone cleanup removed the live new owner of docID %d from the vector index", victim)
	})
}
```

</details>
