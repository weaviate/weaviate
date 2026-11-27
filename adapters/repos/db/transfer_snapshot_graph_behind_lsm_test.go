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
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hfresh"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storobj"
	esync "github.com/weaviate/weaviate/entities/sync"
	entdynamic "github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	entflat "github.com/weaviate/weaviate/entities/vectorindex/flat"
	enthfresh "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// The shard seals its vector indexes before it flushes its memtables:
// haltForTransfer runs every index's PrepareForBackup (HNSW switches to a new
// commit log and leaves the active one out of ListFiles) and only then
// FlushMemtables. Whatever reaches the shard between the two lands on both
// sides of the cut: the object in the memtable the flush seals into the
// snapshot, its vector in the commit log the snapshot leaves out. Nothing
// re-indexes it on load: FillQueue and RepairIndex run only from the debug
// endpoints, and the async queue lists none of the chunks it processed while
// in maintenance mode.
//
// These tests pin that window. The hook stands for a concurrent client write
// or a queue worker landing between the seal and the flush; the journeys are
// the two consumers of the halt that copy a live shard. The HNSW and dynamic
// subtests fail. HFresh's centroid graph misses the same window, and
// repairOrphanPostings reassigns, on load, the vectors of every posting the
// persisted posting map lists without a centroid. On this branch that covers
// a split in the window, not the first insert into an empty index: the first
// posting is not in the persisted map yet (its analysis replays after the
// repair), so its vector stays unreachable until a later restart. main covers
// it with a check for posting 1 (cb7838de01).
func TestTransferSnapshotKeepsGraphInStepWithObjects(t *testing.T) {
	for _, journey := range transferJourneys() {
		t.Run(journey.name, func(t *testing.T) {
			t.Run("hnsw, sync indexing, insert after the seal", func(t *testing.T) {
				ctx := testCtx()
				shd, idx := testShardWithSettings(t, ctx, &models.Class{Class: "SealInsert"},
					enthnsw.NewDefaultUserConfig(), false, false)
				shard := underlyingShard(t, shd)

				r := rand.New(rand.NewSource(1))
				base := createRandomObjects(r, "SealInsert", 20, 4)
				putAll(t, shard, base)
				late := createRandomObjects(r, "SealInsert", 1, 4)[0]

				hookSeal(t, shard, "", func() {
					require.NoError(t, shard.PutObject(ctx, late))
				})
				restored := restoreShardFrom(t, idx, shard, journey.snapshot(t, idx, shard))

				total := len(base) + 1
				requireAllSearchable(t, restored, base, total)
				got := vectorStateOf(t, restored, late.ID(), late.Vector, total)
				require.True(t, got.inObjects, "the snapshot sealed the late object into the LSM store")
				assert.True(t, got.inGraph, "the snapshot carries the object but not its node in the graph")
				assert.True(t, got.inSearch, "the restored object is invisible to vector search")

				// nothing started by the load brings it back
				_, settled := foundOverTime(t, restored, []*storobj.Object{late}, total, 5*time.Second)
				assert.Equal(t, 1, settled, "five seconds after the load, the restored object is still invisible to vector search")
			})

			t.Run("hnsw, sync indexing, update to a new vector after the seal", func(t *testing.T) {
				ctx := testCtx()
				shd, idx := testShardWithSettings(t, ctx, &models.Class{Class: "SealUpdate"},
					enthnsw.NewDefaultUserConfig(), false, false)
				shard := underlyingShard(t, shd)

				r := rand.New(rand.NewSource(2))
				base := createRandomObjects(r, "SealUpdate", 20, 4)
				putAll(t, shard, base)
				// a new vector gives the object a new doc id: the old node goes, a new one comes
				updated := *base[0]
				updated.Vector = createRandomObjects(r, "SealUpdate", 1, 4)[0].Vector

				hookSeal(t, shard, "", func() {
					require.NoError(t, shard.PutObject(ctx, &updated))
				})
				restored := restoreShardFrom(t, idx, shard, journey.snapshot(t, idx, shard))

				requireAllSearchable(t, restored, base[1:], len(base))
				got := vectorStateOf(t, restored, updated.ID(), updated.Vector, len(base))
				require.True(t, got.inObjects, "the snapshot sealed the update into the LSM store")
				assert.True(t, got.inGraph, "the snapshot carries the update but not its node in the graph")
				assert.True(t, got.inSearch, "the updated object is invisible to vector search")
			})

			t.Run("hnsw, async indexing, backlog drained after the seal", func(t *testing.T) {
				ctx := testCtx()
				shd, idx := testShardWithSettings(t, ctx, &models.Class{Class: "SealBacklog"},
					enthnsw.NewDefaultUserConfig(), false, true)
				shard := underlyingShard(t, shd)

				backlog := createRandomObjects(rand.New(rand.NewSource(3)), "SealBacklog", 20, 4)
				hookBacklogDrain(t, shard, "", func() { putAll(t, shard, backlog) })
				restored := restoreShardFrom(t, idx, shard, journey.snapshot(t, idx, shard))
				waitQueueDrained(t, restored, "")

				assertSearchableCount(t, restored, backlog, len(backlog),
					"the backlog the queue drained after the seal is in neither the graph nor the queue the snapshot carries")

				// nothing started by the load brings it back
				_, settled := foundOverTime(t, restored, backlog, len(backlog), 5*time.Second)
				assert.Equalf(t, len(backlog), settled,
					"five seconds after the load, %d of %d backlog objects are found by vector search", settled, len(backlog))
			})

			t.Run("dynamic upgraded to hnsw, async indexing, backlog drained after the seal", func(t *testing.T) {
				ctx := testCtx()
				duc := entdynamic.UserConfig{}
				duc.SetDefaults()
				duc.Threshold = 10
				shd, idx := testShardWithSettings(t, ctx, &models.Class{Class: "SealDynamic"}, duc, false, true)
				shard := underlyingShard(t, shd)

				r := rand.New(rand.NewSource(4))
				base := createRandomObjects(r, "SealDynamic", 20, 4)
				putAll(t, shard, base)
				commitLog := filepath.Join(shard.path(), "main.hnsw.commitlog.d")
				flatBucket := filepath.Join(shard.pathLSM(), "vectors")
				require.Eventually(t, func() bool { return exists(commitLog) && !exists(flatBucket) },
					30*time.Second, 50*time.Millisecond, "the queue upgrades the index past the threshold")
				waitQueueDrained(t, shard, "")

				backlog := createRandomObjects(r, "SealDynamic", 20, 4)
				hookBacklogDrain(t, shard, "", func() { putAll(t, shard, backlog) })
				restored := restoreShardFrom(t, idx, shard, journey.snapshot(t, idx, shard))
				waitQueueDrained(t, restored, "")

				total := len(base) + len(backlog)
				requireAllSearchable(t, restored, base, total)
				assertSearchableCount(t, restored, backlog, total,
					"the backlog the queue drained after the seal is in neither the graph nor the queue the snapshot carries")
			})

			t.Run("hfresh, sync indexing, first insert after the seal", func(t *testing.T) {
				ctx := testCtx()
				shd, idx := testShardWithSettings(t, ctx, &models.Class{Class: "SealHFresh"},
					enthfresh.NewDefaultUserConfig(), false, false)
				shard := underlyingShard(t, shd)

				// the first insert creates the first posting (LSM) and registers its
				// centroid in the centroid HNSW (commit log)
				first := createRandomObjects(rand.New(rand.NewSource(5)), "SealHFresh", 1, 32)[0]
				hookSeal(t, shard, "", func() {
					require.NoError(t, shard.PutObject(ctx, first))
				})
				restored := restoreShardFrom(t, idx, shard, journey.snapshot(t, idx, shard))
				require.True(t, vectorStateOf(t, restored, first.ID(), first.Vector, 1).inObjects,
					"the snapshot sealed the first object into the LSM store")

				// Not covered on this branch: repairOrphanPostings only walks the
				// persisted posting map, which does not hold the first posting yet
				atLoad, settled := foundOverTime(t, restored, []*storobj.Object{first}, 1, 30*time.Second)
				t.Logf("found right after the load: %d of 1; after the repair: %d of 1", atLoad, settled)
				assert.Equal(t, 1, settled, "30 seconds after the load the first vector is still unreachable: the repair did not see its posting")
			})

			t.Run("hfresh, split processed after the seal", func(t *testing.T) {
				ctx := testCtx()
				cfg := enthfresh.NewDefaultUserConfig()
				cfg.MaxPostingSizeKB = enthfresh.MaxPostingSizeKBFloor // about 280 vectors of 32 dims a posting
				shd, idx := testShardWithSettings(t, ctx, &models.Class{Class: "SealSplit"}, cfg, false, false)
				shard := underlyingShard(t, shd)

				// the split queue holds the split the inserts below ask for
				splitQueue := fmt.Sprintf("hfresh_split_queue_%s_%s", shard.name, "main")
				shard.scheduler.PauseQueue(splitQueue)
				require.True(t, shard.scheduler.IsQueuePaused(splitQueue), "the split queue is %q", splitQueue)
				objs := createRandomObjects(rand.New(rand.NewSource(8)), "SealSplit", 400, 32)
				putAll(t, shard, objs)
				before := settleHFresh(t, shard)
				require.Equal(t, 1, before, "one oversized posting waits for its split")

				// HFresh.PrepareForBackup sealed the centroid commit log, then resumed
				// the split queue: the split runs before the shard flushes
				splitDir := filepath.Join(shard.path(), "main.hfresh.d", "split.queue.d")
				hookSeal(t, shard, "", func() {
					require.Eventually(t, func() bool {
						processed, _ := filepath.Glob(filepath.Join(splitDir, "chunk-*.bin.processed"))
						return len(processed) > 0
					}, 30*time.Second, 10*time.Millisecond, "the split runs once the split queue's PrepareForBackup resumed it")
					require.NoError(t, shard.scheduler.Wait(ctx, splitQueue))
				})
				restored := restoreShardFrom(t, idx, shard, journey.snapshot(t, idx, shard))

				// Covered: the snapshot carries the split's postings (LSM) and the old
				// centroid, not the new ones (the active centroid commit log), and
				// repairOrphanPostings (hfresh.go:227) reassigns their vectors
				atLoad, settled := foundOverTime(t, restored, objs, 10, 30*time.Second)
				t.Logf("found right after the load: %d of %d; after the repair: %d of %d", atLoad, len(objs), settled, len(objs))
				assert.GreaterOrEqualf(t, settled, len(objs)*95/100,
					"after the startup repair %d of %d are found by vector search", settled, len(objs))
			})
		})
	}
}

// The geo index is an HNSW of its own with its own queue, sealed by its own
// PrepareForBackup after the vector indexes and before the flush, and listed by
// the same ListFiles that leaves the active commit log out. Its queue resumes
// at its PrepareForBackup like the vector queue. A backlog the workers drain
// between the geo seal and the geo queue's ForceSwitch is in neither the geo
// commit logs nor the queue chunks the snapshot lists. The wait between the
// halt and the listing stands for that window (FlushMemtables, the store's
// and the indexes' listings).
func TestTransferSnapshotKeepsGeoIndexInStepWithObjects(t *testing.T) {
	ctx := testCtx()
	class := &models.Class{Class: "SealGeo", Properties: []*models.Property{{
		Name:     "location",
		DataType: schema.DataTypeGeoCoordinates.PropString(),
	}}}
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, true)
	shard := underlyingShard(t, shd)

	var q *VectorIndexQueue
	require.NoError(t, shard.ForEachGeoQueue(func(propName string, gq *VectorIndexQueue) error {
		if propName == "location" {
			q = gq
		}
		return nil
	}))
	require.NotNil(t, q, "async indexing gives the geo property a queue")

	require.NoError(t, q.Pause(ctx))
	for _, at := range []*models.GeoCoordinates{geoAt(52.520, 13.405), geoAt(48.137, 11.575), geoAt(53.551, 9.993)} {
		require.NoError(t, shard.PutObject(ctx, &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:         strfmt.UUID(uuid.NewString()),
				Class:      class.Class,
				Properties: map[string]interface{}{"location": at},
			},
		}))
	}
	require.NoError(t, q.Flush())
	require.NotZero(t, q.Size(), "the backlog is queued, not indexed")

	require.NoError(t, shard.HaltForTransfer(ctx, false, 0))
	defer func() { require.NoError(t, shard.resumeMaintenanceCycles(ctx)) }()
	require.Eventually(t, func() bool { return q.Size() == 0 }, 30*time.Second, 10*time.Millisecond,
		"the workers drain the backlog once the geo queue's PrepareForBackup resumed it")
	require.NoError(t, q.Wait(ctx))
	require.Len(t, allWithinRange(t, ctx, liveGeoIndex(t, shard, "location")), 3,
		"the live geo index holds the drained backlog")

	files, err := shard.ListBackupFiles(ctx, &backup.ShardDescriptor{})
	require.NoError(t, err)
	restoredShardPath := restoreBackupFiles(t, idx.Config.RootPath, shard.path(), files)
	restored := openGeoIndexAt(t, ctx, shard, "location", restoredShardPath)

	geoQueueDir := fmt.Sprintf("%s.queue.d", geoPropID("location"))
	var queued int
	for _, rel := range files {
		if filepath.Base(filepath.Dir(rel)) == geoQueueDir {
			queued++
		}
	}
	assert.Lenf(t, allWithinRange(t, ctx, restored), 3,
		"the snapshot's geo commit logs miss the drained backlog, and it lists %d geo queue chunks to replay it", queued)
}

// The control: a quiesced shard, nothing in the window. Every journey carries
// the graph with the objects, so the red subtests above fail for the window
// alone, not for the harness.
func TestTransferSnapshotControlWithoutWindow(t *testing.T) {
	for _, journey := range transferJourneys() {
		t.Run(journey.name, func(t *testing.T) {
			hfreshSmall := enthfresh.NewDefaultUserConfig()
			hfreshSmall.MaxPostingSizeKB = enthfresh.MaxPostingSizeKBFloor

			for _, tc := range []struct {
				name  string
				cfg   schemaConfig.VectorIndexConfig
				async bool
				dims  int
				n     int
			}{
				{name: "hnsw sync", cfg: enthnsw.NewDefaultUserConfig(), dims: 4, n: 20},
				{name: "hnsw async", cfg: enthnsw.NewDefaultUserConfig(), async: true, dims: 4, n: 20},
				{name: "hfresh sync", cfg: enthfresh.NewDefaultUserConfig(), dims: 32, n: 20},
				{name: "hfresh sync, split before the transfer", cfg: hfreshSmall, dims: 32, n: 400},
				{name: "flat sync", cfg: flatUserConfig(), dims: 4, n: 20},
			} {
				t.Run(tc.name, func(t *testing.T) {
					ctx := testCtx()
					shd, idx := testShardWithSettings(t, ctx, &models.Class{Class: "SealControl"}, tc.cfg, false, tc.async)
					shard := underlyingShard(t, shd)

					objs := createRandomObjects(rand.New(rand.NewSource(7)), "SealControl", tc.n, tc.dims)
					putAll(t, shard, objs)
					waitQueueDrained(t, shard, "")
					if tc.n > 20 {
						vi, release, ok := shard.AcquireVectorIndex("")
						require.True(t, ok)
						h := vi.(*hfresh.HFresh)
						release()
						require.Eventually(t, func() bool { return h.PostingMap.Size() >= 2 }, 30*time.Second, 10*time.Millisecond,
							"the oversized posting splits before the transfer")
					}

					restored := restoreShardFrom(t, idx, shard, journey.snapshot(t, idx, shard))
					waitQueueDrained(t, restored, "")
					settleHFresh(t, restored)
					if tc.n <= 20 {
						requireAllSearchable(t, restored, objs, len(objs))
						return
					}
					// Right after the load HFresh can search nothing in a
					// posting whose analysis had not run before the snapshot
					// (TestHFreshSearchesRightAfterReopen pins it): the
					// control gives the replay the load starts its time, so
					// that it does not fail on that gap one run in five.
					_, found := foundOverTime(t, restored, objs, 10, 30*time.Second)
					require.GreaterOrEqualf(t, found, len(objs)*95/100, "%d of %d found by vector search", found, len(objs))
				})
			}
		})
	}
}

type transferJourney struct {
	name string
	// snapshot takes the journey's snapshot of a live shard and returns what
	// it delivers: every file, by its path relative to the shard directory.
	snapshot func(t *testing.T, idx *Index, shard *Shard) map[string][]byte
}

func transferJourneys() []transferJourney {
	return []transferJourney{
		{name: "backup", snapshot: backupSnapshotFiles},
		{name: "replica movement", snapshot: replicaSnapshotFiles},
	}
}

// backupSnapshotFiles takes what a backup of the live shard uploads: the
// staged files and the three files the descriptor carries in memory.
func backupSnapshotFiles(t *testing.T, idx *Index, shard *Shard) map[string][]byte {
	t.Helper()
	ctx := context.Background()

	staging := t.TempDir()
	var sd backup.ShardDescriptor
	files, err := shard.CreateBackupSnapshot(ctx, &sd, staging, newReindexGate(idx.db, reindexGateMaxAge))
	require.NoError(t, err)

	out := map[string][]byte{}
	add := func(rootRel string, data []byte) {
		rel, err := shard.shardRelativePath(rootRel)
		require.NoError(t, err)
		out[rel] = data
	}
	for _, rootRel := range files {
		data, err := os.ReadFile(filepath.Join(staging, rootRel))
		require.NoError(t, err)
		add(rootRel, data)
	}
	add(sd.DocIDCounterPath, sd.DocIDCounter)
	add(sd.PropLengthTrackerPath, sd.PropLengthTracker)
	add(sd.ShardVersionPath, sd.Version)
	return out
}

// replicaSnapshotFiles takes what the copier of a replica movement fetches:
// the snapshot's file list, file by file.
func replicaSnapshotFiles(t *testing.T, idx *Index, shard *Shard) map[string][]byte {
	t.Helper()
	ctx := context.Background()

	if idx.replicaSnapshotOpLocks == nil {
		idx.replicaSnapshotOpLocks = esync.NewKeyRWLocker()
	}
	const opID = "move"
	files, err := idx.IncomingCreateReplicaSnapshot(ctx, shard.name, opID)
	require.NoError(t, err)

	out := map[string][]byte{}
	for _, rel := range files {
		rc, err := idx.IncomingGetReplicaSnapshotFile(ctx, opID, rel)
		require.NoError(t, err)
		data, err := io.ReadAll(rc)
		require.NoError(t, err)
		require.NoError(t, rc.Close())
		out[rel] = data
	}
	require.NoError(t, idx.IncomingReleaseReplicaSnapshot(ctx, opID))
	return out
}

// restoreShardFrom replaces the shard's directory with exactly the delivered
// files and loads it, as a restore or the target of a movement does.
func restoreShardFrom(t *testing.T, idx *Index, shard *Shard, files map[string][]byte) *Shard {
	t.Helper()
	ctx := context.Background()

	dir := shard.path()
	require.NoError(t, shard.Shutdown(ctx))
	require.NoError(t, os.RemoveAll(dir))
	for rel, data := range files {
		path := filepath.Join(dir, rel)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, data, 0o644))
	}
	restored, err := idx.initShard(ctx, shard.name, idx.getClass(), nil, true, true)
	require.NoError(t, err)
	idx.shards.Store(shard.name, restored)
	return underlyingShard(t, restored)
}

// sealHookIndex runs afterSeal once, right after the wrapped index's
// PrepareForBackup: the index has sealed what it will list, and the shard has
// not flushed its memtables yet.
type sealHookIndex struct {
	VectorIndex
	afterSeal func()
}

func (i *sealHookIndex) PrepareForBackup(ctx context.Context) error {
	if err := i.VectorIndex.PrepareForBackup(ctx); err != nil {
		return err
	}
	if f := i.afterSeal; f != nil {
		i.afterSeal = nil
		f()
	}
	return nil
}

func hookSeal(t *testing.T, shard *Shard, targetVector string, afterSeal func()) {
	t.Helper()
	inner, release, ok := shard.AcquireVectorIndex(targetVector)
	require.True(t, ok)
	release()
	shard.setVectorIndex(targetVector, &sealHookIndex{VectorIndex: inner, afterSeal: afterSeal})
}

// hookBacklogDrain leaves a backlog in the async queue when the transfer
// starts: fill runs while the queue is paused. The queue's own
// PrepareForBackup resumes it before the index seals, and the hook waits,
// after the seal, until the workers have drained it. With the scheduler
// ticking once a second, that is where the workers drain it anyway.
func hookBacklogDrain(t *testing.T, shard *Shard, targetVector string, fill func()) {
	t.Helper()
	ctx := context.Background()

	q, release, ok := shard.AcquireVectorIndexQueue(targetVector)
	require.True(t, ok)
	release()

	require.NoError(t, q.Pause(ctx))
	fill()
	require.NoError(t, q.Flush())
	require.NotZero(t, q.Size(), "the backlog is queued, not indexed")

	hookSeal(t, shard, targetVector, func() {
		require.Eventually(t, func() bool { return q.Size() == 0 }, 30*time.Second, 10*time.Millisecond,
			"the workers drain the backlog once the queue's PrepareForBackup resumed it")
		require.NoError(t, q.Wait(ctx))
	})
}

// settleHFresh waits, when the shard's vector index is HFresh, until its
// posting map holds a posting again and returns how many it holds. A reopened
// HFresh rebuilds the posting map of the postings its last analyses had not
// persisted by replaying its analyze queue on the scheduler, and searches
// nothing in between (TestHFreshSearchesRightAfterReopen). It does not fail
// when nothing comes: the assertions after it say what is missing.
func settleHFresh(t *testing.T, shard *Shard) int {
	t.Helper()
	vi, release, ok := shard.AcquireVectorIndex("")
	require.True(t, ok)
	defer release()
	h, ok := vi.(*hfresh.HFresh)
	if !ok {
		return 0
	}
	deadline := time.Now().Add(10 * time.Second)
	for h.PostingMap.Size() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	return h.PostingMap.Size()
}

// foundOverTime counts the objects a vector search for their own vector
// returns among the k nearest, right away and then again until all are found
// or within passes: the background work a load starts (HFresh's analyze
// replay and orphan repair) gets that long to bring them back.
func foundOverTime(t *testing.T, shard *Shard, objs []*storobj.Object, k int, within time.Duration) (atLoad, settled int) {
	t.Helper()
	count := func() int {
		n := 0
		for _, o := range objs {
			if vectorStateOf(t, shard, o.ID(), o.Vector, k).inSearch {
				n++
			}
		}
		return n
	}
	atLoad = count()
	settled = atLoad
	for deadline := time.Now().Add(within); settled < len(objs) && time.Now().Before(deadline); {
		time.Sleep(100 * time.Millisecond)
		settled = count()
	}
	return atLoad, settled
}

func waitQueueDrained(t *testing.T, shard *Shard, targetVector string) {
	t.Helper()
	q, release, ok := shard.AcquireVectorIndexQueue(targetVector)
	require.True(t, ok)
	defer release()
	require.Eventually(t, func() bool { return q.Size() == 0 }, 30*time.Second, 10*time.Millisecond)
	require.NoError(t, q.Wait(context.Background()))
}

func putAll(t *testing.T, shard *Shard, objs []*storobj.Object) {
	t.Helper()
	for _, err := range shard.PutObjectBatch(context.Background(), objs) {
		require.NoError(t, err)
	}
}

type vectorState struct {
	inObjects bool // the object is in the LSM store
	inGraph   bool // the vector index holds its doc id
	inSearch  bool // a vector search over every object returns it
}

func vectorStateOf(t *testing.T, shard *Shard, id strfmt.UUID, vector []float32, total int) vectorState {
	t.Helper()
	ctx := context.Background()

	var got vectorState
	obj, err := shard.ObjectByID(ctx, id, nil, additional.Properties{})
	require.NoError(t, err)
	if obj == nil {
		return got
	}
	got.inObjects = true

	vi, release, ok := shard.AcquireVectorIndex("")
	require.True(t, ok)
	got.inGraph = vi.ContainsDoc(obj.DocID)
	release()

	res, _, err := shard.ObjectVectorSearch(ctx, []models.Vector{vector}, []string{""}, 0, total,
		nil, nil, nil, additional.Properties{}, nil, nil)
	require.NoError(t, err)
	for _, o := range res {
		if o.ID() == id {
			got.inSearch = true
		}
	}
	return got
}

func requireAllSearchable(t *testing.T, shard *Shard, objs []*storobj.Object, total int) {
	t.Helper()
	for _, o := range objs {
		got := vectorStateOf(t, shard, o.ID(), o.Vector, total)
		require.Equalf(t, vectorState{inObjects: true, inGraph: true, inSearch: true}, got,
			"object %s, in the snapshot before the window, is restored whole", o.ID())
	}
}

func assertSearchableCount(t *testing.T, shard *Shard, objs []*storobj.Object, total int, msg string) {
	t.Helper()
	var inObjects, inGraph, inSearch int
	for _, o := range objs {
		got := vectorStateOf(t, shard, o.ID(), o.Vector, total)
		if got.inObjects {
			inObjects++
		}
		if got.inGraph {
			inGraph++
		}
		if got.inSearch {
			inSearch++
		}
	}
	require.Equal(t, len(objs), inObjects, "every backlog object was written before the transfer: the LSM store carries it")
	assert.Equalf(t, len(objs), inGraph, "%s: %d of %d in the graph", msg, inGraph, len(objs))
	assert.Equalf(t, len(objs), inSearch, "%s: %d of %d found by vector search", msg, inSearch, len(objs))
}

func flatUserConfig() entflat.UserConfig {
	cfg := entflat.UserConfig{}
	cfg.SetDefaults()
	cfg.Distance = distancer.NewL2SquaredProvider().Type()
	return cfg
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
