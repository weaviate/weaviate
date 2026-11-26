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
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/postingedge"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	esync "github.com/weaviate/weaviate/entities/sync"
	postingedgeent "github.com/weaviate/weaviate/entities/vectorindex/postingedge"
)

// A backup and a replica movement of a shard with a posting-edge index,
// taken while the shard goes on writing and restored, as a restore or the
// target of a movement loads them. The shard prepares its indexes, flushes
// its store, lists its files, has its indexes copy what they change in place
// and links the rest (Shard.haltForTransfer, CreateBackupSnapshot,
// collectShardRelativeFiles), and it does not stop its writes meanwhile.
//
//   - An object written between the PrepareForBackup of the index and the
//     flush of the store (#14003, where HNSW loses its node) is in the
//     snapshot with its vector: the buckets of the index hold what the flush
//     wrote, and its journal what came after.
//   - An object written after the listing and before the index copies its
//     files is not in the snapshot, since its memtable was not flushed, and
//     neither may its vector be: an index restored with a vector whose object
//     is not there holds a doc id that the doc id counter of the snapshot,
//     read before the listing, gives out again, and the object that gets it
//     cannot be indexed.
//
// The second is red, on purpose, and escalated: the copy of the files of
// the index is taken after the doc id counter is read (ListBackupFiles reads
// the counter first, and the shard has the indexes copy their files after
// listing), so the restored index holds the vector of an object written in
// between, and the counter gives its doc id out again: the put of the next
// object fails with "id ... was already added". No cut the index can take by
// itself avoids it: one before the flush of the store is not the state of
// its buckets in the snapshot, and every one after it comes after the
// counter is read. The order of the shard is the team's to decide (#14003),
// so nothing here changes it.
func TestPostingEdge_TransferSnapshotRestoresTheIndex(t *testing.T) {
	for _, journey := range []struct {
		name     string
		snapshot func(t *testing.T, idx *Index, shard *Shard) map[string][]byte
	}{
		{name: "backup", snapshot: postingEdgeBackupFiles},
		{name: "replica movement", snapshot: postingEdgeReplicaFiles},
		{
			name: "replica movement, halted for the transfer",
			snapshot: func(t *testing.T, idx *Index, shard *Shard) map[string][]byte {
				t.Setenv("WEAVIATE_TEST_FORCE_NO_HARDLINK", "true")
				return postingEdgeReplicaFiles(t, idx, shard)
			},
		},
	} {
		t.Run(journey.name, func(t *testing.T) {
			const (
				class = "PostingEdgeTransfer"
				dims  = 32
				n     = 400
				k     = 10
			)
			ctx := context.Background()
			uc := postingedgeent.NewDefaultUserConfig()
			uc.MaxPostingSizeKB = postingedgeent.MinimumMaxPostingSizeKB
			shd, idx := testShardWithSettings(t, ctx, &models.Class{Class: class}, uc, false, false,
				func(i *Index) { i.Config.PostingEdgeBaseSize = 150 })
			shard := underlyingShard(t, shd)

			r := rand.New(rand.NewSource(11))
			for _, err := range shard.PutObjectBatch(ctx, createRandomObjects(r, class, n, dims)) {
				require.NoError(t, err)
			}
			dirs, err := filepath.Glob(filepath.Join(shard.path(), "*.postingedge.d"))
			require.NoError(t, err)
			require.Len(t, dirs, 1)
			require.Eventually(t, func() bool {
				_, err := os.Stat(filepath.Join(dirs[0], "meta.json"))
				return err == nil
			}, 30*time.Second, 10*time.Millisecond, "the index builds its base")

			sealed := createRandomObjects(r, class, 1, dims)[0]
			late := createRandomObjects(r, class, 1, dims)[0]
			inner, release, ok := shard.AcquireVectorIndex("")
			require.True(t, ok)
			release()
			// The vectors that came while the base was built are taken by the
			// index in the background: its answers change until they are.
			persistent, ok := inner.(*postingedge.Persistent)
			require.True(t, ok, "the vector index of the class is a %T", inner)
			require.NoError(t, persistent.Settle(ctx))
			shard.setVectorIndex("", &postingEdgeTransferHook{
				VectorIndex:  inner,
				afterPrepare: func() { require.NoError(t, shard.PutObject(ctx, sealed)) },
				beforeCopy:   func() { require.NoError(t, shard.PutObject(ctx, late)) },
			})
			files := journey.snapshot(t, idx, shard)

			docID := func(s *Shard, o *storobj.Object) (uint64, bool) {
				got, err := s.ObjectByID(ctx, o.ID(), nil, additional.Properties{})
				require.NoError(t, err)
				if got == nil {
					return 0, false
				}
				return got.DocID, true
			}
			sealedID, ok := docID(shard, sealed)
			require.True(t, ok)
			lateID, ok := docID(shard, late)
			require.True(t, ok)
			queries := createRandomObjects(r, class, 20, dims)
			search := func(vi VectorIndex, vector []float32) []uint64 {
				ids, _, err := vi.SearchByVector(ctx, vector, k, nil)
				require.NoError(t, err)
				return ids
			}
			var live [][]uint64
			for _, q := range queries {
				live = append(live, search(inner, q.Vector))
			}

			restored := restorePostingEdgeShardFrom(t, idx, shard, files)
			vi, release, ok := restored.AcquireVectorIndex("")
			require.True(t, ok)
			defer release()

			// The answers are those the shard gave when it was taken.
			for i, q := range queries {
				assert.Equal(t, live[i], search(vi, q.Vector), "query %d", i)
			}

			// Between the seal and the flush.
			got, ok := docID(restored, sealed)
			require.True(t, ok, "the object written between the seal and the flush is in the snapshot")
			require.Equal(t, sealedID, got)
			assert.True(t, vi.ContainsDoc(sealedID), "and so is its vector")
			assert.Contains(t, search(vi, sealed.Vector), sealedID, "it is found by its vector")

			// After the listing.
			_, ok = docID(restored, late)
			require.False(t, ok, "the object written after the listing is not in the snapshot")
			assert.False(t, vi.ContainsDoc(lateID),
				"the index holds the vector of doc %d, whose object the snapshot does not carry", lateID)
			next := createRandomObjects(r, class, 1, dims)[0]
			assert.NoError(t, restored.PutObject(ctx, next), "an object put after the restore is indexed")
		})
	}
}

// An object written between the flush of the store, which ends the halt for
// the transfer, and the read of the doc id counter, which starts the listing,
// is not in the snapshot: its memtable was not flushed. Neither may its vector
// be. An index restored with it holds a vector whose object is not there,
// which a search drops from its results, so a query returns fewer than it
// asked for, and which no delete ever takes out. Its doc id is under the
// counter of the snapshot, so unlike the write after the listing
// (TestPostingEdge_TransferSnapshotRestoresTheIndex) it is not given out
// again. Red on purpose: the index copies its journal after the listing, and
// what the journal tells goes back into the index when it opens.
func TestPostingEdge_TransferSnapshotWriteBetweenFlushAndCounter(t *testing.T) {
	// Each journey halts the shard as its transfer does, writes, and takes
	// the rest of the snapshot as the transfer takes it.
	for _, journey := range []struct {
		name string
		take func(t *testing.T, idx *Index, shard *Shard, write func()) map[string][]byte
	}{
		{name: "backup", take: postingEdgeBackupFilesAfterHalt},
		{
			name: "replica movement",
			take: func(t *testing.T, _ *Index, shard *Shard, write func()) map[string][]byte {
				return postingEdgeReplicaFilesAfterHalt(t, shard, true, write)
			},
		},
		{
			name: "replica movement, halted for the transfer",
			take: func(t *testing.T, _ *Index, shard *Shard, write func()) map[string][]byte {
				t.Setenv("WEAVIATE_TEST_FORCE_NO_HARDLINK", "true")
				return postingEdgeReplicaFilesAfterHalt(t, shard, false, write)
			},
		},
	} {
		t.Run(journey.name, func(t *testing.T) {
			const (
				class = "PostingEdgeTransferFlushCounter"
				dims  = 32
				n     = 400
			)
			ctx := context.Background()
			uc := postingedgeent.NewDefaultUserConfig()
			uc.MaxPostingSizeKB = postingedgeent.MinimumMaxPostingSizeKB
			shd, idx := testShardWithSettings(t, ctx, &models.Class{Class: class}, uc, false, false,
				func(i *Index) { i.Config.PostingEdgeBaseSize = 150 })
			shard := underlyingShard(t, shd)
			r := rand.New(rand.NewSource(12))
			for _, err := range shard.PutObjectBatch(ctx, createRandomObjects(r, class, n, dims)) {
				require.NoError(t, err)
			}
			dirs, err := filepath.Glob(filepath.Join(shard.path(), "*.postingedge.d"))
			require.NoError(t, err)
			require.Len(t, dirs, 1)
			require.Eventually(t, func() bool {
				_, err := os.Stat(filepath.Join(dirs[0], "meta.json"))
				return err == nil
			}, 30*time.Second, 10*time.Millisecond, "the index builds its base")
			inner, release, ok := shard.AcquireVectorIndex("")
			require.True(t, ok)
			release()
			persistent, ok := inner.(*postingedge.Persistent)
			require.True(t, ok, "the vector index of the class is a %T", inner)
			require.NoError(t, persistent.Settle(ctx))

			between := createRandomObjects(r, class, 1, dims)[0]
			files := journey.take(t, idx, shard, func() { require.NoError(t, shard.PutObject(ctx, between)) })
			docID := func(s *Shard, o *storobj.Object) (uint64, bool) {
				got, err := s.ObjectByID(ctx, o.ID(), nil, additional.Properties{})
				require.NoError(t, err)
				if got == nil {
					return 0, false
				}
				return got.DocID, true
			}
			betweenID, ok := docID(shard, between)
			require.True(t, ok)

			restored := restorePostingEdgeShardFrom(t, idx, shard, files)
			vi, release, ok := restored.AcquireVectorIndex("")
			require.True(t, ok)
			defer release()
			_, ok = docID(restored, between)
			require.False(t, ok, "the object written between the flush and the counter is not in the snapshot")
			assert.False(t, vi.ContainsDoc(betweenID),
				"the index holds the vector of doc %d, whose object the snapshot does not carry", betweenID)
			next := createRandomObjects(r, class, 1, dims)[0]
			require.NoError(t, restored.PutObject(ctx, next), "an object put after the restore is indexed")
			nextID, ok := docID(restored, next)
			require.True(t, ok)
			assert.Greater(t, nextID, betweenID, "the counter of the snapshot does not give doc %d out again", betweenID)
		})
	}
}

// postingEdgeBackupFilesAfterHalt takes what a backup of the live shard
// uploads, as CreateBackupSnapshot does, calling write between the halt for
// the transfer, which flushes the store, and the listing, which reads the doc
// id counter first.
func postingEdgeBackupFilesAfterHalt(t *testing.T, idx *Index, shard *Shard, write func()) map[string][]byte {
	t.Helper()
	ctx := context.Background()
	staging := t.TempDir()
	require.NoError(t, shard.haltForTransfer(ctx, false, 0, newReindexGate(idx.db, reindexGateMaxAge)))
	write()
	var sd backup.ShardDescriptor
	files, err := shard.stageBackupSnapshot(ctx, &sd, staging)
	require.NoError(t, err)
	require.NoError(t, shard.resumeMaintenanceCycles(ctx))
	return postingEdgeStagedBackup(t, shard, staging, files, &sd)
}

// postingEdgeReplicaFilesAfterHalt takes what the copier of a replica
// movement fetches, as CreateReplicaSnapshot does, calling write between the
// halt and the listing. A file that is not staged is read from the shard
// while it is halted, as a target reads it in halt-for-duration mode.
func postingEdgeReplicaFilesAfterHalt(t *testing.T, shard *Shard, hardlink bool, write func()) map[string][]byte {
	t.Helper()
	ctx := context.Background()
	staging := t.TempDir()
	require.NoError(t, shard.HaltForTransfer(ctx, false, 0))
	write()
	rels, err := shard.collectShardRelativeFiles(ctx, staging, hardlink)
	require.NoError(t, err)
	out := map[string][]byte{}
	for _, rel := range rels {
		data, err := os.ReadFile(filepath.Join(staging, rel))
		if os.IsNotExist(err) {
			data, err = os.ReadFile(filepath.Join(shard.path(), rel))
		}
		require.NoError(t, err)
		out[rel] = data
	}
	require.NoError(t, shard.resumeMaintenanceCycles(ctx))
	return out
}

// postingEdgeTransferHook writes to the shard at two points of a transfer:
// right after the PrepareForBackup of the index, before the shard flushes its
// store, and right before the index copies its files, after the shard listed
// them. Each runs once.
type postingEdgeTransferHook struct {
	VectorIndex
	afterPrepare, beforeCopy func()
}

func (h *postingEdgeTransferHook) PrepareForBackup(ctx context.Context) error {
	if err := h.VectorIndex.PrepareForBackup(ctx); err != nil {
		return err
	}
	if f := h.afterPrepare; f != nil {
		h.afterPrepare = nil
		f()
	}
	return nil
}

func (h *postingEdgeTransferHook) SnapshotMutableFiles(ctx context.Context, basePath, stagingDir string) ([]string, error) {
	if f := h.beforeCopy; f != nil {
		h.beforeCopy = nil
		f()
	}
	return h.VectorIndex.SnapshotMutableFiles(ctx, basePath, stagingDir)
}

// postingEdgeBackupFiles takes what a backup of the live shard uploads: the
// staged files and the three files the descriptor carries in memory, by
// their paths relative to the shard.
func postingEdgeBackupFiles(t *testing.T, idx *Index, shard *Shard) map[string][]byte {
	t.Helper()
	staging := t.TempDir()
	var sd backup.ShardDescriptor
	files, err := shard.CreateBackupSnapshot(context.Background(), &sd, staging, newReindexGate(idx.db, reindexGateMaxAge))
	require.NoError(t, err)
	return postingEdgeStagedBackup(t, shard, staging, files, &sd)
}

// postingEdgeStagedBackup is what a backup uploads, by path relative to the
// shard: the files staged and the three the descriptor carries in memory.
func postingEdgeStagedBackup(t *testing.T, shard *Shard, staging string, files []string, sd *backup.ShardDescriptor,
) map[string][]byte {
	t.Helper()
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

// postingEdgeReplicaFiles takes what the copier of a replica movement
// fetches: the files of the snapshot, one by one.
func postingEdgeReplicaFiles(t *testing.T, idx *Index, shard *Shard) map[string][]byte {
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

// restorePostingEdgeShardFrom replaces the directory of the shard with the
// files delivered and loads it, as a restore or the target of a movement
// does.
func restorePostingEdgeShardFrom(t *testing.T, idx *Index, shard *Shard, files map[string][]byte) *Shard {
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
