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

package tachiomfresh

import (
	"io"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"go.uber.org/goleak"
)

// TestCrashAfterSplitOrMergeKeepsEveryToken runs a split or a merge, flushes
// the centroid HNSW's commit log only, and copies the index's directory as a
// crash would leave it: what the buffered writers still hold is not in the
// copy. The index reopened from the copy must hold every token in exactly
// one posting.
func TestCrashAfterSplitOrMergeKeepsEveryToken(t *testing.T) {
	tests := []struct {
		name string
		// run builds the postings and returns the token documents and the
		// operation to run on them
		run func(t *testing.T, tf *testIndex) (map[uint64][][]float32, func() error)
	}{
		{
			name: "split",
			run: func(t *testing.T, tf *testIndex) (map[uint64][][]float32, func() error) {
				vectors := createTestVectors(4, 15)
				postingID, refs := createPostingWithTokens(t, tf, vectors, 200)
				registerPosting(t, tf, postingID, refs, meanVector(vectors))
				return oneTokenDocs(vectors, 200), func() error {
					return tf.Index.doSplit(t.Context(), postingID, false)
				}
			},
		},
		{
			name: "merge",
			run: func(t *testing.T, tf *testIndex) (map[uint64][][]float32, func() error) {
				tf.Index.minPostingSize = 5
				tf.Index.maxPostingSize = 10
				small := createTestVectors(4, 3)
				smallID, smallRefs := createPostingWithTokens(t, tf, small, 300)
				large := createTestVectors(4, 4)
				largeID, largeRefs := createPostingWithTokens(t, tf, large, 400)
				registerPosting(t, tf, smallID, smallRefs, meanVector(small))
				registerPosting(t, tf, largeID, largeRefs, meanVector(large))
				docs := oneTokenDocs(small, 300)
				for id, doc := range oneTokenDocs(large, 400) {
					docs[id] = doc
				}
				return docs, func() error {
					return tf.Index.doMerge(t.Context(), smallID)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			open := func(dir string) testIndex {
				return newTestIndexAt(t, dir, 10, 8, 8, hfreshEFConstruction,
					withDistanceProvider(distancer.NewDotProductProvider()))
			}

			tf := open(dir)
			tf.Index.opts.disableReassign = true
			docs, operation := tt.run(t, &tf)

			// everything before the operation is on disk, as after the
			// shard's write of the WALs at the end of a batch
			require.NoError(t, tf.store.WriteWALs())
			require.NoError(t, tf.Index.Centroids.hnsw.Flush())

			require.NoError(t, operation())
			checkPlacement(t, &tf, docs)

			require.NoError(t, tf.Index.Centroids.hnsw.Flush())
			crashed := t.TempDir()
			copyDir(t, dir, crashed)
			tf.close(t)

			reopened := open(crashed)
			for id, doc := range docs {
				reopened.mvStore.storeMultiVector(id, doc)
			}
			checkPlacement(t, &reopened, docs)
			reopened.close(t)
		})
	}
}

// checkPlacement is the placement part of checkInvariants, for postings
// built by hand: every token of docs is in exactly one posting, every
// posting with entries has a centroid, and in a typed index every such
// posting has the type registerPosting gives it.
func checkPlacement(t *testing.T, tf *testIndex, docs map[uint64][][]float32) {
	t.Helper()

	seen := make(map[tokenRef]int)
	tf.Index.postings.mu.RLock()
	postings := len(tf.Index.postings.lists)
	tf.Index.postings.mu.RUnlock()
	for id := range uint64(postings) {
		refs := tf.posting(id)
		if len(refs) == 0 {
			continue
		}
		require.True(t, tf.Index.Centroids.Exists(id), "posting %d has entries but no centroid", id)
		require.Equal(t, testPostingType, tf.Index.types.typeOf(id), "posting %d", id)
		for _, ref := range refs {
			seen[ref]++
		}
	}
	for docID := range docs {
		ref := tokenRef{DocID: uint32(docID)}
		require.Equal(t, 1, seen[ref], "token of doc %d is not in exactly one posting", docID)
	}
	require.Len(t, seen, len(docs), "the postings hold tokens of documents never inserted")
}

// oneTokenDocs returns the documents createPostingWithTokens stores.
func oneTokenDocs(vectors [][]float32, startID uint64) map[uint64][][]float32 {
	docs := make(map[uint64][][]float32, len(vectors))
	for i, vec := range vectors {
		docs[startID+uint64(i)] = [][]float32{vec}
	}
	return docs
}

// copyDir copies the files under src to dst as they are on disk now.
func copyDir(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.Create(target)
		if err != nil {
			return err
		}
		defer out.Close()
		_, err = io.Copy(out, in)
		return err
	})
	require.NoError(t, err)
}

// TestCrashAfterFirstPostingKeepsTheIDSequence creates the first posting,
// flushes the centroid HNSW's commit log only, and reopens a copy of the
// directory as a crash would leave it. The id sequence must not hand out the
// id of the centroid that survived, and in a typed index the posting's type
// must have survived with it.
func TestCrashAfterFirstPostingKeepsTheIDSequence(t *testing.T) {
	dir := t.TempDir()
	open := func(dir string) testIndex {
		return newTestIndexAt(t, dir, 10, 8, 8, hfreshEFConstruction,
			withDistanceProvider(distancer.NewDotProductProvider()))
	}

	tf := open(dir)
	v := []float32{1, 0, 0, 0}
	require.NoError(t, tf.Index.initDimensions(v))
	require.NoError(t, tf.store.WriteWALs())

	postingID, err := tf.Index.ensureInitialPosting(v, testPostingType)
	require.NoError(t, err)

	require.NoError(t, tf.Index.Centroids.hnsw.Flush())
	crashed := t.TempDir()
	copyDir(t, dir, crashed)
	tf.close(t)

	reopened := open(crashed)
	require.True(t, reopened.Index.Centroids.Exists(postingID))
	next, err := reopened.Index.IDs.Next()
	require.NoError(t, err)
	require.Greater(t, next, postingID, "the sequence handed out the id of a centroid on disk")
	require.Equal(t, testPostingType, reopened.Index.types.typeOf(postingID))
	reopened.close(t)
}

// TestCrashAfterFlushKeepsEveryToken inserts documents, lets the splits,
// merges and reassigns run, calls the index's Flush and nothing else, and
// reopens a copy of the directory as a crash would leave it. Every token
// of every document must be in exactly one posting.
func TestCrashAfterFlushKeepsEveryToken(t *testing.T) {
	dir := t.TempDir()
	open := func(dir string) testIndex {
		return newTestIndexAt(t, dir, testMaxPostingSize, testKCentroids, testKDocsToScore, hfreshEFConstruction,
			withDistanceProvider(distancer.NewDotProductProvider()))
	}

	tf := open(dir)
	rng := rand.New(rand.NewSource(11))
	docs := make(map[uint64][][]float32, testDocs)
	for i := range uint64(testDocs) {
		docs[i] = randomMultiVector(rng, testTokens, testDim)
		addMultiVectorToIndex(t, &tf, i, docs[i])
	}
	tf.Index.waitForMaintenance(t)
	checkInvariants(t, &tf, docs, nil, false)

	require.NoError(t, tf.Index.Flush())
	crashed := t.TempDir()
	copyDir(t, dir, crashed)
	tf.close(t)

	reopened := open(crashed)
	for id, doc := range docs {
		reopened.mvStore.storeMultiVector(id, doc)
	}
	checkInvariants(t, &reopened, docs, nil, false)
	reopened.close(t)
}

// A New that fails leaves no goroutine behind: restore refuses the posting
// format, after the centroid HNSW and the task queue were started.
func TestFailedNewLeavesNoGoroutine(t *testing.T) {
	dir := t.TempDir()
	tf := newTestIndexAt(t, dir, 8, 8, 8, hfreshEFConstruction,
		withDistanceProvider(distancer.NewDotProductProvider()))
	require.NoError(t, tf.Index.initDimensions([]float32{1, 0, 0, 0}))
	bucket, release, err := tf.Index.types.acquire()
	require.NoError(t, err)
	require.NoError(t, bucket.Delete(postingFormatKey))
	release()
	tf.close(t)

	cfg, uc, store, _, _ := newTestConfigAt(t, dir, 8, 8, 8, hfreshEFConstruction,
		withDistanceProvider(distancer.NewDotProductProvider()))
	ignore := goleak.IgnoreCurrent()
	_, err = New(cfg, uc, store)
	require.ErrorContains(t, err, "another posting format")
	require.NoError(t, store.Shutdown(t.Context()))
	goleak.VerifyNone(t, ignore)
}

// TestRestoreDropsEveryPostingWithoutCentroid reopens a copy of the
// directory, as a crash would leave it, that holds the keys of a posting
// with no live centroid. The restore must mark every such posting dead,
// whatever its id is next to the live centroids', so that the bucket leaves
// its keys out at the next flush or compaction.
func TestRestoreDropsEveryPostingWithoutCentroid(t *testing.T) {
	tests := []struct {
		name string
		// run builds the postings and returns the id of the posting left
		// without a centroid and the id of a live one
		run func(t *testing.T, tf *testIndex) (orphan, live uint64)
	}{
		{
			// a split or a merge deleted the centroid with the highest id
			name: "deleted centroid above every live one",
			run: func(t *testing.T, tf *testIndex) (uint64, uint64) {
				low := createTestVectors(4, 3)
				lowID, lowRefs := createPostingWithTokens(t, tf, low, 100)
				high := createTestVectors(4, 3)
				highID, highRefs := createPostingWithTokens(t, tf, high, 200)
				registerPosting(t, tf, lowID, lowRefs, meanVector(low))
				registerPosting(t, tf, highID, highRefs, meanVector(high))
				require.NoError(t, tf.store.WriteWALs())
				require.NoError(t, tf.Index.Centroids.MarkAsDeleted(highID))
				tf.Index.dropPosting(highID)
				return highID, lowID
			},
		},
		{
			// a deleted centroid with an id below a live one
			name: "deleted centroid below a live one",
			run: func(t *testing.T, tf *testIndex) (uint64, uint64) {
				low := createTestVectors(4, 3)
				lowID, lowRefs := createPostingWithTokens(t, tf, low, 100)
				high := createTestVectors(4, 3)
				highID, highRefs := createPostingWithTokens(t, tf, high, 200)
				registerPosting(t, tf, lowID, lowRefs, meanVector(low))
				registerPosting(t, tf, highID, highRefs, meanVector(high))
				require.NoError(t, tf.store.WriteWALs())
				require.NoError(t, tf.Index.Centroids.MarkAsDeleted(lowID))
				tf.Index.dropPosting(lowID)
				return lowID, highID
			},
		},
		{
			// a split stored a new posting and its writes reached the disk,
			// and the crash came before the new centroid was inserted
			name: "posting stored before its centroid was inserted",
			run: func(t *testing.T, tf *testIndex) (uint64, uint64) {
				vectors := createTestVectors(4, 3)
				liveID, liveRefs := createPostingWithTokens(t, tf, vectors, 100)
				registerPosting(t, tf, liveID, liveRefs, meanVector(vectors))
				orphan := createTestVectors(4, 3)
				orphanID, orphanRefs := createPostingWithTokens(t, tf, orphan, 200)
				require.NoError(t, tf.Index.types.register(orphanID, testPostingType))
				require.NoError(t, tf.Index.putPosting(t.Context(), orphanID, orphanRefs))
				return orphanID, liveID
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			open := func(dir string) testIndex {
				return newTestIndexAt(t, dir, 10, 8, 8, hfreshEFConstruction,
					withDistanceProvider(distancer.NewDotProductProvider()))
			}

			tf := open(dir)
			tf.Index.opts.disableReassign = true
			orphan, live := tt.run(t, &tf)
			liveRefs := tf.posting(live)
			require.NoError(t, tf.Index.writeWALs())
			require.NoError(t, tf.Index.Centroids.hnsw.Flush())
			crashed := t.TempDir()
			copyDir(t, dir, crashed)
			tf.close(t)

			reopened := open(crashed)
			require.False(t, reopened.Index.Centroids.Exists(orphan))
			require.True(t, reopened.Index.PostingStore.isDead(orphan),
				"posting %d has no centroid and is not marked dead", orphan)
			require.False(t, reopened.Index.PostingStore.isDead(live))
			require.ElementsMatch(t, liveRefs, reopened.posting(live))
			reopened.close(t)
		})
	}
}

// TestCrashAfterFlushKeepsTheSpreadSums changes the spread sums of a type,
// calls the index's Flush and nothing else, and reopens a copy of the
// directory as a crash would leave it. The sums must be the ones Flush saw.
func TestCrashAfterFlushKeepsTheSpreadSums(t *testing.T) {
	dir := t.TempDir()
	open := func(dir string) testIndex {
		return newTestIndexAt(t, dir, 10, 8, 8, hfreshEFConstruction,
			withDistanceProvider(distancer.NewDotProductProvider()))
	}

	tf := open(dir)
	vectors := createTestVectors(4, 3)
	postingID, refs := createPostingWithTokens(t, &tf, vectors, 100)
	registerPosting(t, &tf, postingID, refs, meanVector(vectors))

	g := tf.Index.types.group(testPostingType)
	require.NotNil(t, g)
	g.spreadMu.Lock()
	g.sum = []float64{1, 2, 3, 4}
	g.sumSq = 30
	g.seen = 5
	g.dirty = true
	g.spreadMu.Unlock()

	require.NoError(t, tf.Index.Flush())
	crashed := t.TempDir()
	copyDir(t, dir, crashed)
	tf.close(t)

	reopened := open(crashed)
	rg := reopened.Index.types.group(testPostingType)
	require.NotNil(t, rg)
	rg.spreadMu.Lock()
	seen, sumSq, sum := rg.seen, rg.sumSq, append([]float64(nil), rg.sum...)
	rg.spreadMu.Unlock()
	require.Equal(t, uint64(5), seen)
	require.Equal(t, 30.0, sumSq)
	require.Equal(t, []float64{1, 2, 3, 4}, sum)
	reopened.close(t)
}

// TestCrashUnderOrAfterTheThresholdKeepsTheDocuments stages documents, or
// builds the index from them, and copies the directory as a crash would
// leave it, before the shard writes the buckets' logs at the end of the
// batch. Staged: only the staging bucket's log is written, as when a staged
// document fills its buffer. Built: no log is written after the build. Every
// document must be in the reopened index.
func TestCrashUnderOrAfterTheThresholdKeepsTheDocuments(t *testing.T) {
	tests := []struct {
		name string
		// threshold is the staging threshold, docs the documents inserted
		threshold uint64
		docs      int
		built     bool
		// writeStaging writes the staging bucket's log alone
		writeStaging bool
	}{
		{name: "staged, the first document", threshold: 10000, docs: 1, writeStaging: true},
		{name: "built from the staged documents", threshold: 100, docs: 3, built: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			open := func(dir string) testIndex {
				return newTestIndexAt(t, dir, 10, 8, 8, hfreshEFConstruction,
					withDistanceProvider(distancer.NewDotProductProvider()))
			}

			tf := open(dir)
			tf.Index.setCenterTokens(tt.threshold)
			rng := rand.New(rand.NewSource(3))
			docs := make(map[uint64][][]float32, tt.docs)
			for i := range uint64(tt.docs) {
				docs[i] = randomMultiVector(rng, 40, 32)
				types := make([]uint32, len(docs[i]))
				for pos := range types {
					types[pos] = testTokenType(pos)
				}
				tf.mvStore.storeMultiVector(i, docs[i])
				require.NoError(t, tf.Index.AddMultiWithTypes(t.Context(), i, docs[i], types))
			}
			require.Equal(t, tt.built, tf.Index.built.Load())
			if tt.writeStaging {
				bucket, release, err := tf.Index.staging.acquire()
				require.NoError(t, err)
				require.NoError(t, bucket.WriteWAL())
				release()
			}
			require.NoError(t, tf.Index.Centroids.hnsw.Flush())
			crashed := t.TempDir()
			copyDir(t, dir, crashed)
			tf.close(t)

			reopened := open(crashed)
			for id, doc := range docs {
				reopened.mvStore.storeMultiVector(id, doc)
			}
			require.Equal(t, tt.built, reopened.Index.built.Load(), "phase after reopen")
			for id := range docs {
				require.True(t, reopened.Index.ContainsDoc(id), "doc %d", id)
			}
			reopened.close(t)
		})
	}
}
