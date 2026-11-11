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
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// A document inserted again under its id: after a delete, after a failed
// insert, racing a delete, and a restart that finds a document in part or a
// token twice.

func reinsertIndexAt(t *testing.T, dir string) testIndex {
	return newTestIndexAt(t, dir, testMaxPostingSize, testKCentroids, testKDocsToScore, hfreshEFConstruction,
		withDistanceProvider(distancer.NewDotProductProvider()))
}

func reinsertCorpus(n int) map[uint64][][]float32 {
	rng := rand.New(rand.NewSource(21))
	docs := make(map[uint64][][]float32, n)
	for i := range uint64(n) {
		docs[i] = randomMultiVector(rng, testTokens, testDim)
	}
	return docs
}

// insertAll stores and inserts docs, in id order.
func insertAll(t *testing.T, tf *testIndex, docs map[uint64][][]float32) {
	t.Helper()
	for id := range uint64(len(docs)) {
		addMultiVectorToIndex(t, tf, id, docs[id])
	}
}

// staleEntries counts the entries a re-insert left behind.
func staleEntries(t *testing.T, tf *testIndex) int {
	t.Helper()
	n := 0
	tf.Index.postings.mu.RLock()
	postings := len(tf.Index.postings.lists)
	tf.Index.postings.mu.RUnlock()
	for id := range uint64(postings) {
		for _, ref := range tf.posting(id) {
			live, err := tf.Index.live(t.Context(), ref)
			require.NoError(t, err)
			if !live {
				n++
			}
		}
	}
	return n
}

func TestReinsertSameID(t *testing.T) {
	for _, staged := range []bool{false, true} {
		name := "built index"
		if staged {
			name = "staged, then built"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			tf := reinsertIndexAt(t, dir)
			docs := reinsertCorpus(30)
			if staged {
				tf.Index.setCenterTokens(uint64(25 * testTokens))
			}
			// the second insert of doc 3 comes while the index still stages,
			// or once it is built
			const again = 3
			for id := range uint64(len(docs)) {
				addMultiVectorToIndex(t, &tf, id, docs[id])
				if id == 9 {
					addMultiVectorToIndex(t, &tf, again, docs[again])
				}
			}
			if !staged {
				addMultiVectorToIndex(t, &tf, again, docs[again])
			}
			tf.Index.waitForMaintenance(t)

			require.True(t, tf.Index.ContainsDoc(again))
			checkInvariants(t, &tf, docs, nil, false)
			version, err := tf.Index.VersionMap.Get(t.Context(), again)
			require.NoError(t, err)
			if staged {
				// staged when it came again: no entry to replace
				require.Equal(t, uint8(1), version.Version())
			} else {
				// built when doc 3 came the second and the third time
				require.Equal(t, uint8(3), version.Version())
				// splits, merges and cleanups may have dropped some already
				require.LessOrEqual(t, staleEntries(t, &tf), 2*testTokens)
			}
			tf.close(t)

			reopened := reinsertIndexAt(t, dir)
			for id, doc := range docs {
				reopened.mvStore.storeMultiVector(id, doc)
			}
			require.True(t, reopened.Index.ContainsDoc(again))
			checkInvariants(t, &reopened, docs, nil, false)
			reopened.close(t)
		})
	}
}

func TestDeleteThenReinsert(t *testing.T) {
	tf := reinsertIndexAt(t, t.TempDir())
	docs := reinsertCorpus(30)
	insertAll(t, &tf, docs)
	const id = 4
	require.NoError(t, tf.Index.Delete(id))
	require.False(t, tf.Index.ContainsDoc(id))

	addMultiVectorToIndex(t, &tf, id, docs[id])
	tf.Index.waitForMaintenance(t)
	require.True(t, tf.Index.ContainsDoc(id))
	version, err := tf.Index.VersionMap.Get(t.Context(), id)
	require.NoError(t, err)
	require.False(t, version.Deleted())
	checkInvariants(t, &tf, docs, nil, false)
	ids, _, err := tf.Index.SearchByMultiVector(t.Context(), docs[id], 1, nil)
	require.NoError(t, err)
	require.Equal(t, []uint64{id}, ids)
}

// appendsFailAfter makes every append after the first n fail.
func appendsFailAfter(n int64) func(string) error {
	var calls atomic.Int64
	return func(op string) error {
		if op == "append" && calls.Add(1) > n {
			return errors.New("injected append failure")
		}
		return nil
	}
}

// An insert that fails after some tokens leaves a document that is not
// contained, in memory and after a restart; the retry, as the shard's
// catch-up sends it, makes it whole, and the tokens of the failed insert are
// stale.
func TestInsertFailingPartWay(t *testing.T) {
	for _, restart := range []bool{false, true} {
		name := "retried"
		if restart {
			name = "restarted, then retried"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			tf := reinsertIndexAt(t, dir)
			docs := reinsertCorpus(30)
			insertAll(t, &tf, docs)
			tf.Index.waitForMaintenance(t)

			const id = 30
			docs[id] = reinsertCorpus(31)[30]
			tf.mvStore.storeMultiVector(id, docs[id])
			tf.Index.PostingStore.fault = appendsFailAfter(3)
			require.ErrorContains(t, tf.addMulti(t.Context(), id, docs[id]), "injected append failure")
			tf.Index.PostingStore.fault = nil
			require.False(t, tf.Index.ContainsDoc(id))

			if restart {
				tf.close(t)
				tf = reinsertIndexAt(t, dir)
				for id, doc := range docs {
					tf.mvStore.storeMultiVector(id, doc)
				}
				require.False(t, tf.Index.ContainsDoc(id), "a partial document is not contained after a restart")
			}

			require.NoError(t, tf.addMulti(t.Context(), id, docs[id]))
			tf.Index.waitForMaintenance(t)
			require.True(t, tf.Index.ContainsDoc(id))
			checkInvariants(t, &tf, docs, nil, false)
			// what the failed insert placed is stale, unless a split, merge
			// or cleanup dropped it already
			require.LessOrEqual(t, staleEntries(t, &tf), 3)
		})
	}
}

// A delete that lands while the insert is placing its tokens wins: the
// document is not contained once the insert finishes.
func TestInsertRacingDelete(t *testing.T) {
	tf := reinsertIndexAt(t, t.TempDir())
	docs := reinsertCorpus(30)
	insertAll(t, &tf, docs)
	tf.Index.waitForMaintenance(t)

	const id = 30
	doc := reinsertCorpus(31)[30]
	tf.mvStore.storeMultiVector(id, doc)
	placed, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	tf.Index.PostingStore.fault = func(op string) error {
		if op == "append" && calls.Add(1) == 2 {
			close(placed)
			<-release
		}
		return nil
	}
	var wg sync.WaitGroup
	wg.Add(1)
	var insertErr error
	enterrors.GoWrapper(func() {
		defer wg.Done()
		insertErr = tf.addMulti(t.Context(), id, doc)
	}, tf.Index.logger)
	<-placed
	require.NoError(t, tf.Index.Delete(id))
	close(release)
	wg.Wait()
	require.NoError(t, insertErr)
	require.False(t, tf.Index.ContainsDoc(id))
}

// A synchronous split that fails does not fail the insert: its entry is in.
func TestFailingImmediateSplit(t *testing.T) {
	tf := newTestIndex(t, 4, 8, 8, hfreshEFConstruction, withDistanceProvider(distancer.NewDotProductProvider()))
	tf.Index.opts.disableReassign = true
	vectors := make([][]float32, 20)
	for i := range vectors {
		vectors[i] = []float32{1, float32(i) / 100, 0, 0}
	}
	postingID, refs := createPostingWithTokens(t, &tf, vectors, 100)
	registerPosting(t, &tf, postingID, refs, vectors[0])
	// the posting's documents can no longer be read, so a split fails
	for i := range vectors {
		tf.mvStore.deleteMultiVector(100 + uint64(i))
	}

	doc := [][]float32{{1, 0.5, 0, 0}}
	tf.mvStore.storeMultiVector(500, doc)
	require.NoError(t, tf.Index.AddMulti(t.Context(), 500, doc))
	require.True(t, tf.Index.ContainsDoc(500))
	require.Contains(t, tf.posting(postingID), tokenRef{DocID: 500})
	require.True(t, tf.Index.taskQueue.splitList.Contains(postingID), "the split is left to the queue")
	logged := false
	for _, entry := range tf.Logs.AllEntries() {
		logged = logged || strings.HasPrefix(entry.Message, "immediate split failed, enqueued instead")
	}
	require.True(t, logged)
}

// A token found in two postings at restore keeps the copy whose centroid is
// nearer to it, and its document is contained. The documents are in the
// object store when the index reopens, as in a shard, so that restore reads
// the token. Either posting may have the lower id: the copy found first is
// what restore keeps only when it cannot read the token.
func TestRestoreKeepsTheNearerCopy(t *testing.T) {
	for _, nearFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("near posting created first %v", nearFirst), func(t *testing.T) {
			dir := t.TempDir()
			tf := reinsertIndexAt(t, dir)
			tf.Index.opts.disableReassign = true
			near, far := []float32{1, 0, 0, 0}, []float32{0, 1, 0, 0}
			var nearID, farID uint64
			var nearRefs, farRefs []tokenRef
			if nearFirst {
				nearID, nearRefs = createPostingWithTokens(t, &tf, [][]float32{near}, 10)
				farID, farRefs = createPostingWithTokens(t, &tf, [][]float32{far}, 20)
			} else {
				farID, farRefs = createPostingWithTokens(t, &tf, [][]float32{far}, 20)
				nearID, nearRefs = createPostingWithTokens(t, &tf, [][]float32{near}, 10)
			}
			registerPosting(t, &tf, nearID, nearRefs, near)
			registerPosting(t, &tf, farID, farRefs, far)

			doc := [][]float32{{0.95, 0.1, 0, 0}}
			tf.mvStore.storeMultiVector(500, doc)
			require.NoError(t, tf.Index.AddMulti(t.Context(), 500, doc))
			ref := tokenRef{DocID: 500}
			require.Contains(t, tf.posting(nearID), ref)
			// what a crash between the two writes of a move leaves
			require.NoError(t, tf.Index.PostingStore.Append(farID, ref))
			tf.close(t)

			cfg, uc, store, mvStore, hook := newTestConfigAt(t, dir, testMaxPostingSize, testKCentroids, testKDocsToScore, hfreshEFConstruction,
				withDistanceProvider(distancer.NewDotProductProvider()))
			mvStore.storeMultiVector(500, doc)
			mvStore.storeMultiVector(10, [][]float32{near})
			mvStore.storeMultiVector(20, [][]float32{far})
			index, err := New(cfg, uc, store)
			require.NoError(t, err)
			reopened := testIndex{Index: index, Logs: hook, mvStore: mvStore, store: store}
			t.Cleanup(func() {
				_ = index.Shutdown(context.Background())
				_ = store.Shutdown(context.Background())
			})

			for _, entry := range hook.AllEntries() {
				require.NotContains(t, entry.Message, "not readable", "restore could not read the token")
			}
			require.Contains(t, reopened.posting(nearID), ref)
			require.NotContains(t, reopened.posting(farID), ref)
			stored, err := reopened.Index.PostingStore.Get(farID)
			require.NoError(t, err)
			require.NotContains(t, stored, ref, "the dropped copy is gone on disk too")
			require.True(t, reopened.Index.ContainsDoc(500))
		})
	}
}
