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
	"math/rand"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hfresh"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	ent "github.com/weaviate/weaviate/entities/vectorindex/tachiomfresh"
)

// The staging phase and the centering (staging.go): under the
// threshold the documents sit in the staging bucket and a search scores
// every one of them exactly; at the threshold the index is built on the
// mean of every staged token, which every document token is then centered
// on before it meets a centroid, never a query token.

// newDotTestIndex is a test index on the dot distance, the one centering
// needs, with nothing inserted yet.
func newDotTestIndex(t *testing.T, maxPostingSize uint32) testIndex {
	t.Helper()
	return newTestIndex(t, maxPostingSize, 8, 8, hfreshEFConstruction,
		withDistanceProvider(distancer.NewDotProductProvider()))
}

// a corpus of docs documents of tokens tokens each, sharing one direction
// (as the tokens of some encoders do), so that the mean is far from zero
func centeredCorpus(docs, tokens, dims int, seed int64) (corpus [][][]float32, random func() []float32) {
	rng := rand.New(rand.NewSource(seed))
	common := make([]float32, dims)
	for i := range common {
		common[i] = float32(rng.NormFloat64())
	}
	random = func() []float32 {
		v := make([]float32, dims)
		for i := range v {
			v[i] = common[i] + 0.3*float32(rng.NormFloat64())
		}
		return v
	}
	corpus = make([][][]float32, docs)
	for d := range corpus {
		corpus[d] = make([][]float32, tokens)
		for i := range corpus[d] {
			corpus[d][i] = random()
		}
	}
	return corpus, random
}

// bruteForce returns the documents of corpus in ascending MaxSim distance
// (the sum over query tokens of the minimum dot distance), deleted ones
// left out.
func bruteForce(corpus [][][]float32, query [][]float32, deleted map[uint64]bool) (ids []uint64, dists []float32) {
	type scored struct {
		id   uint64
		dist float32
	}
	var exact []scored
	for d := range corpus {
		if deleted[uint64(d)] {
			continue
		}
		var sum float32
		for _, qt := range query {
			best := float32(1e9)
			for _, dt := range corpus[d] {
				var dot float32
				for j := range qt {
					dot += qt[j] * dt[j]
				}
				best = min(best, -dot)
			}
			sum += best
		}
		exact = append(exact, scored{uint64(d), sum})
	}
	sort.Slice(exact, func(i, j int) bool { return exact[i].dist < exact[j].dist })
	for _, s := range exact {
		ids = append(ids, s.id)
		dists = append(dists, s.dist)
	}
	return ids, dists
}

// centroidCount is how many live centroids the index holds, once the
// background tasks have run: a split running meanwhile would change it.
func (tf *testIndex) centroidCount(t testing.TB) int {
	t.Helper()
	tf.Index.waitForMaintenance(t)
	tf.Index.postings.mu.RLock()
	n := len(tf.Index.postings.lists)
	tf.Index.postings.mu.RUnlock()
	count := 0
	for id := range uint64(n) {
		if tf.Index.Centroids.Exists(id) {
			count++
		}
	}
	return count
}

// entryCount is how many posting entries the index holds, once the
// background tasks have run.
func (tf *testIndex) entryCount(t testing.TB) int {
	t.Helper()
	tf.Index.waitForMaintenance(t)
	tf.Index.postings.mu.RLock()
	n := len(tf.Index.postings.lists)
	tf.Index.postings.mu.RUnlock()
	count := 0
	for id := range uint64(n) {
		count += len(tf.posting(id))
	}
	return count
}

func (tf *testIndex) stagedCount(t *testing.T) (docs int, tokens int) {
	t.Helper()
	require.NoError(t, tf.Index.staging.iterate(func(docID uint64, vectors [][]float32, types []uint32) error {
		docs++
		tokens += len(vectors)
		return nil
	}))
	return docs, tokens
}

// Under the threshold: no centroid, the documents in the staging bucket,
// every search exact over them, a deleted document gone, a filter applied.
func TestStagingUnderThreshold(t *testing.T) {
	const docs, tokens, dims = 12, 4, 8
	corpus, random := centeredCorpus(docs, tokens, dims, 3)
	tf := newDotTestIndex(t, 8)
	tf.Index.setCenterTokens(1000)
	for d := range corpus {
		tf.mvStore.storeMultiVector(uint64(d), corpus[d])
		require.NoError(t, tf.Index.AddMulti(t.Context(), uint64(d), corpus[d]))
		require.True(t, tf.Index.ContainsDoc(uint64(d)))
	}
	require.False(t, tf.Index.built.Load())
	require.Nil(t, tf.Index.center)
	require.Equal(t, 0, tf.centroidCount(t))
	staged, stagedTokens := tf.stagedCount(t)
	require.Equal(t, docs, staged)
	require.Equal(t, docs*tokens, stagedTokens)

	query := [][]float32{random(), random(), random()}
	ids, dists, err := tf.Index.SearchByMultiVector(t.Context(), query, 5, nil)
	require.NoError(t, err)
	wantIDs, wantDists := bruteForce(corpus, query, nil)
	require.Equal(t, wantIDs[:5], ids)
	require.InDeltaSlice(t, wantDists[:5], dists, 1e-4)

	// a filter: the best allowed documents only
	allow := helpers.NewAllowList(1, 4, 7, 10)
	ids, _, err = tf.Index.SearchByMultiVector(t.Context(), query, 2, allow)
	require.NoError(t, err)
	var wantAllowed []uint64
	for _, id := range wantIDs {
		if allow.Contains(id) {
			wantAllowed = append(wantAllowed, id)
		}
	}
	require.Equal(t, wantAllowed[:2], ids)

	// the top document deleted: it leaves the bucket and the results
	require.NoError(t, tf.Index.Delete(wantIDs[0]))
	require.False(t, tf.Index.ContainsDoc(wantIDs[0]))
	staged, stagedTokens = tf.stagedCount(t)
	require.Equal(t, docs-1, staged)
	require.Equal(t, (docs-1)*tokens, stagedTokens)
	ids, _, err = tf.Index.SearchByMultiVector(t.Context(), query, 5, nil)
	require.NoError(t, err)
	wantIDs, _ = bruteForce(corpus, query, map[uint64]bool{wantIDs[0]: true})
	require.Equal(t, wantIDs[:5], ids)
}

// At the threshold the index is built: the mean of every staged token is
// the centering mean, every staged token is in a posting, the staging
// bucket is empty, the next document takes the normal path, and the search
// is still exact when every centroid is probed.
func TestStagingBuildsAtThreshold(t *testing.T) {
	const docs, tokens, dims = 40, 4, 8
	corpus, random := centeredCorpus(docs, tokens, dims, 5)
	tf := newDotTestIndex(t, 8)
	tf.Index.setCenterTokens(3 * tokens)

	for d := range 2 {
		tf.mvStore.storeMultiVector(uint64(d), corpus[d])
		require.NoError(t, tf.Index.AddMultiWithTypes(t.Context(), uint64(d), corpus[d], make([]uint32, tokens)))
	}
	require.False(t, tf.Index.built.Load())
	require.Equal(t, 0, tf.centroidCount(t))

	// the third document reaches the threshold
	tf.mvStore.storeMultiVector(2, corpus[2])
	require.NoError(t, tf.Index.AddMultiWithTypes(t.Context(), 2, corpus[2], make([]uint32, tokens)))
	require.True(t, tf.Index.built.Load())
	require.Equal(t, uint64(3*tokens), tf.centeredOn.Load())
	mean := make([]float32, dims)
	for d := range 3 {
		for _, vec := range corpus[d] {
			for j, x := range vec {
				mean[j] += x / float32(3*tokens)
			}
		}
	}
	require.InDeltaSlice(t, mean, tf.Index.center, 1e-5)
	staged, _ := tf.stagedCount(t)
	require.Equal(t, 0, staged)
	entries := tf.entryCount(t)
	require.Equal(t, 3*tokens, entries)
	phase, err := tf.Index.types.getPhase()
	require.NoError(t, err)
	require.Equal(t, phaseBuilt, phase)

	// the rest takes the normal path
	for d := 3; d < docs; d++ {
		tf.mvStore.storeMultiVector(uint64(d), corpus[d])
		require.NoError(t, tf.Index.AddMultiWithTypes(t.Context(), uint64(d), corpus[d], make([]uint32, tokens)))
	}
	tf.Index.waitForMaintenance(t)
	entries = tf.entryCount(t)
	require.Equal(t, docs*tokens, entries)
	require.Greater(t, tf.centroidCount(t), 1, "the splits made more than one centroid")
	// every centroid probed, every document kept for stage 3: the exact
	// answer on the original tokens
	tf.Index.setSearchBudgets(uint32(tf.centroidCount(t)), docs)
	for range 10 {
		query := [][]float32{random(), random(), random(), random()}
		ids, dists, err := tf.Index.SearchByMultiVector(t.Context(), query, 10, nil)
		require.NoError(t, err)
		wantIDs, wantDists := bruteForce(corpus, query, nil)
		require.Equal(t, wantIDs[:10], ids)
		require.InDeltaSlice(t, wantDists[:10], dists, 1e-4)
	}
}

// A restart under the threshold finds the staged documents again, a
// deleted one left out, and the build after the restart centers on every
// token staged before and after it; a restart after the build finds the
// mean, the phase and the postings.
func TestStagingSurvivesRestart(t *testing.T) {
	const tokens, dims = 4, 8
	corpus, random := centeredCorpus(6, tokens, dims, 11)
	dir := t.TempDir()
	open := func() testIndex {
		tf := newTestIndexAt(t, dir, 8, 8, 8, hfreshEFConstruction,
			withDistanceProvider(distancer.NewDotProductProvider()))
		tf.Index.setCenterTokens(4 * tokens)
		return tf
	}

	tf := open()
	for d := range 3 {
		tf.mvStore.storeMultiVector(uint64(d), corpus[d])
		require.NoError(t, tf.Index.AddMulti(t.Context(), uint64(d), corpus[d]))
	}
	require.NoError(t, tf.Index.Delete(1))
	require.False(t, tf.Index.built.Load())
	tf.close(t)

	tf = open()
	require.False(t, tf.Index.built.Load())
	require.Nil(t, tf.Index.center)
	require.Equal(t, uint64(2*tokens), tf.Index.stagedTokens)
	require.True(t, tf.Index.ContainsDoc(0))
	require.False(t, tf.Index.ContainsDoc(1))
	require.True(t, tf.Index.ContainsDoc(2))
	query := [][]float32{random(), random()}
	ids, _, err := tf.Index.SearchByMultiVector(t.Context(), query, 2, nil)
	require.NoError(t, err)
	wantIDs, _ := bruteForce(corpus[:3], query, map[uint64]bool{1: true})
	require.Equal(t, wantIDs, ids)

	// two more documents reach the threshold: built on docs 0, 2, 3, 4
	for d := 3; d < 5; d++ {
		tf.mvStore.storeMultiVector(uint64(d), corpus[d])
		require.NoError(t, tf.Index.AddMulti(t.Context(), uint64(d), corpus[d]))
	}
	require.True(t, tf.Index.built.Load())
	mean := make([]float32, dims)
	for _, d := range []int{0, 2, 3, 4} {
		for _, vec := range corpus[d] {
			for j, x := range vec {
				mean[j] += x / float32(4*tokens)
			}
		}
	}
	require.InDeltaSlice(t, mean, tf.Index.center, 1e-5)
	tf.Index.waitForMaintenance(t)
	entries := tf.entryCount(t)
	require.Equal(t, 4*tokens, entries)
	tf.close(t)

	tf = open()
	require.True(t, tf.Index.built.Load())
	require.InDeltaSlice(t, mean, tf.Index.center, 1e-5)
	require.Equal(t, uint64(0), tf.Index.stagedTokens)
	entries = tf.entryCount(t)
	require.Equal(t, 4*tokens, entries)
	tf.mvStore.storeMultiVector(5, corpus[5])
	require.NoError(t, tf.Index.AddMulti(t.Context(), 5, corpus[5]))
	entries = tf.entryCount(t)
	require.Equal(t, 5*tokens, entries)
	tf.close(t)
}

// stoppingContext reports cancellation from its n-th call of Err on. A build
// checks the context once per document, so a build stops after n-1 documents.
type stoppingContext struct {
	context.Context
	n atomic.Int64
}

func (c *stoppingContext) Err() error {
	if c.n.Add(-1) <= 0 {
		return context.Canceled
	}
	return nil
}

// interruptedBuild stages the corpus up to the threshold and stops the build
// after three documents. The postings then hold those documents, and the
// phase is still staging.
func interruptedBuild(t *testing.T, tf *testIndex, corpus [][][]float32) {
	t.Helper()
	tf.Index.setCenterTokens(uint64(len(corpus) * len(corpus[0])))
	for d := range corpus {
		tf.mvStore.storeMultiVector(uint64(d), corpus[d])
		if d < len(corpus)-1 {
			require.NoError(t, tf.addMulti(t.Context(), uint64(d), corpus[d]))
			continue
		}
		stop := &stoppingContext{Context: t.Context()}
		stop.n.Store(4)
		require.ErrorIs(t, tf.addMulti(stop, uint64(d), corpus[d]), context.Canceled)
	}
	require.False(t, tf.Index.built.Load())
	require.Positive(t, tf.centroidCount(t))
	phase, err := tf.Index.types.getPhase()
	require.NoError(t, err)
	require.Equal(t, phaseStaging, phase)
}

// checkBuiltFromCorpus checks an index built from the whole corpus: every
// token in one posting, nothing staged, and the mean of the whole corpus.
func checkBuiltFromCorpus(t *testing.T, tf *testIndex, corpus [][][]float32) {
	t.Helper()
	tf.Index.waitForMaintenance(t)
	require.True(t, tf.Index.built.Load())
	docs := make(map[uint64][][]float32, len(corpus))
	for d := range corpus {
		docs[uint64(d)] = corpus[d]
	}
	checkInvariants(t, tf, docs, nil, false)
	require.Equal(t, len(corpus)*len(corpus[0]), tf.entryCount(t))
	stagedDocs, _ := tf.stagedCount(t)
	require.Zero(t, stagedDocs)
	require.Equal(t, meanOf(corpus), tf.Index.center)
}

// checkBuiltFromCorpusAfterThreshold is checkBuiltFromCorpus for an index
// whose last documents came after the build, so the mean is not the whole
// corpus's.
func checkBuiltFromCorpusAfterThreshold(t *testing.T, tf *testIndex, corpus [][][]float32) {
	t.Helper()
	tf.Index.waitForMaintenance(t)
	require.True(t, tf.Index.built.Load())
	docs := make(map[uint64][][]float32, len(corpus))
	for d := range corpus {
		docs[uint64(d)] = corpus[d]
	}
	checkInvariants(t, tf, docs, nil, false)
	require.Equal(t, len(corpus)*len(corpus[0]), tf.entryCount(t))
}

func meanOf(corpus [][][]float32) []float32 {
	sum := make([]float64, len(corpus[0][0]))
	n := 0
	for _, doc := range corpus {
		for _, token := range doc {
			for j, x := range token {
				sum[j] += float64(x)
			}
			n++
		}
	}
	mean := make([]float32, len(sum))
	for j := range mean {
		mean[j] = float32(sum[j] / float64(n))
	}
	return mean
}

// A build that stops part way is retried by the next insert of the crossing
// document, as the shard's queue retries it. The retry starts from nothing,
// so no document is placed twice.
func TestInterruptedBuildIsRetried(t *testing.T) {
	const tokens = 4
	corpus, _ := centeredCorpus(12, tokens, 8, 13)
	tf := newDotTestIndex(t, 8)
	interruptedBuild(t, &tf, corpus)

	last := uint64(len(corpus) - 1)
	require.NoError(t, tf.addMulti(t.Context(), last, corpus[last]))
	checkBuiltFromCorpus(t, &tf, corpus)
}

// A crash during a build leaves the staging phase and centroids. The index
// opens, answers from the staging bucket, and builds again at the next insert
// past the threshold.
func TestInterruptedBuildSurvivesRestart(t *testing.T) {
	const tokens = 4
	dir := t.TempDir()
	corpus, _ := centeredCorpus(12, tokens, 8, 13)
	open := func(dir string) testIndex {
		return newTestIndexAt(t, dir, 8, 8, 8, hfreshEFConstruction,
			withDistanceProvider(distancer.NewDotProductProvider()))
	}
	tf := open(dir)
	interruptedBuild(t, &tf, corpus)
	require.NoError(t, tf.store.WriteWALs())
	require.NoError(t, tf.Index.Centroids.hnsw.Flush())
	crashed := t.TempDir()
	copyDir(t, dir, crashed)
	tf.close(t)

	reopened := open(crashed)
	for d := range corpus {
		reopened.mvStore.storeMultiVector(uint64(d), corpus[d])
	}
	require.False(t, reopened.Index.built.Load())
	require.False(t, reopened.Index.hasCentroids())
	require.Nil(t, reopened.Index.center)

	query := corpus[3]
	wantIDs, wantDists := bruteForce(corpus, query, nil)
	ids, dists, err := reopened.Index.SearchByMultiVector(t.Context(), query, len(corpus), nil)
	require.NoError(t, err)
	require.Equal(t, wantIDs, ids)
	require.InDeltaSlice(t, wantDists, dists, 1e-4)

	reopened.Index.centerTokens = uint64(len(corpus) * tokens)
	last := uint64(len(corpus) - 1)
	require.NoError(t, reopened.addMulti(t.Context(), last, corpus[last]))
	checkBuiltFromCorpus(t, &reopened, corpus)
	reopened.close(t)

	// what is on disk holds no entry of the dropped attempt
	again := open(crashed)
	for d := range corpus {
		again.mvStore.storeMultiVector(uint64(d), corpus[d])
	}
	checkBuiltFromCorpus(t, &again, corpus)
	again.close(t)
}

// A shutdown closes the task queues, so a build that needs a split fails. The
// reopened index builds again and holds no token twice.
func TestBuildFailingOnClosedQueuesIsRebuiltAfterRestart(t *testing.T) {
	const tokens = 4
	dir := t.TempDir()
	corpus, _ := centeredCorpus(12, tokens, 8, 13)
	open := func(dir string) testIndex {
		return newTestIndexAt(t, dir, 4, 8, 8, hfreshEFConstruction,
			withDistanceProvider(distancer.NewDotProductProvider()))
	}
	tf := open(dir)
	tf.Index.setCenterTokens(uint64(len(corpus) * tokens))
	for d := range corpus[:len(corpus)-1] {
		tf.mvStore.storeMultiVector(uint64(d), corpus[d])
		require.NoError(t, tf.addMulti(t.Context(), uint64(d), corpus[d]))
	}
	require.NoError(t, tf.store.WriteWALs())
	require.NoError(t, tf.Index.taskQueue.Close(t.Context()))
	last := uint64(len(corpus) - 1)
	tf.mvStore.storeMultiVector(last, corpus[last])
	err := tf.addMulti(t.Context(), last, corpus[last])
	require.ErrorContains(t, err, "failed to index the staged documents")
	require.NoError(t, tf.store.WriteWALs())
	require.NoError(t, tf.Index.Centroids.hnsw.Flush())
	crashed := t.TempDir()
	copyDir(t, dir, crashed)

	reopened := open(crashed)
	for d := range corpus {
		reopened.mvStore.storeMultiVector(uint64(d), corpus[d])
	}
	require.False(t, reopened.Index.built.Load())
	require.False(t, reopened.Index.hasCentroids())
	reopened.Index.centerTokens = uint64(len(corpus) * tokens)
	require.NoError(t, reopened.addMulti(t.Context(), last, corpus[last]))
	checkBuiltFromCorpus(t, &reopened, corpus)
	reopened.close(t)

	// what is on disk holds no entry of the dropped attempt
	again := open(crashed)
	for d := range corpus {
		again.mvStore.storeMultiVector(uint64(d), corpus[d])
	}
	checkBuiltFromCorpus(t, &again, corpus)
	again.close(t)
}

// What restore does with the phase on disk.
func TestRestoreByPhase(t *testing.T) {
	v := []float32{1, 0, 0, 0}
	deletePhase := func(t *testing.T, tf *testIndex) {
		bucket, release, err := tf.Index.types.acquire()
		require.NoError(t, err)
		defer release()
		require.NoError(t, bucket.Delete(phaseKey))
	}
	open := func(t *testing.T, dir string) testIndex {
		return newTestIndexAt(t, dir, 8, 8, 8, hfreshEFConstruction,
			withDistanceProvider(distancer.NewDotProductProvider()))
	}

	t.Run("dimensions without a phase key: the first insert is staged", func(t *testing.T) {
		dir := t.TempDir()
		tf := open(t, dir)
		tf.Index.setCenterTokens(1000)
		require.NoError(t, tf.Index.initDimensions(v))
		deletePhase(t, &tf)
		tf.close(t)

		reopened := open(t, dir)
		require.False(t, reopened.Index.built.Load())
		reopened.Index.centerTokens = 1000
		doc := [][]float32{v}
		reopened.mvStore.storeMultiVector(1, doc)
		require.NoError(t, reopened.addMulti(t.Context(), 1, doc))
		docs, tokens := reopened.stagedCount(t)
		require.Equal(t, 1, docs)
		require.Equal(t, 1, tokens)
		require.Zero(t, reopened.centroidCount(t))
		reopened.close(t)
	})

	t.Run("centroids without a phase key: the index is built", func(t *testing.T) {
		dir := t.TempDir()
		tf := open(t, dir)
		doc := [][]float32{v}
		tf.mvStore.storeMultiVector(1, doc)
		require.NoError(t, tf.addMulti(t.Context(), 1, doc))
		deletePhase(t, &tf)
		tf.close(t)

		reopened := open(t, dir)
		reopened.mvStore.storeMultiVector(1, doc)
		require.True(t, reopened.Index.built.Load())
		require.True(t, reopened.Index.ContainsDoc(1))
		reopened.close(t)
	})

	t.Run("the first insert finds the phase persisted with the dimensions", func(t *testing.T) {
		for _, tt := range []struct {
			threshold uint64
			want      byte
		}{{threshold: 1000, want: phaseStaging}, {threshold: 0, want: phaseBuilt}} {
			tf := open(t, t.TempDir())
			tf.Index.setCenterTokens(tt.threshold)
			require.NoError(t, tf.Index.initDimensions(v))
			got, err := tf.Index.types.getPhase()
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
			tf.close(t)
		}
	})

	t.Run("staged keys left by a crash after the phase are deleted", func(t *testing.T) {
		dir := t.TempDir()
		tf := open(t, dir)
		doc := [][]float32{v}
		tf.mvStore.storeMultiVector(1, doc)
		require.NoError(t, tf.addMulti(t.Context(), 1, doc))
		require.NoError(t, tf.Index.staging.put(1, doc, []uint32{testTokenType(0)}))
		tf.close(t)

		reopened := open(t, dir)
		reopened.mvStore.storeMultiVector(1, doc)
		require.True(t, reopened.Index.built.Load())
		docs, _ := reopened.stagedCount(t)
		require.Zero(t, docs)
		reopened.close(t)
	})
}

// A staged search that finds the index built once its cursor is open does
// not trust the cursor, whose staged documents may be gone: it reports itself
// stale, and SearchByMultiVector runs the search of the built index.
func TestStagedSearchNoticesTheBuild(t *testing.T) {
	const tokens = 4
	corpus, _ := centeredCorpus(4, tokens, 8, 13)
	tf := newDotTestIndex(t, 8)
	tf.Index.setCenterTokens(4 * tokens)
	for d := range corpus {
		tf.mvStore.storeMultiVector(uint64(d), corpus[d])
		require.NoError(t, tf.Index.AddMulti(t.Context(), uint64(d), corpus[d]))
	}
	require.True(t, tf.Index.built.Load())

	ids, _, stale, err := tf.Index.stagedSearch(t.Context(), corpus[0], 2, nil)
	require.NoError(t, err)
	require.True(t, stale)
	require.Empty(t, ids)

	// the staging bucket is empty: the results come from the built index
	ids, _, err = tf.Index.SearchByMultiVector(t.Context(), corpus[0], 2, nil)
	require.NoError(t, err)
	require.Len(t, ids, 2)
}

// With the threshold at 0 (the package tests' setting) nothing is staged
// and nothing centered: the first insert builds.
func TestZeroThresholdNeverStages(t *testing.T) {
	tf := newDotTestIndex(t, 8)
	doc := [][]float32{{1, 0, 0, 0}, {0, 1, 0, 0}}
	tf.mvStore.storeMultiVector(1, doc)
	require.NoError(t, tf.Index.AddMulti(t.Context(), 1, doc))
	require.True(t, tf.Index.built.Load())
	require.Nil(t, tf.Index.center)
	require.Equal(t, 1, tf.centroidCount(t))
	staged, _ := tf.stagedCount(t)
	require.Equal(t, 0, staged)
}

// Only the dot distance is accepted.
func TestOnlyDotIsAccepted(t *testing.T) {
	for _, tt := range []struct {
		name     string
		provider distancer.Provider
	}{
		{"cosine", distancer.NewCosineDistanceProvider()},
		{"l2-squared", distancer.NewL2SquaredProvider()},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := hfresh.DefaultConfig()
			cfg.DistanceProvider = tt.provider
			_, err := New(cfg, ent.NewDefaultUserConfig(), testinghelpers.NewDummyStore(t))
			require.ErrorContains(t, err, "supports the dot distance only")
		})
	}
}

// An index with the default user configuration, whose distance is dot,
// crosses the threshold and keeps accepting inserts.
func TestDefaultConfigCrossesTheThreshold(t *testing.T) {
	uc := ent.NewDefaultUserConfig()
	require.Equal(t, "dot", uc.Distance)

	const tokens = 4
	corpus, _ := centeredCorpus(8, tokens, 8, 17)
	tf := newTestIndex(t, uc.MaxPostingSize, int(uc.KCentroids), int(uc.KDocsToScore), uc.EFConstruction)
	tf.Index.setCenterTokens(6 * tokens)
	for d := range corpus {
		tf.mvStore.storeMultiVector(uint64(d), corpus[d])
		require.NoError(t, tf.addMulti(t.Context(), uint64(d), corpus[d]))
	}
	checkBuiltFromCorpusAfterThreshold(t, &tf, corpus)
}

// A token is put into the posting of the centroid nearest to its centered
// form: with the mean u = e1 (the staged document's tokens 2 e1 and 0) and
// two centroids A = e1/2 and B = e2/2 of type 1 in the centered space, the
// raw token (1, 0.45) is nearer to A (both by L2 and by dot), its centered
// form (0, 0.45) nearer to B.
func TestCenteredPlacementUsesTheCenteredToken(t *testing.T) {
	tf := newDotTestIndex(t, 64)
	tf.Index.setCenterTokens(2)
	staged := [][]float32{{2, 0, 0, 0}, {0, 0, 0, 0}}
	tf.mvStore.storeMultiVector(500, staged)
	require.NoError(t, tf.Index.AddMultiWithTypes(t.Context(), 500, staged, []uint32{2, 2}))
	require.True(t, tf.Index.built.Load())
	require.Equal(t, []float32{1, 0, 0, 0}, tf.Index.center)
	aID, _ := typedPosting(t, &tf, 1, []float32{0.5, 0, 0, 0}, 6, 100)
	bID, _ := typedPosting(t, &tf, 1, []float32{0, 0.5, 0, 0}, 6, 200)

	token := []float32{1, 0.45, 0, 0}
	tf.mvStore.storeMultiVector(1000, [][]float32{token})
	require.NoError(t, tf.Index.AddMultiWithTypes(t.Context(), 1000, [][]float32{token}, []uint32{1}))
	ref := tokenRef{DocID: 1000}
	require.Contains(t, tf.posting(bID), ref)
	require.NotContains(t, tf.posting(aID), ref)
}

// A centered index returns what brute force on the original tokens returns:
// every centroid probed and every reached document scored, the top 10 and
// their MaxSim distances are the exact ones. Half the corpus is staged and
// builds the index, the other half takes the normal path.
func TestCenteredIndexMatchesBruteForce(t *testing.T) {
	const docs, tokens, dims, queries = 60, 6, 16, 20
	corpus, random := centeredCorpus(docs, tokens, dims, 7)

	tf := newDotTestIndex(t, 8)
	tf.Index.setCenterTokens(docs * tokens / 2)
	for d := range corpus {
		tf.mvStore.storeMultiVector(uint64(d), corpus[d])
		require.NoError(t, tf.Index.AddMultiWithTypes(t.Context(), uint64(d), corpus[d], make([]uint32, tokens)))
	}
	require.True(t, tf.Index.built.Load())
	require.Equal(t, uint64(docs*tokens/2), tf.centeredOn.Load())
	tf.Index.waitForMaintenance(t)
	require.Greater(t, tf.centroidCount(t), 1, "the splits made more than one centroid")
	// every centroid probed, every document kept for stage 3
	tf.Index.setSearchBudgets(uint32(tf.centroidCount(t)), docs)

	for q := range queries {
		query := make([][]float32, 4)
		for i := range query {
			query[i] = random()
		}
		ids, dists, err := tf.Index.SearchByMultiVector(t.Context(), query, 10, nil)
		require.NoError(t, err)
		wantIDs, wantDists := bruteForce(corpus, query, nil)
		require.Equal(t, wantIDs[:10], ids, "query %d", q)
		require.InDeltaSlice(t, wantDists[:10], dists, 1e-4, "query %d", q)
	}
}

// TestBuildFailingAfterTheBuiltPhaseStaysStaging: a build that wrote the
// built phase and fails before that write reaches its log has not ended.
// The phase must be staging again, or a later write of the type bucket's log
// and a restart would drop the staged documents as those of a built index.
func TestBuildFailingAfterTheBuiltPhaseStaysStaging(t *testing.T) {
	tf := newTestIndex(t, 10, 8, 8, hfreshEFConstruction)
	defer tf.close(t)
	tf.Index.setCenterTokens(5)
	ctx := t.Context()
	injected := errors.New("injected failure")
	tf.Index.opts.beforeBuiltWAL = func() error { return injected }

	require.NoError(t, tf.Index.AddMultiWithTypes(ctx, 1, [][]float32{{1, 0, 0, 0}, {0, 1, 0, 0}}, []uint32{7, 7}))
	require.NoError(t, tf.Index.AddMultiWithTypes(ctx, 2, [][]float32{{0, 0, 1, 0}, {0, 0, 0, 1}}, []uint32{7, 7}))
	err := tf.Index.AddMultiWithTypes(ctx, 3, [][]float32{{1, 1, 0, 0}, {0, 1, 1, 0}}, []uint32{7, 7})
	require.ErrorIs(t, err, injected)
	require.False(t, tf.Index.built.Load())

	phase, err := tf.Index.types.getPhase()
	require.NoError(t, err)
	require.Equal(t, phaseStaging, phase)

	// the next insert builds again
	tf.Index.opts.beforeBuiltWAL = nil
	require.NoError(t, tf.Index.AddMultiWithTypes(ctx, 4, [][]float32{{1, 0, 1, 0}, {0, 1, 0, 1}}, []uint32{7, 7}))
	require.True(t, tf.Index.built.Load())
	for id := range uint64(4) {
		require.True(t, tf.Index.ContainsDoc(id+1), "doc %d", id+1)
	}
}
