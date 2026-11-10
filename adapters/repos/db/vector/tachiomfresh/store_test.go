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
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hfresh"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func TestPostingStore(t *testing.T) {
	a, b, c := tokenRef{DocID: 1, Pos: 0}, tokenRef{DocID: 1, Pos: 7}, tokenRef{DocID: 70000, Pos: 65535}

	tests := []struct {
		name string
		do   func(t *testing.T, s *postingStore, id uint64)
		want []tokenRef
	}{
		{
			name: "a posting never written is empty",
			do:   func(t *testing.T, s *postingStore, id uint64) {},
		},
		{
			name: "append",
			do: func(t *testing.T, s *postingStore, id uint64) {
				require.NoError(t, s.Append(id, a))
				require.NoError(t, s.Append(id, c))
			},
			want: []tokenRef{a, c},
		},
		{
			name: "remove",
			do: func(t *testing.T, s *postingStore, id uint64) {
				require.NoError(t, s.Put(id, nil, []tokenRef{a, b, c}))
				require.NoError(t, s.Remove(id, b))
			},
			want: []tokenRef{a, c},
		},
		{
			name: "a removed entry can come back",
			do: func(t *testing.T, s *postingStore, id uint64) {
				require.NoError(t, s.Append(id, a))
				require.NoError(t, s.Remove(id, a))
				require.NoError(t, s.Append(id, a))
			},
			want: []tokenRef{a},
		},
		{
			name: "put writes the difference",
			do: func(t *testing.T, s *postingStore, id uint64) {
				require.NoError(t, s.Put(id, nil, []tokenRef{a, b}))
				require.NoError(t, s.Put(id, []tokenRef{a, b}, []tokenRef{b, c}))
			},
			want: []tokenRef{b, c},
		},
		{
			name: "put of nothing empties the posting",
			do: func(t *testing.T, s *postingStore, id uint64) {
				require.NoError(t, s.Put(id, nil, []tokenRef{a, b}))
				require.NoError(t, s.Put(id, []tokenRef{a, b}, nil))
			},
		},
		{
			name: "a dropped posting leaves the disk at the next flush",
			do: func(t *testing.T, s *postingStore, id uint64) {
				require.NoError(t, s.Put(id, nil, []tokenRef{a, b}))
				s.Drop(id)
				require.True(t, s.isDead(id))
				require.False(t, s.isDead(id+1))
				require.NoError(t, s.store.FlushMemtables(t.Context()))
			},
		},
		{
			// a store that keys a posting by a one-byte version, as
			// hfresh.PostingStore does, wraps at the 256th put and then
			// reads back what was put 256 puts earlier
			name: "300 puts leave the last one only",
			do: func(t *testing.T, s *postingStore, id uint64) {
				var prev []tokenRef
				for i := range 300 {
					next := []tokenRef{{DocID: uint32(i), Pos: 1}, c}
					require.NoError(t, s.Put(id, prev, next))
					prev = next
				}
			},
			want: []tokenRef{{DocID: 299, Pos: 1}, c},
		},
	}

	tf := newTestIndex(t, testMaxPostingSize, testKCentroids, testKDocsToScore, hfreshEFConstruction)
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := uint64(100 + i)
			tt.do(t, tf.Index.PostingStore, id)
			got, err := tf.Index.PostingStore.Get(id)
			require.NoError(t, err)
			require.ElementsMatch(t, tt.want, got)
			// a neighbouring posting is untouched
			other, err := tf.Index.PostingStore.Get(id + 1000)
			require.NoError(t, err)
			require.Empty(t, other)
		})
	}
}

// A token that leaves a posting is gone from it after a restart, however
// often the posting changed. A store that rewrites the whole posting on
// every removal under a key with a one-byte version, as
// hfresh.PostingStore.Put does, reads a posting rewritten more than 255
// times back with tokens that had moved away.
func TestRestartAfterManyRemovals(t *testing.T) {
	dir := t.TempDir()
	open := func() testIndex {
		return newTestIndexAt(t, dir, 1024, testKCentroids, testKDocsToScore, hfreshEFConstruction,
			withDistanceProvider(distancer.NewDotProductProvider()))
	}

	tf := open()
	const postingID = 1
	stay := tokenRef{DocID: 1, Pos: 0}
	centroid := make([]float32, testDim)
	centroid[0] = 1
	registerPosting(t, &tf, postingID, []tokenRef{stay}, centroid)

	// 300 tokens come and go, as reassigns move them
	for i := range 300 {
		ref := tokenRef{DocID: uint32(100 + i), Pos: 0}
		added, err := tf.Index.append(t.Context(), ref, postingID)
		require.NoError(t, err)
		require.True(t, added)
		removed, err := tf.Index.removeToken(t.Context(), postingID, ref)
		require.NoError(t, err)
		require.True(t, removed)
	}
	require.Equal(t, []tokenRef{stay}, tf.posting(postingID))
	tf.close(t)

	reopened := open()
	require.Equal(t, []tokenRef{stay}, reopened.posting(postingID))
	reopened.close(t)
}

func TestRestoreRefusesAnotherPostingFormat(t *testing.T) {
	tf := newTestIndex(t, testMaxPostingSize, testKCentroids, testKDocsToScore, hfreshEFConstruction)
	centroid := make([]float32, testDim)
	centroid[0] = 1
	registerPosting(t, &tf, 1, []tokenRef{{DocID: 1, Pos: 0}}, centroid)
	require.NoError(t, tf.Index.restore(0))

	tests := []struct {
		name  string
		value []byte
	}{
		{name: "an older format", value: []byte{1}},
		{name: "no format: saved before the key existed", value: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, release, err := tf.Index.types.acquire()
			require.NoError(t, err)
			if tt.value == nil {
				require.NoError(t, bucket.Delete(postingFormatKey))
			} else {
				require.NoError(t, bucket.Put(postingFormatKey, tt.value))
			}
			release()
			require.ErrorContains(t, tf.Index.restore(0), "must be rebuilt")
		})
	}
}

// A Remove is recorded under the "remove" store operation and not under "put".
func TestPostingStoreRemoveMetric(t *testing.T) {
	prom := monitoring.GetMetrics()
	count := func(t *testing.T, class, op string) uint64 {
		t.Helper()
		obs, err := prom.VectorIndexStoreOperationsDurations.GetMetricWith(prometheus.Labels{
			"class_name": class, "shard_name": "shard", "operation": op,
		})
		require.NoError(t, err)
		var out dto.Metric
		require.NoError(t, obs.(prometheus.Metric).Write(&out))
		return out.GetSummary().GetSampleCount()
	}

	// a label of its own per run: the registry is the process's
	class := fmt.Sprintf("%s-%d", t.Name(), metricsClasses.Add(1))
	tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction)
	tf.Index.PostingStore.metrics = hfresh.NewMetrics(prom, class, "shard")
	ref := tokenRef{DocID: 1, Pos: 2}
	require.NoError(t, tf.Index.PostingStore.Append(7, ref))
	require.NoError(t, tf.Index.PostingStore.Remove(7, ref))

	require.Equal(t, uint64(1), count(t, class, "remove"))
	require.Equal(t, uint64(0), count(t, class, "put"))
	require.Equal(t, uint64(1), count(t, class, "append"))
}

// A posting whose key was written and whose centroid never reached the disk
// keeps its key after a restart: the crash a split can leave behind. The key
// is not read, because restore reads the postings of live centroids, and the
// id is not reused. It is dropped once a later centroid id passes it.
func TestPostingKeyAboveTheLastCentroid(t *testing.T) {
	dir := t.TempDir()
	open := func() testIndex {
		return newTestIndexAt(t, dir, testMaxPostingSize, testKCentroids, testKDocsToScore, hfreshEFConstruction,
			withDistanceProvider(distancer.NewDotProductProvider()))
	}
	centroid := make([]float32, testDim)
	centroid[0] = 1
	stay := tokenRef{DocID: 1, Pos: 0}
	orphan := tokenRef{DocID: 2, Pos: 0}

	tf := open()
	registerPosting(t, &tf, 1, []tokenRef{stay}, centroid)
	const orphanID = 5
	require.NoError(t, tf.Index.PostingStore.Append(orphanID, orphan))
	tf.close(t)

	reopened := open()
	require.Equal(t, []tokenRef{stay}, reopened.posting(1))
	require.Empty(t, reopened.posting(orphanID), "not loaded")
	require.False(t, reopened.Index.PostingStore.isDead(orphanID), "not above the last centroid's id yet")
	got, err := reopened.Index.PostingStore.Get(orphanID)
	require.NoError(t, err)
	require.Equal(t, []tokenRef{orphan}, got, "the key stays on disk")

	registerPosting(t, &reopened, orphanID+1, []tokenRef{stay}, centroid)
	reopened.close(t)

	again := open()
	require.True(t, again.Index.PostingStore.isDead(orphanID), "a later centroid id passed it")
	again.close(t)
}

// A split, then a restart: the old posting's id is dead and the entries come
// back from the new postings alone.
func TestReopenAfterSplit(t *testing.T) {
	dir := t.TempDir()
	open := func() testIndex {
		return newTestIndexAt(t, dir, 10, 8, 8, hfreshEFConstruction,
			withDistanceProvider(distancer.NewDotProductProvider()))
	}
	tf := open()
	vectors := createTestVectors(4, 15)
	postingID, refs := createPostingWithTokens(t, &tf, vectors, 200)
	registerPosting(t, &tf, postingID, refs, meanVector(vectors))
	require.NoError(t, tf.Index.doSplit(t.Context(), postingID, false))
	tf.close(t)

	reopened := open()
	require.False(t, reopened.Index.Centroids.Exists(postingID))
	require.True(t, reopened.Index.PostingStore.isDead(postingID))
	require.Empty(t, reopened.posting(postingID))
	require.ElementsMatch(t, refs, append(reopened.posting(postingID+1), reopened.posting(postingID+2)...))
	reopened.close(t)
}
