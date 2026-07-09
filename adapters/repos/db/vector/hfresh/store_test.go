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

package hfresh

import (
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

func NewPostingStoreTest(store *lsmkv.Store, metrics *Metrics, id string, cfg StoreConfig) (*PostingStore, error) {
	bucket, err := NewSharedBucket(store, id, cfg)
	if err != nil {
		return nil, err
	}

	logger, _ := test.NewNullLogger()
	return NewPostingStore(store, bucket, metrics, logger, id, cfg)
}

func TestStore(t *testing.T) {
	ctx := t.Context()
	t.Run("get", func(t *testing.T) {
		store := testinghelpers.NewDummyStore(t)
		s, err := NewPostingStoreTest(store, NewMetrics(nil, "n/a", "n/a"), "test_bucket", StoreConfig{
			MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
		})
		require.NoError(t, err)

		// unknown posting
		p, err := s.Get(ctx, 1)
		require.Nil(t, err)
		require.Equal(t, len(p), 0)

		// create a posting
		var posting Posting
		posting = posting.AddVector(NewVector(1, 1, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}))
		err = s.Put(ctx, 1, posting)
		require.NoError(t, err)

		// get the posting
		p, err = s.Get(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, posting, p)

		// get a different posting
		p, err = s.Get(ctx, 2)
		require.Nil(t, err)
		require.Equal(t, len(p), 0)
	})

	t.Run("multi-get", func(t *testing.T) {
		store := testinghelpers.NewDummyStore(t)
		s, err := NewPostingStoreTest(store, NewMetrics(nil, "n/a", "n/a"), "test_bucket", StoreConfig{
			MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
		})
		require.NoError(t, err)

		// nil
		ps, err := s.MultiGet(ctx, nil)
		require.NoError(t, err)
		require.Len(t, ps, 0)

		// unknown postings
		ps, err = s.MultiGet(ctx, []uint64{1, 2, 3})
		require.Nil(t, err)
		require.Equal(t, len(ps[0]), 0)
		require.Equal(t, len(ps[1]), 0)
		require.Equal(t, len(ps[2]), 0)

		var postings []Posting
		// create a few postings
		for i := range 5 {
			var posting Posting
			posting = posting.AddVector(NewVector(uint64(i), 1, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}))
			postings = append(postings, posting)
			err = s.Put(ctx, uint64(i), posting)
			require.NoError(t, err)
		}

		// get some postings
		ps, err = s.MultiGet(ctx, []uint64{1, 2, 4})
		require.NoError(t, err)
		require.Len(t, ps, 3)
		require.Equal(t, postings[1], ps[0])
		require.Equal(t, postings[2], ps[1])
		require.Equal(t, postings[4], ps[2])
	})

	t.Run("multi-get with stats", func(t *testing.T) {
		store := testinghelpers.NewDummyStore(t)
		s, err := NewPostingStoreTest(store, NewMetrics(nil, "n/a", "n/a"), "test_bucket", StoreConfig{
			MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
		})
		require.NoError(t, err)

		var postings []Posting
		for i := range 3 {
			var posting Posting
			posting = posting.AddVector(NewVector(uint64(i), 1, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}))
			postings = append(postings, posting)
			err = s.Put(ctx, uint64(i), posting)
			require.NoError(t, err)
		}

		// nil stats is allowed (equivalent to MultiGet)
		ps, err := s.MultiGetWithStats(ctx, []uint64{0, 1}, nil)
		require.NoError(t, err)
		require.Len(t, ps, 2)

		// two found postings and one miss must be reflected in the stats
		var st searchStats
		ps, err = s.MultiGetWithStats(ctx, []uint64{0, 2, 99}, &st)
		require.NoError(t, err)
		require.Len(t, ps, 3)
		require.Equal(t, postings[0], ps[0])
		require.Equal(t, postings[2], ps[1])
		require.Empty(t, ps[2])

		vectorLen := 8 + 1 + 10 // id + version + data
		require.EqualValues(t, 2, st.PostingsRead)
		require.EqualValues(t, 1, st.PostingsEmpty)
		// dummy store keeps everything in the active memtable
		require.EqualValues(t, 2, st.MemtableReads)
		require.EqualValues(t, 0, st.SegmentReads)
		require.EqualValues(t, 2*vectorLen, st.PostingBytes)
	})

	t.Run("multi-get parallel matches sequential", func(t *testing.T) {
		store := testinghelpers.NewDummyStore(t)
		s, err := NewPostingStoreTest(store, NewMetrics(nil, "n/a", "n/a"), "test_bucket", StoreConfig{
			MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
		})
		require.NoError(t, err)

		// postings of varying sizes, with holes (odd IDs are missing)
		ids := make([]uint64, 0, 100)
		for i := range uint64(100) {
			ids = append(ids, i)
			if i%2 == 1 {
				continue
			}
			var posting Posting
			for j := range int(i%7) + 1 {
				posting = posting.AddVector(NewVector(i*1000+uint64(j), 1, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}))
			}
			require.NoError(t, s.Put(ctx, i, posting))
		}

		var seqStats searchStats
		s.readConcurrency = 1
		seq, err := s.MultiGetWithStats(ctx, ids, &seqStats)
		require.NoError(t, err)

		var parStats searchStats
		s.readConcurrency = 8
		par, err := s.MultiGetWithStats(ctx, ids, &parStats)
		require.NoError(t, err)

		// same postings, same order
		require.Equal(t, seq, par)

		// same aggregate IO accounting
		require.Equal(t, seqStats, parStats)
		require.EqualValues(t, 50, parStats.PostingsRead)
		require.EqualValues(t, 50, parStats.PostingsEmpty)
	})

	t.Run("multi-get stream delivers every posting exactly once", func(t *testing.T) {
		store := testinghelpers.NewDummyStore(t)
		s, err := NewPostingStoreTest(store, NewMetrics(nil, "n/a", "n/a"), "test_bucket", StoreConfig{
			MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
		})
		require.NoError(t, err)

		want := make(map[int]Posting) // index in the request -> expected posting
		ids := make([]uint64, 0, 60)
		for i := range uint64(60) {
			ids = append(ids, i)
			if i%3 == 2 { // holes
				want[int(i)] = Posting{}
				continue
			}
			var posting Posting
			for j := range int(i%5) + 1 {
				posting = posting.AddVector(NewVector(i*1000+uint64(j), 1, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}))
			}
			require.NoError(t, s.Put(ctx, i, posting))
			want[int(i)] = posting
		}

		// reference stats from the barrier API
		var wantStats searchStats
		_, err = s.MultiGetWithStats(ctx, ids, &wantStats)
		require.NoError(t, err)

		for _, concurrency := range []int{1, 8} {
			s.readConcurrency = concurrency

			var st searchStats
			ch, wait := s.MultiGetStreamWithStats(ctx, ids, &st)

			got := make(map[int]Posting)
			for res := range ch {
				_, dup := got[res.Index]
				require.False(t, dup, "index %d delivered twice", res.Index)
				got[res.Index] = res.Posting
			}
			require.NoError(t, wait())

			require.Len(t, got, len(ids))
			for i, posting := range want {
				require.Equal(t, len(posting), len(got[i]), "posting at index %d", i)
				if len(posting) > 0 {
					require.Equal(t, posting, got[i])
				}
			}

			// stats must match the barrier API's accounting
			require.Equal(t, wantStats, st, "concurrency=%d", concurrency)
		}
	})

	t.Run("multi-get stream with no ids", func(t *testing.T) {
		store := testinghelpers.NewDummyStore(t)
		s, err := NewPostingStoreTest(store, NewMetrics(nil, "n/a", "n/a"), "test_bucket", StoreConfig{
			MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
		})
		require.NoError(t, err)

		ch, wait := s.MultiGetStreamWithStats(ctx, nil, nil)
		for range ch {
			t.Fatal("no postings expected")
		}
		require.NoError(t, wait())
	})

	t.Run("put", func(t *testing.T) {
		store := testinghelpers.NewDummyStore(t)
		s, err := NewPostingStoreTest(store, NewMetrics(nil, "n/a", "n/a"), "test_bucket", StoreConfig{
			MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
		})
		require.NoError(t, err)

		// nil posting
		err = s.Put(ctx, 1, nil)
		require.Error(t, err)

		// empty posting
		err = s.Put(ctx, 1, Posting{})
		require.NoError(t, err)

		version, err := s.versions.Get(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, uint8(1), version)

		err = s.Put(ctx, 1, Posting{})
		require.NoError(t, err)

		version, err = s.versions.Get(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, uint8(2), version)
	})

	t.Run("append", func(t *testing.T) {
		store := testinghelpers.NewDummyStore(t)
		s, err := NewPostingStoreTest(store, NewMetrics(nil, "n/a", "n/a"), "test_bucket", StoreConfig{
			MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
		})
		require.NoError(t, err)

		v := NewVector(1, 1, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

		// append to non-existing posting
		err = s.Append(ctx, 1, v)
		require.NoError(t, err)

		p, err := s.Get(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, 1, len(p))
		require.Equal(t, v, p[0])

		// append to existing posting
		v2 := NewVector(1, 1, []byte{11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
		err = s.Append(ctx, 1, v2)
		require.NoError(t, err)

		p, err = s.Get(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, 2, len(p))
		require.Equal(t, v, p[0])
		require.Equal(t, v2, p[1])
	})
}
