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

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

func NewPostingStoreTest(store *lsmkv.Store, metrics *Metrics, id string, cfg StoreConfig) (*PostingStore, error) {
	bucket, err := NewSharedBucket(store, id, cfg)
	if err != nil {
		return nil, err
	}

	return NewPostingStore(store, bucket, metrics, id, cfg)
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
