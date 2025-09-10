//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package spfresh

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

func TestStore(t *testing.T) {
	ctx := t.Context()
	t.Run("get", func(t *testing.T) {
		store := testinghelpers.NewDummyStore(t)
		s, err := NewLSMStore(store, "test_bucket", true)
		require.NoError(t, err)
		s.Init(10)

		// unknown posting
		p, err := s.Get(ctx, 1)
		require.ErrorIs(t, err, ErrPostingNotFound)
		require.Nil(t, p)

		// create a posting
		posting := CompressedPosting{
			vectorSize: 10,
		}
		posting.AddVector(NewCompressedVector(1, 1, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}))
		err = s.Put(ctx, 1, &posting)
		require.NoError(t, err)

		// get the posting
		p, err = s.Get(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, &posting, p)

		// get a different posting
		p, err = s.Get(ctx, 2)
		require.ErrorIs(t, err, ErrPostingNotFound)
		require.Nil(t, p)
	})

	t.Run("multi-get", func(t *testing.T) {
		store := testinghelpers.NewDummyStore(t)
		s, err := NewLSMStore(store, "test_bucket", true)
		require.NoError(t, err)
		s.Init(10)

		// nil
		ps, err := s.MultiGet(ctx, nil)
		require.NoError(t, err)
		require.Len(t, ps, 0)

		// unknown postings
		ps, err = s.MultiGet(ctx, []uint64{1, 2, 3})
		require.ErrorIs(t, err, ErrPostingNotFound)

		var postings []*CompressedPosting
		// create a few postings
		for i := range 5 {
			posting := CompressedPosting{
				vectorSize: 10,
			}
			posting.AddVector(NewCompressedVector(uint64(i), 1, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}))
			postings = append(postings, &posting)
			err = s.Put(ctx, uint64(i), &posting)
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
		s, err := NewLSMStore(store, "test_bucket", true)
		require.NoError(t, err)
		s.Init(10)

		// nil posting
		err = s.Put(ctx, 1, nil)
		require.Error(t, err)

		// empty posting
		err = s.Put(ctx, 1, &CompressedPosting{vectorSize: 10})
		require.NoError(t, err)
	})
}
