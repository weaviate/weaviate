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
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

// Every operation must report a bucket that is already gone rather than
// working on a stale pointer. This covers the resolve step only: teardown has
// completed before the operation starts. The concurrent case — teardown
// landing mid-operation, which the resolve alone cannot catch — is pinned by
// TestBucketRefAcquirePinsAgainstShutdown and
// TestHFreshIterationPinsBucketForItsWholeDuration.
func TestHFreshStoresOnTornDownStore(t *testing.T) {
	ctx := context.Background()

	newStores := func(t *testing.T) (*lsmkv.Store, bucketRef, *PostingStore) {
		t.Helper()

		store := testinghelpers.NewDummyStore(t)
		cfg := StoreConfig{MakeBucketOptions: lsmkv.MakeNoopBucketOptions}
		shared, err := NewSharedBucket(store, "torn", cfg)
		require.NoError(t, err)
		postings, err := NewPostingStore(store, shared, NewMetrics(nil, "n/a", "n/a"), "torn", cfg)
		require.NoError(t, err)

		// One real write before the teardown. It leaves data a stale read
		// could return, and it caches posting 1's version, so the operations
		// below reach the postings bucket instead of stopping at the version
		// lookup that precedes it.
		var posting Posting
		posting = posting.AddVector(NewVector(1, 1, []byte{1, 2, 3, 4, 5, 6, 7, 8}))
		require.NoError(t, postings.Put(ctx, 1, posting))

		return store, shared, postings
	}

	tests := []struct {
		name string
		op   func(shared bucketRef, postings *PostingStore) error
	}{
		{
			name: "posting get",
			op: func(_ bucketRef, p *PostingStore) error {
				_, err := p.Get(ctx, 1)
				return err
			},
		},
		{
			name: "posting put",
			op: func(_ bucketRef, p *PostingStore) error {
				var posting Posting
				posting = posting.AddVector(NewVector(1, 1, []byte{1, 2, 3, 4, 5, 6, 7, 8}))
				return p.Put(ctx, 1, posting)
			},
		},
		{
			name: "posting append",
			op: func(_ bucketRef, p *PostingStore) error {
				return p.Append(ctx, 1, NewVector(2, 1, []byte{1, 2, 3, 4, 5, 6, 7, 8}))
			},
		},
		{
			name: "posting version get",
			op: func(shared bucketRef, _ *PostingStore) error {
				_, err := NewPostingVersionsStore(shared).Get(ctx, 7)
				return err
			},
		},
		{
			name: "posting version set",
			op: func(shared bucketRef, _ *PostingStore) error {
				return NewPostingVersionsStore(shared).Set(ctx, 7, 3)
			},
		},
		{
			name: "vector version get",
			op: func(shared bucketRef, _ *PostingStore) error {
				_, err := NewVersionStore(shared).Get(ctx, 7)
				return err
			},
		},
		{
			name: "vector version set",
			op: func(shared bucketRef, _ *PostingStore) error {
				return NewVersionStore(shared).Set(ctx, 7, 2)
			},
		},
		{
			name: "vector version iterate",
			op: func(shared bucketRef, _ *PostingStore) error {
				return NewVersionStore(shared).IterateAll(func(uint64, VectorVersion) bool { return true })
			},
		},
		{
			name: "posting map get",
			op: func(shared bucketRef, _ *PostingStore) error {
				_, err := NewPostingMapStore(shared, postingMapBucketPrefixV2).Get(ctx, 7)
				return err
			},
		},
		{
			name: "posting map set",
			op: func(shared bucketRef, _ *PostingStore) error {
				return NewPostingMapStore(shared, postingMapBucketPrefixV2).Set(ctx, 7, PackedPostingMetadata{})
			},
		},
		{
			name: "posting map delete",
			op: func(shared bucketRef, _ *PostingStore) error {
				return NewPostingMapStore(shared, postingMapBucketPrefixV2).Delete(ctx, 7)
			},
		},
		{
			name: "posting map iterate",
			op: func(shared bucketRef, _ *PostingStore) error {
				return NewPostingMapStore(shared, postingMapBucketPrefixV2).
					Iter(ctx, func(uint64, PackedPostingMetadata) error { return nil })
			},
		},
		{
			name: "posting size get",
			op: func(shared bucketRef, _ *PostingStore) error {
				_, err := NewPostingSizesStore(shared, postingSizesBucketPrefix).Get(ctx, 7)
				return err
			},
		},
		{
			name: "posting size set",
			op: func(shared bucketRef, _ *PostingStore) error {
				return NewPostingSizesStore(shared, postingSizesBucketPrefix).Set(ctx, 7, 3)
			},
		},
		{
			name: "posting size iterate",
			op: func(shared bucketRef, _ *PostingStore) error {
				return NewPostingSizesStore(shared, postingSizesBucketPrefix).
					Iter(ctx, func(uint64, uint32) error { return nil })
			},
		},
		{
			name: "index metadata get dimensions",
			op: func(shared bucketRef, _ *PostingStore) error {
				_, err := NewIndexMetadataStore(shared).GetDimensions()
				return err
			},
		},
		{
			name: "index metadata set dimensions",
			op: func(shared bucketRef, _ *PostingStore) error {
				return NewIndexMetadataStore(shared).SetDimensions(64)
			},
		},
		{
			name: "id sequence load",
			op: func(shared bucketRef, _ *PostingStore) error {
				_, err := NewBucketStore(shared).Load()
				return err
			},
		},
		{
			name: "id sequence store",
			op: func(shared bucketRef, _ *PostingStore) error {
				return NewBucketStore(shared).Store(42)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, shared, postings := newStores(t)
			require.NoError(t, store.Shutdown(ctx))

			require.ErrorIs(t, test.op(shared, postings), lsmkv.ErrBucketNotFound)
		})
	}
}
