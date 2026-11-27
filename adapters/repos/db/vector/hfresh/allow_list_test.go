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
	"fmt"
	"iter"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/visited"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

func TestPostingPassesFilterSlice_PassesIfAddsNewVector(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	bucket, err := NewSharedBucket(store, "test", StoreConfig{MakeBucketOptions: lsmkv.MakeNoopBucketOptions})
	require.NoError(t, err)
	pm := NewPostingMap(bucket)
	ctx := t.Context()

	// posting 0 has allowed vector 0
	pm.FastAddVectorID(ctx, 0, 0)

	// posting 1 has allowed vector 1 (different allowed vector)
	pm.FastAddVectorID(ctx, 1, 1)

	hf := &HFresh{
		visitedPool: visited.NewPool(10),
		PostingMap:  pm,
	}

	allowList := helpers.NewAllowList(0, 1)
	hfAllowList := hf.wrapAllowList(ctx, allowList)

	ok := hfAllowList.Contains(0) // pass: holds allowed vector 0
	require.True(t, ok)

	ok = hfAllowList.Contains(1) // pass: holds allowed vector 1
	require.True(t, ok)

	hfAllowList.Close()
}

func TestPostingPassesFilterSlice_IgnoresDisallowedVectors(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	bucket, err := NewSharedBucket(store, "test", StoreConfig{MakeBucketOptions: lsmkv.MakeNoopBucketOptions})
	require.NoError(t, err)
	pm := NewPostingMap(bucket)
	ctx := t.Context()

	// postings only have vectors that are NOT in allowList
	pm.FastAddVectorID(ctx, 0, 10)
	pm.FastAddVectorID(ctx, 0, 11)
	pm.FastAddVectorID(ctx, 1, 12)

	hf := &HFresh{
		visitedPool: visited.NewPool(10),
		PostingMap:  pm,
	}

	allowList := helpers.NewAllowList(0) // only vector 0 allowed
	hfAllowList := hf.wrapAllowList(ctx, allowList)

	ok := hfAllowList.Contains(0)
	require.False(t, ok)

	ok = hfAllowList.Contains(1)
	require.False(t, ok)

	hfAllowList.Close()
}

func TestPostingPassesFilterSlice_SkipsAlreadyUsedAndFindsLaterNewOne(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	bucket, err := NewSharedBucket(store, "test", StoreConfig{MakeBucketOptions: lsmkv.MakeNoopBucketOptions})
	require.NoError(t, err)
	pm := NewPostingMap(bucket)
	ctx := t.Context()

	// posting 0 holds allowed vector 0
	pm.FastAddVectorID(ctx, 0, 0)

	// posting 1 shares vector 0 and adds vector 1: membership is pure, so
	// probing posting 0 first must not affect posting 1
	pm.FastAddVectorID(ctx, 1, 0)
	pm.FastAddVectorID(ctx, 1, 1)

	hf := &HFresh{
		visitedPool: visited.NewPool(10),
		PostingMap:  pm,
	}

	allowList := helpers.NewAllowList(0, 1)
	hfAllowList := hf.wrapAllowList(ctx, allowList)

	ok := hfAllowList.Contains(0)
	require.True(t, ok)

	ok = hfAllowList.Contains(1) // pure membership: unaffected by the earlier probe
	require.True(t, ok)

	hfAllowList.Close()
}

func TestPostingPassesFilterSlice_StopsOnFirstNewContribution(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	bucket, err := NewSharedBucket(store, "test", StoreConfig{MakeBucketOptions: lsmkv.MakeNoopBucketOptions})
	require.NoError(t, err)
	pm := NewPostingMap(bucket)
	ctx := t.Context()

	// posting 0 holds [0, 1], posting 1 holds only [1]: probing posting 0
	// must not make posting 1 unreachable (the old claim machinery could)
	pm.FastAddVectorID(ctx, 0, 0)
	pm.FastAddVectorID(ctx, 0, 1)

	pm.FastAddVectorID(ctx, 1, 1)

	hf := &HFresh{
		visitedPool: visited.NewPool(10),
		PostingMap:  pm,
	}

	allowList := helpers.NewAllowList(0, 1)
	hfAllowList := hf.wrapAllowList(ctx, allowList)

	ok := hfAllowList.Contains(0)
	require.True(t, ok)

	ok = hfAllowList.Contains(1)
	require.True(t, ok)

	hfAllowList.Close()
}

func TestPostingPassesFilterSlice_CachesAcceptedPostingID(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	bucket, err := NewSharedBucket(store, "test", StoreConfig{MakeBucketOptions: lsmkv.MakeNoopBucketOptions})
	require.NoError(t, err)
	pm := NewPostingMap(bucket)
	ctx := t.Context()

	// posting 0 has allowed vector 0
	pm.FastAddVectorID(ctx, 0, 0)

	hf := &HFresh{
		visitedPool: visited.NewPool(10),
		PostingMap:  pm,
	}

	allowList := helpers.NewAllowList(0)
	hfAllowList := hf.wrapAllowList(ctx, allowList)

	ok := hfAllowList.Contains(0)
	require.True(t, ok)

	// second call answers from the memoized allowed-posting set
	ok = hfAllowList.Contains(0)
	require.True(t, ok)

	hfAllowList.Close()
}

func TestAllowListIteratorReturnsAcceptedPostingWithSingleAllowedVector(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	bucket, err := NewSharedBucket(store, "test", StoreConfig{MakeBucketOptions: lsmkv.MakeNoopBucketOptions})
	require.NoError(t, err)
	pm := NewPostingMap(bucket)
	ctx := t.Context()

	pm.FastAddVectorID(ctx, 0, 0)

	hf := &HFresh{
		visitedPool: visited.NewPool(10),
		PostingMap:  pm,
	}

	allowList := helpers.NewAllowList(0)
	hfAllowList := hf.wrapAllowList(ctx, allowList)
	defer hfAllowList.Close()

	next, stop := iter.Pull2(hf.PostingMap.Iter())
	defer stop()

	iterator := &AllowListIterator{
		allowList: hfAllowList,
		next:      next,
		stop:      func() {},
		len:       1,
	}

	id, ok := iterator.Next()
	require.True(t, ok)
	require.EqualValues(t, 0, id)
}

// Each probe is uncached, including successful probes, which normally memoize
// the posting for the rest of the query.
func TestAllowListContainsAllowedMemberZeroAlloc(t *testing.T) {
	for _, maxID := range []uint64{1<<16 - 1, 1<<24 - 1, 1<<32 - 1, 1<<40 - 1, ^uint64(0)} {
		t.Run(fmt.Sprint(schemeFor(maxID).BytesPerValue()), func(t *testing.T) {
			h := &HFresh{PostingMap: NewPostingMap(bucketRef{}), visitedPool: visited.NewPool(10)}
			middle := maxID & 0x0102030405060708
			h.PostingMap.setSlot(1, NewPackedPostingMetadata([]uint64{0, middle, maxID}))
			for _, allowedID := range []uint64{1, 0, middle, maxID} {
				t.Run(fmt.Sprint(allowedID), func(t *testing.T) {
					match := allowedID != 1
					allowed := helpers.NewAllowList(allowedID)
					defer allowed.Close()
					a := h.wrapAllowList(t.Context(), allowed).(*allowList)
					defer a.Close()
					allocs := testing.AllocsPerRun(100, func() {
						a.allowedPostings.Reset()
						if got := a.Contains(1); got != match {
							t.Fatalf("membership = %v, want %v", got, match)
						}
					})
					require.Zero(t, allocs, "uncached posting probe must not allocate")
				})
			}
		})
	}
}

func BenchmarkAllowListContains(b *testing.B) {
	for _, maxID := range []uint64{1<<16 - 1, 1<<24 - 1, 1<<32 - 1, 1<<40 - 1, ^uint64(0)} {
		b.Run(fmt.Sprint(schemeFor(maxID).BytesPerValue()), func(b *testing.B) {
			ids := make([]uint64, 200)
			for i := range ids {
				ids[i] = maxID - uint64(len(ids)-i-1)
			}
			h := &HFresh{PostingMap: NewPostingMap(bucketRef{}), visitedPool: visited.NewPool(10)}
			h.PostingMap.setSlot(1, NewPackedPostingMetadata(ids))
			for _, match := range []bool{false, true} {
				b.Run(fmt.Sprint(match), func(b *testing.B) {
					allowed := helpers.NewAllowList(0)
					defer allowed.Close()
					if match {
						allowed.Insert(maxID)
					}
					a := h.wrapAllowList(b.Context(), allowed).(*allowList)
					defer a.Close()
					b.ReportAllocs()
					for b.Loop() {
						a.allowedPostings.Reset()
						if a.Contains(1) != match {
							b.Fatal("unexpected membership")
						}
					}
				})
			}
		})
	}
}

func TestAllowListPackedMetadata(t *testing.T) {
	h := &HFresh{PostingMap: NewPostingMap(bucketRef{}), visitedPool: visited.NewPool(10)}
	allowed := helpers.NewAllowList(0, 10)
	defer allowed.Close()
	a := h.wrapAllowList(t.Context(), allowed).(*allowList)
	defer a.Close()
	for n := range 6 {
		t.Run(fmt.Sprintf("short/%d", n), func(t *testing.T) {
			h.PostingMap.setSlot(1, make(PackedPostingMetadata, n))
			require.False(t, a.Contains(1))
		})
	}
	// Unknown schemes retain the same eight-byte interpretation as Iter.
	data := NewPackedPostingMetadata([]uint64{^uint64(0), 10})
	data[0] = 255
	h.PostingMap.setSlot(1, data)
	require.True(t, a.Contains(1))
}

// countingAllowList measures document membership checks rather than posting probes.
type countingAllowList struct {
	helpers.AllowList
	calls int
}

func (a *countingAllowList) Contains(id uint64) bool {
	a.calls++
	return a.AllowList.Contains(id)
}

func newPostingAllowList(t testing.TB) *HFresh {
	t.Helper()
	store := testinghelpers.NewDummyStore(t)
	bucket, err := NewSharedBucket(store, "allow-list", StoreConfig{MakeBucketOptions: lsmkv.MakeNoopBucketOptions})
	require.NoError(t, err)
	return &HFresh{PostingMap: NewPostingMap(bucket), visitedPool: visited.NewPool(512)}
}

func TestAllowListCachesRejectedMetadata(t *testing.T) {
	for _, iteratorFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("iterator-first=%t", iteratorFirst), func(t *testing.T) {
			h := newPostingAllowList(t)
			for id := range uint64(minCachedPostingMembers) {
				_, err := h.PostingMap.FastAddVectorID(t.Context(), 1, id)
				require.NoError(t, err)
			}
			inner := &countingAllowList{AllowList: helpers.NewAllowList(999)}
			defer inner.Close()
			al := h.wrapAllowList(t.Context(), inner)
			if !iteratorFirst {
				require.False(t, al.Contains(1))
			}
			next, stop := iter.Pull2(h.PostingMap.Iter())
			it := &AllowListIterator{allowList: al, next: next, stop: stop}
			_, ok := it.Next()
			require.False(t, ok)
			it.Stop()
			scans := 1
			if iteratorFirst {
				require.Nil(t, al.(*allowList).rejectedPostings, "an iterator-only pass needs no negative cache")
				scans++
			}
			require.False(t, al.Contains(1))
			require.False(t, al.Contains(1))
			require.Equal(t, scans*minCachedPostingMembers, inner.calls, "random re-probes must reuse rejected metadata")

			// Appending mutates the same metadata; rewriting installs a new one.
			_, err := h.PostingMap.FastAddVectorID(t.Context(), 1, 998)
			require.NoError(t, err)
			require.False(t, al.Contains(1))
			require.Equal(t, scans*minCachedPostingMembers+minCachedPostingMembers+1, inner.calls)
			replacement := Posting(nil)
			for id := range uint64(minCachedPostingMembers) {
				replacement = replacement.AddVector(NewVector(id, 1, nil))
			}
			replacement = replacement.AddVector(NewVector(999, 1, nil))
			require.NoError(t, h.PostingMap.SetVectorIDs(t.Context(), 1, replacement))
			require.True(t, al.Contains(1))
			require.False(t, al.Contains(0))
			_, err = h.PostingMap.FastAddVectorID(t.Context(), 0, 999)
			require.NoError(t, err)
			require.True(t, al.Contains(0), "an absent posting must not stay rejected after creation")

			cached := al.(*allowList).rejectedPostings
			allowed := al.(*allowList).allowedPostings
			al.Close()
			require.Empty(t, cached, "returning a cache must release its metadata references")
			require.False(t, allowed.Visited(1), "returning the visited set must reset it")
			other := helpers.NewAllowList(998)
			defer other.Close()
			nextQuery := h.wrapAllowList(t.Context(), other)
			require.False(t, nextQuery.Contains(1))
			_, err = h.PostingMap.FastAddVectorID(t.Context(), 1, 998)
			require.NoError(t, err)
			require.True(t, nextQuery.Contains(1), "an appended allowed member must be visible")
			nextQuery.Close()
		})
	}
}

func BenchmarkAllowListReprobe(b *testing.B) {
	h := newPostingAllowList(b)
	for id := range uint64(800) {
		_, err := h.PostingMap.FastAddVectorID(b.Context(), 1, id)
		require.NoError(b, err)
	}
	for _, allowed := range []uint64{0, 999} {
		b.Run(fmt.Sprintf("allowed=%d", allowed), func(b *testing.B) {
			inner := helpers.NewAllowList(allowed)
			defer inner.Close()
			b.ReportAllocs()
			for b.Loop() {
				al := h.wrapAllowList(b.Context(), inner)
				al.Contains(1)
				al.Contains(1)
				al.Close()
			}
		})
	}
}

func TestAllowListSkipsSmallRejectedPostings(t *testing.T) {
	h := newPostingAllowList(t)
	inner := helpers.NewAllowList(999)
	defer inner.Close()
	a := h.wrapAllowList(t.Context(), inner).(*allowList)
	for id := range uint64(minCachedPostingMembers - 1) {
		_, err := h.PostingMap.FastAddVectorID(t.Context(), 1, id)
		require.NoError(t, err)
	}
	require.False(t, a.Contains(1))
	require.False(t, a.Contains(1))
	require.Nil(t, a.rejectedPostings)
	_, err := h.PostingMap.FastAddVectorID(t.Context(), 1, minCachedPostingMembers-1)
	require.NoError(t, err)
	require.False(t, a.Contains(1))
	require.Len(t, a.rejectedPostings, 1)
	// A large query may exceed the pooling cap, but must still drop its cache.
	for range maxPooledRejectedPostings {
		a.rejectedPostings[&PostingMetadata{}] = [5]byte{}
	}
	cached := a.rejectedPostings
	a.Close()
	require.Nil(t, a.rejectedPostings)
	// Pooling clears the map first; an oversized map must bypass that path.
	// Inspect the retained alias because sync.Pool may discard any stored item.
	require.Len(t, cached, maxPooledRejectedPostings+1)
}

func TestAllowListRechecksAppendToPaddedMetadata(t *testing.T) {
	h := newPostingAllowList(t)
	ids := make([]uint64, minCachedPostingMembers)
	for i := range ids {
		ids[i] = uint64(i)
	}
	// Restore accepts a non-legacy suffix after the counted IDs. The next
	// append widens the encoding, so the header changes but the length does not.
	metadata := NewPackedPostingMetadata(ids)
	grown := NewPackedPostingMetadata(append(ids, 65536))
	metadata = append(metadata, make([]byte, len(grown)-len(metadata))...)
	require.NoError(t, h.PostingMap.bucket.Set(t.Context(), 1, metadata))
	require.NoError(t, h.PostingMap.Restore(t.Context()))
	inner := helpers.NewAllowList(65536)
	defer inner.Close()
	a := h.wrapAllowList(t.Context(), inner)
	defer a.Close()
	require.False(t, a.Contains(1))
	before, err := h.PostingMap.Get(t.Context(), 1)
	require.NoError(t, err)
	size := len(before.PackedPostingMetadata)
	_, err = h.PostingMap.FastAddVectorID(t.Context(), 1, 65536)
	require.NoError(t, err)
	require.Len(t, before.PackedPostingMetadata, size)
	require.True(t, a.Contains(1), "an appended allowed member must be visible even when packed length is unchanged")
}
