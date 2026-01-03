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

	"github.com/maypok86/otter/v2"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// PostingVersions keeps track of the version of the posting list.
// Versions are incremented on each Put operation to the posting list,
// and allow for simpler cleanup of stale data during LSMKV compactions.
// It uses a combination of an LSMKV store for persistence and an in-memory
// cache for fast access.
type PostingVersions struct {
	metrics *Metrics
	cache   *otter.Cache[uint64, uint32]
	store   *PostingSizeStore
}

func NewPostingVersions(bucket *lsmkv.Bucket, metrics *Metrics) (*PostingVersions, error) {
	pStore := NewPostingSizeStore(bucket, postingVersionBucketPrefix)

	cache, err := otter.New(&otter.Options[uint64, uint32]{
		MaximumSize: 10_000,
	})
	if err != nil {
		return nil, err
	}

	return &PostingVersions{
		cache:   cache,
		metrics: metrics,
		store:   pStore,
	}, nil
}

// Get returns the size of the posting with the given ID.
func (v *PostingVersions) Get(ctx context.Context, postingID uint64) (uint32, error) {
	version, err := v.cache.Get(ctx, postingID, otter.LoaderFunc[uint64, uint32](func(ctx context.Context, key uint64) (uint32, error) {
		version, err := v.store.Get(ctx, postingID)
		if err != nil {
			if errors.Is(err, ErrPostingNotFound) {
				return 0, otter.ErrNotFound
			}

			return 0, err
		}

		return version, nil
	}))
	if errors.Is(err, otter.ErrNotFound) {
		v.store.Set(ctx, postingID, 0)
		v.cache.Set(postingID, 0)
		return 0, nil
	}

	return version, err
}

// Sets the size of the posting to newSize.
// This method assumes the posting has been locked for writing by the caller.
func (v *PostingVersions) Set(ctx context.Context, postingID uint64, newSize uint32) error {
	err := v.store.Set(ctx, postingID, newSize)
	if err != nil {
		return err
	}
	v.cache.Set(postingID, newSize)
	v.metrics.ObservePostingSize(float64(newSize))
	return nil
}

// Incr increments the size of the posting by delta and returns the new size.
// This method assumes the posting has been locked for writing by the caller.
func (v *PostingVersions) Inc(ctx context.Context, postingID uint64, delta uint32) (uint32, error) {
	old, err := v.Get(ctx, postingID)
	if err != nil {
		if !errors.Is(err, ErrPostingNotFound) {
			return 0, err
		}
	}

	newSize := old + delta
	err = v.store.Set(ctx, postingID, newSize)
	if err != nil {
		return 0, err
	}

	v.cache.Set(postingID, newSize)
	v.metrics.ObservePostingSize(float64(newSize))
	return newSize, nil
}
