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
	"encoding/binary"
	"fmt"

	"github.com/maypok86/otter/v2"
	"github.com/pkg/errors"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// These constants define the prefixes used in the
// lsmkv bucket to namespace different types of data.
const (
	postingSizeBucketPrefix    = 's'
	postingVersionBucketPrefix = 'l'
	versionMapBucketPrefix     = 'v'
	metadataBucketPrefix       = 'm'
	reassignBucketKey          = "pending_reassignments"
)

// NewSharedBucket creates a shared lsmkv bucket for the HFresh index.
// This bucket is used to store metadata in namespaced regions of the bucket.
func NewSharedBucket(store *lsmkv.Store, indexID string, cfg StoreConfig) (*lsmkv.Bucket, error) {
	bName := sharedBucketName(indexID)
	err := store.CreateOrLoadBucket(context.Background(),
		bName,
		cfg.MakeBucketOptions(lsmkv.StrategyReplace, lsmkv.WithForceCompaction(true))...,
	)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create or load bucket %s", bName)
	}

	return store.Bucket(bName), nil
}

func sharedBucketName(id string) string {
	return fmt.Sprintf("hfresh_shared_%s", id)
}

// PostingSizes keeps track of the number of vectors in each posting.
// It uses a combination of an LSMKV store for persistence and an in-memory
// cache for fast access.
type PostingSizes struct {
	metrics    *Metrics
	cache      *otter.Cache[uint64, uint32]
	store      *PostingSizeStore
	postingIDs *xsync.Map[uint64, struct{}]
}

func NewPostingSizes(bucket *lsmkv.Bucket, metrics *Metrics) (*PostingSizes, error) {
	pStore := NewPostingSizeStore(bucket, postingSizeBucketPrefix)

	cache, err := otter.New(&otter.Options[uint64, uint32]{
		MaximumSize: 10_000,
	})
	if err != nil {
		return nil, err
	}

	postingIDs := xsync.NewMap[uint64, struct{}]()
	// preload existing posting IDs into the map
	existingIDs, err := pStore.ListPostingIDs(context.Background())
	if err != nil {
		return nil, errors.Wrap(err, "failed to list existing posting IDs")
	}
	for _, id := range existingIDs {
		postingIDs.Store(id, struct{}{})
	}

	return &PostingSizes{
		cache:      cache,
		metrics:    metrics,
		store:      pStore,
		postingIDs: postingIDs,
	}, nil
}

// Get returns the size of the posting with the given ID.
func (v *PostingSizes) Get(ctx context.Context, postingID uint64) (uint32, error) {
	size, err := v.cache.Get(ctx, postingID, otter.LoaderFunc[uint64, uint32](func(ctx context.Context, key uint64) (uint32, error) {
		size, err := v.store.Get(ctx, postingID)
		if err != nil {
			if errors.Is(err, ErrPostingNotFound) {
				return 0, otter.ErrNotFound
			}

			return 0, err
		}

		return size, nil
	}))
	if errors.Is(err, otter.ErrNotFound) {
		return 0, ErrPostingNotFound
	}

	return size, err
}

// Sets the size of the posting to newSize.
// This method assumes the posting has been locked for writing by the caller.
func (v *PostingSizes) Set(ctx context.Context, postingID uint64, newSize uint32) error {
	err := v.store.Set(ctx, postingID, newSize)
	if err != nil {
		return err
	}
	v.cache.Set(postingID, newSize)
	if newSize > 0 {
		v.postingIDs.Store(postingID, struct{}{})
	} else {
		v.postingIDs.Delete(postingID)
	}
	v.metrics.ObservePostingSize(float64(newSize))
	return nil
}

// Incr increments the size of the posting by delta and returns the new size.
// This method assumes the posting has been locked for writing by the caller.
func (v *PostingSizes) Inc(ctx context.Context, postingID uint64, delta uint32) (uint32, error) {
	old, err := v.Get(ctx, postingID)
	if err != nil {
		if !errors.Is(err, ErrPostingNotFound) {
			return 0, err
		}
	}

	if old == 0 && delta > 0 {
		v.postingIDs.Store(postingID, struct{}{})
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

// ListPostingIDs returns the list of all posting IDs with non-zero size.
func (v *PostingSizes) ListPostingIDs(ctx context.Context) ([]uint64, error) {
	postingIDs := make([]uint64, 0, v.postingIDs.Size())
	v.postingIDs.Range(func(key uint64, value struct{}) bool {
		postingIDs = append(postingIDs, key)
		return true
	})
	return postingIDs, nil
}

// PostingSizeStore is a persistent store for posting sizes.
// It stores the sizes in an LSMKV bucket.
type PostingSizeStore struct {
	bucket    *lsmkv.Bucket
	keyPrefix byte
}

func NewPostingSizeStore(bucket *lsmkv.Bucket, keyPrefix byte) *PostingSizeStore {
	return &PostingSizeStore{
		bucket:    bucket,
		keyPrefix: keyPrefix,
	}
}

func (p *PostingSizeStore) key(postingID uint64) [9]byte {
	var buf [9]byte
	buf[0] = p.keyPrefix
	binary.LittleEndian.PutUint64(buf[1:], postingID)
	return buf
}

func (p *PostingSizeStore) Get(ctx context.Context, postingID uint64) (uint32, error) {
	key := p.key(postingID)
	v, err := p.bucket.Get(key[:])
	if err != nil {
		return 0, errors.Wrapf(err, "failed to get posting size for %d", postingID)
	}
	if len(v) == 0 {
		return 0, ErrPostingNotFound
	}

	return binary.LittleEndian.Uint32(v), nil
}

func (p *PostingSizeStore) Set(ctx context.Context, postingID uint64, size uint32) error {
	key := p.key(postingID)
	return p.bucket.Put(key[:], binary.LittleEndian.AppendUint32(nil, size))
}

// ListPostingIDs returns a list of all posting IDs stored in the PostingSizeStore.
func (p *PostingSizeStore) ListPostingIDs(ctx context.Context) ([]uint64, error) {
	var postingIDs []uint64

	c := p.bucket.Cursor()
	defer c.Close()

	prefix := []byte{p.keyPrefix}
	for k, v := c.Seek(prefix); len(k) > 0 && k[0] == p.keyPrefix; k, v = c.Next() {
		if len(k) != 9 || len(v) != 4 {
			continue
		}

		postingID := binary.LittleEndian.Uint64(k[1:])
		// skip postings with size zero
		if binary.LittleEndian.Uint32(v) == 0 {
			continue
		}

		postingIDs = append(postingIDs, postingID)
	}

	return postingIDs, nil
}
