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
	"time"

	"github.com/maypok86/otter/v2"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
)

type PostingStore struct {
	store    *lsmkv.Store
	bucket   *lsmkv.Bucket
	locks    *common.ShardedRWLocks
	metrics  *Metrics
	versions *PostingVersionsStore
}

func NewPostingStore(store *lsmkv.Store, sharedBucket *lsmkv.Bucket, metrics *Metrics, id string, cfg StoreConfig) (*PostingStore, error) {
	bName := postingsBucketName(id)

	versions := NewPostingVersionsStore(sharedBucket)

	err := store.CreateOrLoadBucket(context.Background(),
		bName,
		cfg.MakeBucketOptions(
			lsmkv.StrategySetCollection,
			lsmkv.WithForceCompaction(true),
			lsmkv.WithShouldSkipKeyFunction(
				func(key []byte, ctx context.Context) (bool, error) {
					if len(key) != 12 {
						// don't skip on error
						return false, fmt.Errorf("invalid key length: %d", len(key))
					}
					postingId := binary.LittleEndian.Uint64(key[:8])
					segmentPostingVersion := binary.LittleEndian.Uint32(key[8:])
					currentPostingVersion, err := versions.Get(ctx, postingId)
					if err != nil {
						return false, errors.Wrap(err, "get posting version during compaction")
					}
					skip := segmentPostingVersion != currentPostingVersion
					return skip, nil
				},
			),
		)...,
	)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create or load bucket %s", bName)
	}

	return &PostingStore{
		store:    store,
		bucket:   store.Bucket(bName),
		locks:    common.NewDefaultShardedRWLocks(),
		metrics:  metrics,
		versions: versions,
	}, nil
}

func (p *PostingStore) getKeyBytes(ctx context.Context, postingID uint64) ([]byte, error) {
	var buf [12]byte
	binary.LittleEndian.PutUint64(buf[:], postingID)
	version, err := p.versions.Get(ctx, postingID)
	if err != nil {
		return nil, errors.Wrapf(err, "get posting version for id %d", postingID)
	}
	binary.LittleEndian.PutUint32(buf[8:], version)
	return buf[:], nil
}

func (p *PostingStore) Get(ctx context.Context, postingID uint64) (Posting, error) {
	start := time.Now()
	defer p.metrics.StoreGetDuration(start)

	p.locks.RLock(postingID)
	key, err := p.getKeyBytes(ctx, postingID)
	if err != nil {
		p.locks.RUnlock(postingID)
		return nil, err
	}
	list, err := p.bucket.SetList(key)
	p.locks.RUnlock(postingID)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get posting %d", postingID)
	}

	posting := Posting(make([]Vector, len(list)))

	for i, v := range list {
		posting[i] = Vector(v)
	}

	return posting, nil
}

func (p *PostingStore) MultiGet(ctx context.Context, postingIDs []uint64) ([]Posting, error) {
	postings := make([]Posting, 0, len(postingIDs))

	for _, id := range postingIDs {
		posting, err := p.Get(ctx, id)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get posting %d", id)
		}
		postings = append(postings, posting)
	}

	return postings, nil
}

func (p *PostingStore) Put(ctx context.Context, postingID uint64, posting Posting) error {
	start := time.Now()
	defer p.metrics.StorePutDuration(start)

	if posting == nil {
		return errors.New("posting cannot be nil")
	}

	p.locks.Lock(postingID)
	defer p.locks.Unlock(postingID)

	set := make([][]byte, len(posting))
	for i, v := range posting {
		set[i] = v
	}

	currentVersion, err := p.versions.Get(ctx, postingID)
	if err != nil {
		return err
	}
	newVersion := currentVersion + 1

	var buf [12]byte
	binary.LittleEndian.PutUint64(buf[:], postingID)
	binary.LittleEndian.PutUint32(buf[8:], newVersion)
	err = p.bucket.SetAdd(buf[:], set)
	if err != nil {
		return errors.Wrapf(err, "failed to put posting %d", postingID)
	}

	err = p.versions.Set(ctx, postingID, newVersion)
	if err != nil {
		return errors.Wrapf(err, "set new posting version for id %d", postingID)
	}

	return nil
}

func (p *PostingStore) Append(ctx context.Context, postingID uint64, vector Vector) error {
	start := time.Now()
	defer p.metrics.StoreAppendDuration(start)

	p.locks.Lock(postingID)
	defer p.locks.Unlock(postingID)

	key, err := p.getKeyBytes(ctx, postingID)
	if err != nil {
		return err
	}

	return p.bucket.SetAdd(key, [][]byte{vector})
}

func postingsBucketName(id string) string {
	return fmt.Sprintf("hfresh_postings_%s", id)
}

// PostingVersions keeps track of the version of the posting list.
// Versions are incremented on each Put operation to the posting list,
// and allow for simpler cleanup of stale data during LSMKV compactions.
// It uses a combination of an LSMKV store for persistence and an in-memory
// cache for fast access.
type PostingVersionsStore struct {
	bucket    *lsmkv.Bucket
	keyPrefix byte
	cache     *otter.Cache[uint64, uint32]
}

func NewPostingVersionsStore(bucket *lsmkv.Bucket) *PostingVersionsStore {
	cache, _ := otter.New[uint64, uint32](nil)

	return &PostingVersionsStore{
		bucket:    bucket,
		keyPrefix: postingVersionBucketPrefix,
		cache:     cache,
	}
}

func (p *PostingVersionsStore) key(postingID uint64) [9]byte {
	var buf [9]byte
	buf[0] = p.keyPrefix
	binary.LittleEndian.PutUint64(buf[1:], postingID)
	return buf
}

func (p *PostingVersionsStore) Get(ctx context.Context, postingID uint64) (uint32, error) {
	version, err := p.cache.Get(ctx, postingID, otter.LoaderFunc[uint64, uint32](func(ctx context.Context, key uint64) (uint32, error) {
		k := p.key(postingID)
		v, err := p.bucket.Get(k[:])
		if err != nil {
			return 0, errors.Wrapf(err, "failed to get posting size for %d", postingID)
		}
		if len(v) == 0 {
			return 0, otter.ErrNotFound
		}

		return binary.LittleEndian.Uint32(v), nil
	}))
	if errors.Is(err, otter.ErrNotFound) {
		return 0, nil
	}

	return version, err
}

func (p *PostingVersionsStore) Set(ctx context.Context, postingID uint64, version uint32) error {
	key := p.key(postingID)
	err := p.bucket.Put(key[:], binary.LittleEndian.AppendUint32(nil, version))
	if err != nil {
		return err
	}

	p.cache.Set(postingID, version)
	return nil
}
