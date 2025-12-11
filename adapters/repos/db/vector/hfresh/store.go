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

package hfresh

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
)

type PostingStore struct {
	store    *lsmkv.Store
	bucket   *lsmkv.Bucket
	locks    *common.ShardedRWLocks
	metrics  *Metrics
	versions *PostingVersions
}

func NewPostingStore(store *lsmkv.Store, metadataBucket *lsmkv.Bucket, metrics *Metrics, id string, cfg StoreConfig) (*PostingStore, error) {
	bName := postingsBucketName(id)
	err := store.CreateOrLoadBucket(context.Background(),
		bName,
		cfg.MakeBucketOptions(lsmkv.StrategySetCollection, lsmkv.WithForceCompaction(true))...,
	)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create or load bucket %s", bName)
	}

	versions, err := NewPostingVersions(metadataBucket, metrics)
	if err != nil {
		return nil, errors.Wrap(err, "create posting versions store")
	}

	return &PostingStore{
		store:    store,
		bucket:   store.Bucket(bName),
		locks:    common.NewDefaultShardedRWLocks(),
		metrics:  metrics,
		versions: versions,
	}, nil
}

func NewPostingStoreTest(store *lsmkv.Store, metrics *Metrics, id string, cfg StoreConfig) (*PostingStore, error) {
	bucket, err := NewSharedBucket(store, id, cfg)
	if err != nil {
		return nil, err
	}

	return NewPostingStore(store, bucket, metrics, id, cfg)
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

	p.locks.RLock(postingID)
	key, err := p.getKeyBytes(ctx, postingID)
	if err != nil && !errors.Is(err, ErrPostingNotFound) {
		p.locks.RUnlock(postingID)
		return err
	} else if err != nil && errors.Is(err, ErrPostingNotFound) {
		p.versions.Set(ctx, postingID, 0)
		key, err = p.getKeyBytes(ctx, postingID)
		if err != nil {
			p.locks.RUnlock(postingID)
			return err
		}
	}

	p.locks.RUnlock(postingID)

	set := make([][]byte, len(posting))
	for i, v := range posting {
		set[i] = v
	}

	p.locks.Lock(postingID)
	defer p.locks.Unlock(postingID)
	list, err := p.bucket.SetList(key)
	if err != nil {
		return errors.Wrapf(err, "failed to get posting %d", postingID)
	}
	for _, v := range list {
		err = p.bucket.SetDeleteSingle(key, v)
		if err != nil {
			return errors.Wrapf(err, "failed to delete old vector from posting %d", postingID)
		}
	}

	version, err := p.versions.Inc(ctx, postingID, 1)
	if err != nil {
		return errors.Wrapf(err, "increment posting version for id %d", postingID)
	}

	var buf [12]byte
	binary.LittleEndian.PutUint64(buf[:], postingID)
	binary.LittleEndian.PutUint32(buf[8:], version)
	return p.bucket.SetAdd(buf[:], set)
}

func (p *PostingStore) Append(ctx context.Context, postingID uint64, vector Vector) error {
	start := time.Now()
	defer p.metrics.StoreAppendDuration(start)

	key, err := p.getKeyBytes(ctx, postingID)
	if err != nil {
		return err
	}

	p.locks.Lock(postingID)
	defer p.locks.Unlock(postingID)

	return p.bucket.SetAdd(key, [][]byte{vector})
}

func postingsBucketName(id string) string {
	return fmt.Sprintf("hfresh_postings_%s", id)
}
