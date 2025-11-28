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

func NewBucket(store *lsmkv.Store, indexID string, cfg StoreConfig) (*lsmkv.Bucket, error) {
	bName := bucketName(indexID)
	err := store.CreateOrLoadBucket(context.Background(),
		bName,
		cfg.MakeBucketOptions(lsmkv.StrategySetCollection, lsmkv.WithForceCompaction(true))...,
	)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create or load bucket %s", bucketName)
	}

	return store.Bucket(bName), nil
}

type PostingStore struct {
	store   *lsmkv.Store
	bucket  *lsmkv.Bucket
	locks   *common.ShardedRWLocks
	metrics *Metrics
}

func NewPostingStore(store *lsmkv.Store, metrics *Metrics, bucketName string, cfg StoreConfig) (*PostingStore, error) {
	err := store.CreateOrLoadBucket(context.Background(),
		bucketName,
		cfg.MakeBucketOptions(lsmkv.StrategySetCollection, lsmkv.WithForceCompaction(true))...,
	)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create or load bucket %s", bucketName)
	}

	return &PostingStore{
		store:   store,
		bucket:  store.Bucket(bucketName),
		locks:   common.NewDefaultShardedRWLocks(),
		metrics: metrics,
	}, nil
}

func (p *PostingStore) Get(ctx context.Context, postingID uint64) (Posting, error) {
	start := time.Now()
	defer p.metrics.StoreGetDuration(start)

	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], postingID)
	p.locks.RLock(postingID)
	list, err := p.bucket.SetList(buf[:])
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

	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], postingID)

	set := make([][]byte, len(posting))
	for i, v := range posting {
		set[i] = v
	}

	p.locks.Lock(postingID)
	defer p.locks.Unlock(postingID)

	list, err := p.bucket.SetList(buf[:])
	if err != nil {
		return errors.Wrapf(err, "failed to get posting %d", postingID)
	}
	for _, v := range list {
		err = p.bucket.SetDeleteSingle(buf[:], v)
		if err != nil {
			return errors.Wrapf(err, "failed to delete old vector from posting %d", postingID)
		}
	}

	return p.bucket.SetAdd(buf[:], set)
}

func (p *PostingStore) Append(ctx context.Context, postingID uint64, vector Vector) error {
	start := time.Now()
	defer p.metrics.StoreAppendDuration(start)

	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], postingID)

	p.locks.Lock(postingID)
	defer p.locks.Unlock(postingID)

	return p.bucket.SetAdd(buf[:], [][]byte{vector})
}

func bucketName(id string) string {
	return fmt.Sprintf("hfresh_%s", id)
}
