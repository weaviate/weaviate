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
	"context"
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
)

type PostingStore struct {
	store      *lsmkv.Store
	bucket     *lsmkv.Bucket
	vectorSize atomic.Int32
	locks      *common.ShardedRWLocks
	metrics    *Metrics
	compressed bool
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

// Init is called by the index upon receiving the first vector and
// determining the vector size.
// Prior to calling this method, the store will assume the index is empty.
func (p *PostingStore) Init(size int32, compressed bool) {
	p.vectorSize.Store(size)
	p.compressed = compressed
}

func (p *PostingStore) Get(ctx context.Context, postingID uint64) (Posting, error) {
	start := time.Now()
	defer p.metrics.StoreGetDuration(start)

	vectorSize := p.vectorSize.Load()
	if vectorSize == 0 {
		// the store is empty
		return nil, errors.WithStack(ErrPostingNotFound)
	}

	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], postingID)
	p.locks.RLock(postingID)
	list, err := p.bucket.SetList(buf[:])
	p.locks.RUnlock(postingID)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get posting %d", postingID)
	}

	posting := EncodedPosting{
		vectorSize: int(vectorSize),
		data:       make([]byte, 0, len(list)*(8+1+int(vectorSize))),
		compressed: p.compressed,
	}

	for _, v := range list {
		posting.data = append(posting.data, v...)
	}

	return &posting, nil
}

func (p *PostingStore) MultiGet(ctx context.Context, postingIDs []uint64) ([]Posting, error) {
	vectorSize := p.vectorSize.Load()
	if vectorSize == 0 {
		// the store is empty
		return nil, errors.WithStack(ErrPostingNotFound)
	}

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

	set := make([][]byte, posting.Len())
	for i, v := range posting.Iter() {
		set[i] = v.Encode()
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

	return p.bucket.SetAdd(buf[:], [][]byte{vector.Encode()})
}

func postingBucketName(id string) string {
	return fmt.Sprintf("spfresh_postings_%s", id)
}
