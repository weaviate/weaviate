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

type LSMStore struct {
	store      *lsmkv.Store
	bucket     *lsmkv.Bucket
	vectorSize atomic.Int32
	locks      *common.ShardedRWLocks
	metrics    *Metrics
}

func NewLSMStore(store *lsmkv.Store, metrics *Metrics, bucketName string) (*LSMStore, error) {
	err := store.CreateOrLoadBucket(
		context.Background(),
		bucketName,
		lsmkv.WithStrategy(lsmkv.StrategySetCollection),
		lsmkv.WithMinMMapSize(8192),
		lsmkv.WithWalThreshold(4096),
	)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create or load bucket %s", bucketName)
	}

	return &LSMStore{
		store:   store,
		bucket:  store.Bucket(bucketName),
		locks:   common.NewDefaultShardedRWLocks(),
		metrics: metrics,
	}, nil
}

// Init is called by the index upon receiving the first vector and
// determining the vector size.
// Prior to calling this method, the store will assume the index is empty.
func (l *LSMStore) Init(size int32) {
	l.vectorSize.Store(size)
}

func (l *LSMStore) Get(ctx context.Context, postingID uint64) (Posting, error) {
	start := time.Now()
	defer l.metrics.StoreGetDuration(start)

	vectorSize := l.vectorSize.Load()
	if vectorSize == 0 {
		// the store is empty
		return nil, errors.WithStack(ErrPostingNotFound)
	}

	l.locks.RLock(postingID)
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], postingID)
	list, err := l.bucket.SetList(buf[:])
	l.locks.RUnlock(postingID)

	if err != nil {
		return nil, errors.Wrapf(err, "failed to get posting %d", postingID)
	}

	posting := CompressedPosting{
		vectorSize: int(vectorSize),
		data:       make([]byte, 0, len(list)*(8+1+int(vectorSize))),
	}

	for _, v := range list {
		posting.data = append(posting.data, v...)
	}

	return &posting, nil
}

func (l *LSMStore) MultiGet(ctx context.Context, postingIDs []uint64) ([]Posting, error) {
	vectorSize := l.vectorSize.Load()
	if vectorSize == 0 {
		// the store is empty
		return nil, errors.WithStack(ErrPostingNotFound)
	}

	postings := make([]Posting, 0, len(postingIDs))

	for _, id := range postingIDs {
		posting, err := l.Get(ctx, id)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get posting %d", id)
		}
		postings = append(postings, posting)
	}

	return postings, nil
}

func (l *LSMStore) Put(ctx context.Context, postingID uint64, posting Posting) error {
	start := time.Now()
	defer l.metrics.StorePutDuration(start)

	if posting == nil {
		return errors.New("posting cannot be nil")
	}

	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], postingID)

	set := make([][]byte, posting.Len())
	for i, v := range posting.Iter() {
		set[i] = v.(CompressedVector)
	}

	l.locks.Lock(postingID)
	defer l.locks.Unlock(postingID)

	list, err := l.bucket.SetList(buf[:])
	if err != nil {
		return errors.Wrapf(err, "failed to get posting %d", postingID)
	}
	for _, v := range list {
		err = l.bucket.SetDeleteSingle(buf[:], v)
		if err != nil {
			return errors.Wrapf(err, "failed to delete old vector from posting %d", postingID)
		}
	}

	return l.bucket.SetAdd(buf[:], set)
}

func (l *LSMStore) Append(ctx context.Context, postingID uint64, vector Vector) error {
	start := time.Now()
	defer l.metrics.StoreAppendDuration(start)

	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], postingID)

	l.locks.Lock(postingID)
	defer l.locks.Unlock(postingID)

	return l.bucket.SetAdd(buf[:], [][]byte{vector.(CompressedVector)})
}

func (l *LSMStore) Flush() error {
	return l.bucket.FlushMemtable()
}

func bucketName(id string) string {
	return fmt.Sprintf("spfresh_postings_%s", id)
}
