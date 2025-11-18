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
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
)

type PostingStore struct {
	store              *lsmkv.Store
	bucket             *lsmkv.Bucket
	vectorSize         atomic.Int32
	locks              *common.ShardedRWLocks
	metrics            *Metrics
	compressed         bool
	replaceCounterLock *sync.RWMutex
	replaceCounters    map[uint64]uint32
}

func NewPostingStore(store *lsmkv.Store, metrics *Metrics, bucketName string, cfg StoreConfig) (*PostingStore, error) {
	err := store.CreateOrLoadBucket(context.Background(),
		bucketName,
		lsmkv.WithForceCompaction(true),
		lsmkv.WithStrategy(lsmkv.StrategySetCollection),
		lsmkv.WithAllocChecker(cfg.AllocChecker),
		lsmkv.WithMinMMapSize(cfg.MinMMapSize),
		lsmkv.WithMinWalThreshold(cfg.MaxReuseWalSize),
		lsmkv.WithLazySegmentLoading(cfg.LazyLoadSegments),
		lsmkv.WithWriteSegmentInfoIntoFileName(cfg.WriteSegmentInfoIntoFileName),
		lsmkv.WithWriteMetadata(cfg.WriteMetadataFilesEnabled),
	)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create or load bucket %s", bucketName)
	}

	return &PostingStore{
		store:              store,
		bucket:             store.Bucket(bucketName),
		locks:              common.NewDefaultShardedRWLocks(),
		metrics:            metrics,
		replaceCounters:    make(map[uint64]uint32),
		replaceCounterLock: &sync.RWMutex{},
	}, nil
}

// Init is called by the index upon receiving the first vector and
// determining the vector size.
// Prior to calling this method, the store will assume the index is empty.
func (p *PostingStore) Init(size int32, compressed bool) {
	p.vectorSize.Store(size)
	p.compressed = compressed
}

func (p *PostingStore) AddPostingId(ctx context.Context, postingID uint64) {
	p.replaceCounterLock.Lock()
	defer p.replaceCounterLock.Unlock()
	p.locks.Lock(postingID)
	defer p.locks.Unlock(postingID)
	if _, ok := p.replaceCounters[postingID]; ok {
		return
	}
	p.replaceCounters[postingID] = 0
}

func (p *PostingStore) Get(ctx context.Context, postingID uint64) (Posting, error) {
	start := time.Now()
	defer p.metrics.StoreGetDuration(start)

	vectorSize := p.vectorSize.Load()
	if vectorSize == 0 {
		// the store is empty
		return nil, errors.WithStack(ErrPostingNotFound)
	}

	var buf [12]byte
	binary.LittleEndian.PutUint64(buf[:], postingID)
	p.locks.RLock(postingID)
	p.replaceCounterLock.RLock()
	binary.LittleEndian.PutUint32(buf[8:], p.replaceCounters[postingID])
	list, err := p.bucket.SetList(buf[:])
	p.locks.RUnlock(postingID)
	p.replaceCounterLock.RUnlock()
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

	keys := make([][]byte, len(postingIDs))
	postings := make([]Posting, 0, len(postingIDs))

	p.replaceCounterLock.RLock()
	for i, postingID := range postingIDs {
		p.locks.RLock(postingID)
		keys[i] = make([]byte, 12)
		binary.LittleEndian.PutUint64(keys[i][:], postingID)
		binary.LittleEndian.PutUint32(keys[i][8:], p.replaceCounters[postingID])
		p.locks.RUnlock(postingID)
	}
	p.replaceCounterLock.RUnlock()

	lists, err := p.bucket.SetLists(keys)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to multi-get postings")
	}

	for _, list := range lists {
		if list == nil {
			postings = append(postings, nil)
			continue
		}

		posting := EncodedPosting{
			vectorSize: int(vectorSize),
			data:       make([]byte, 0, len(list)*(8+1+int(vectorSize))),
			compressed: p.compressed,
		}

		for _, v := range list {
			posting.data = append(posting.data, v...)
		}

		postings = append(postings, &posting)
	}
	return postings, nil
}

func (p *PostingStore) Put(ctx context.Context, postingID uint64, posting Posting) error {
	start := time.Now()
	defer p.metrics.StorePutDuration(start)

	if posting == nil {
		return errors.New("posting cannot be nil")
	}

	var buf [12]byte
	binary.LittleEndian.PutUint64(buf[:], postingID)

	p.replaceCounterLock.RLock()
	p.locks.Lock(postingID)
	defer p.locks.Unlock(postingID)

	binary.LittleEndian.PutUint32(buf[8:], p.replaceCounters[postingID])
	p.replaceCounterLock.RUnlock()

	set := make([][]byte, posting.Len())
	for i, v := range posting.Iter() {
		set[i] = v.Encode()
	}

	err := p.bucket.SetDeleteKey(buf[:])
	if err != nil {
		return errors.Wrapf(err, "failed to get posting %d", postingID)
	}
	p.replaceCounterLock.Lock()
	p.replaceCounters[postingID]++
	binary.LittleEndian.PutUint32(buf[8:], p.replaceCounters[postingID])
	p.replaceCounterLock.Unlock()
	err = p.bucket.SetAdd(buf[:], set)

	return err
}

func (p *PostingStore) Append(ctx context.Context, postingID uint64, vector Vector) error {
	start := time.Now()
	defer p.metrics.StoreAppendDuration(start)

	var buf [12]byte
	p.locks.Lock(postingID)
	p.replaceCounterLock.RLock()
	binary.LittleEndian.PutUint64(buf[:], postingID)
	binary.LittleEndian.PutUint32(buf[8:], p.replaceCounters[postingID])
	defer p.locks.Unlock(postingID)
	p.replaceCounterLock.RUnlock()

	return p.bucket.SetAdd(buf[:], [][]byte{vector.Encode()})
}

func postingBucketName(id string) string {
	return fmt.Sprintf("spfresh_postings_%s", id)
}
