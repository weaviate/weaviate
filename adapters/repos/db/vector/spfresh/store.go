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
	store           *lsmkv.Store
	bucket          *lsmkv.Bucket
	vectorSize      atomic.Int32
	locks           *common.ShardedRWLocks
	metrics         *Metrics
	compressed      bool
	postingVersions *PostingSizes
}

func NewPostingStore(store *lsmkv.Store, metrics *Metrics, bucketName string, postingVersions *PostingSizes, cfg StoreConfig) (*PostingStore, error) {
	err := store.CreateOrLoadBucket(context.Background(),
		bucketName,
		cfg.MakeBucketOptions(lsmkv.StrategySetCollection,
			lsmkv.WithForceCompaction(true),
			lsmkv.WithMemtableThreshold(uint64(50*1024*1024)),
			lsmkv.WithWalThreshold(uint64(5*1024*1024*1024)),
			lsmkv.WithPostingVersionsBucket(postingVersions.store.bucket),
		)...,
	)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create or load bucket %s", bucketName)
	}

	return &PostingStore{
		store:           store,
		bucket:          store.Bucket(bucketName),
		locks:           common.NewDefaultShardedRWLocks(),
		metrics:         metrics,
		postingVersions: postingVersions,
	}, nil
}

func NewPostingStoreTest(store *lsmkv.Store, metrics *Metrics, bucketName string, cfg StoreConfig) (*PostingStore, error) {
	postingVersions, err := NewPostingSizes(store, metrics, postingVersionName("dummy"), cfg)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create or load posting sizes for bucket %s", bucketName)
	}
	return NewPostingStore(store, metrics, bucketName, postingVersions, cfg)
}

func (p *PostingStore) getVersion(ctx context.Context, postingID uint64) (uint32, error) {
	p.locks.RLock(postingID)
	val, err := p.postingVersions.Get(ctx, postingID)
	p.locks.RUnlock(postingID)
	if err != nil {
		if errors.Is(err, ErrPostingNotFound) {
			p.locks.Lock(postingID)
			defer p.locks.Unlock(postingID)
			err = p.postingVersions.Set(ctx, postingID, 0)
		}
		return 0, err
	}
	return val, nil
}

// determining the vector size.
// Prior to calling this method, the store will assume the index is empty.
func (p *PostingStore) Init(size int32, compressed bool) {
	p.vectorSize.Store(size)
	p.compressed = compressed
}

func (p *PostingStore) GetInner(ctx context.Context, postingID uint64, view lsmkv.BucketConsistentView) (Posting, error) {
	start := time.Now()
	defer p.metrics.StoreGetDuration(start)

	vectorSize := p.vectorSize.Load()
	if vectorSize == 0 {
		// the store is empty
		return nil, errors.WithStack(ErrPostingNotFound)
	}

	var buf [12]byte
	version, err := p.getVersion(ctx, postingID)
	if err != nil {
		return nil, err
	}
	binary.LittleEndian.PutUint64(buf[:], postingID)
	binary.LittleEndian.PutUint32(buf[8:], version)

	p.locks.RLock(postingID)
	defer p.locks.RUnlock(postingID)
	list, err := p.bucket.SetList(buf[:])
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

func (p *PostingStore) Get(ctx context.Context, postingID uint64) (Posting, error) {
	start := time.Now()
	defer p.metrics.StoreGetDuration(start)

	vectorSize := p.vectorSize.Load()
	if vectorSize == 0 {
		// the store is empty
		return nil, errors.WithStack(ErrPostingNotFound)
	}

	var buf [12]byte
	version, err := p.getVersion(ctx, postingID)
	if err != nil {
		return nil, err
	}
	binary.LittleEndian.PutUint64(buf[:], postingID)
	binary.LittleEndian.PutUint32(buf[8:], version)

	return p.GetInner(ctx, postingID, p.bucket.GetConsistentViewOfSegmentsForKeys([][]byte{buf[:]}))
}

func (p *PostingStore) MultiGet(ctx context.Context, postingIDs []uint64) ([]Posting, error) {
	vectorSize := p.vectorSize.Load()
	if vectorSize == 0 {
		// the store is empty
		return nil, errors.WithStack(ErrPostingNotFound)
	}

	keys := make([][]byte, len(postingIDs))
	postings := make([]Posting, 0, len(postingIDs))

	for i, postingID := range postingIDs {
		version, err := p.getVersion(ctx, postingID)
		if err != nil {
			return nil, err
		}
		keys[i] = make([]byte, 12)
		binary.LittleEndian.PutUint64(keys[i][:], postingID)
		binary.LittleEndian.PutUint32(keys[i][8:], version)
	}
	view := p.bucket.GetConsistentViewOfSegmentsForKeys(keys)

	for _, id := range postingIDs {
		posting, err := p.GetInner(ctx, id, view)
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

	var buf [12]byte
	var newBuf [12]byte
	version, err := p.getVersion(ctx, postingID)
	if err != nil {
		return err
	}

	p.locks.Lock(postingID)
	defer p.locks.Unlock(postingID)

	binary.LittleEndian.PutUint64(buf[:], postingID)
	binary.LittleEndian.PutUint32(buf[8:], version)

	set := make([][]byte, posting.Len())
	for i, v := range posting.Iter() {
		set[i] = v.Encode()
	}

	err = p.bucket.SetDeleteKey(buf[:])
	if err != nil {
		return errors.Wrapf(err, "failed to get posting %d", postingID)
	}
	version += 1
	err = p.postingVersions.Set(ctx, postingID, uint32(version))
	if err != nil {
		return errors.Wrapf(err, "failed to set version for posting %d", postingID)
	}
	binary.LittleEndian.PutUint64(newBuf[:], postingID)
	binary.LittleEndian.PutUint32(newBuf[8:], version)
	err = p.bucket.SetAdd(newBuf[:], set)
	return err
}

func (p *PostingStore) Append(ctx context.Context, postingID uint64, vector Vector) error {
	start := time.Now()
	defer p.metrics.StoreAppendDuration(start)

	version, err := p.getVersion(ctx, postingID)
	if err != nil {
		return err
	}
	var buf [12]byte

	p.locks.Lock(postingID)
	defer p.locks.Unlock(postingID)
	binary.LittleEndian.PutUint64(buf[:], postingID)
	binary.LittleEndian.PutUint32(buf[8:], version)

	return p.bucket.SetAdd(buf[:], [][]byte{vector.Encode()})
}

func postingBucketName(id string) string {
	return fmt.Sprintf("spfresh_postings_%s", id)
}

func postingVersionName(id string) string {
	return fmt.Sprintf("spfresh_posting_version_%s", id)
}
