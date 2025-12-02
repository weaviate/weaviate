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

	postingVersions *PostingSizes
}

func NewPostingStore(store *lsmkv.Store, metrics *Metrics, bucketName string, cfg StoreConfig) (*PostingStore, error) {
	postingVersions, err := NewPostingSizes(store, metrics, postingVersionName(bucketName), cfg)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create or load posting versions store %s", postingVersionName(bucketName))
	}
	err = store.CreateOrLoadBucket(context.Background(),
		postingBucketName(bucketName),
		cfg.MakeBucketOptions(lsmkv.StrategySetCollection, lsmkv.WithForceCompaction(true), lsmkv.WithShouldIgnoreKeyFunction(
			func(key []byte, ctx context.Context) (bool, error) {
				if postingVersions == nil {
					return true, nil
				}
				v, err := postingVersions.Get(ctx, binary.LittleEndian.Uint64(key[:8]))
				if err != nil {
					// assume valid if we cannot check
					return true, nil
				}
				return v != binary.LittleEndian.Uint32(key[8:12]), nil
			},
		))...,
	)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create or load bucket %s", postingBucketName(bucketName))
	}

	return &PostingStore{
		store:           store,
		bucket:          store.Bucket(postingBucketName(bucketName)),
		locks:           common.NewDefaultShardedRWLocks(),
		metrics:         metrics,
		postingVersions: postingVersions,
	}, nil
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

// Init is called by the index upon receiving the first vector and
// determining the vector size.
// Prior to calling this method, the store will assume the index is empty.
func (p *PostingStore) Init(size int32) {
	p.vectorSize.Store(size)
}

func (p *PostingStore) Get(ctx context.Context, postingID uint64) (*Posting, error) {
	return p.GetInner(ctx, postingID, p.bucket.GetConsistentViewOfSegmentsForKeys([][]byte{
		func() []byte {
			var buf [12]byte
			binary.LittleEndian.PutUint64(buf[:], postingID)
			version, err := p.getVersion(ctx, postingID)
			if err != nil {
				return nil
			}
			binary.LittleEndian.PutUint32(buf[8:12], version)
			return buf[:]
		}(),
	}))
}

func (p *PostingStore) GetInner(ctx context.Context, postingID uint64, view lsmkv.BucketConsistentView) (*Posting, error) {
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
	version, err := p.getVersion(ctx, postingID)
	if err != nil {
		p.locks.RUnlock(postingID)
		return nil, errors.Wrapf(err, "failed to get version for posting %d", postingID)
	}
	binary.LittleEndian.PutUint32(buf[8:12], version)
	list, err := p.bucket.SetListFromConsistentView(view, buf[:])
	p.locks.RUnlock(postingID)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get posting %d", postingID)
	}

	posting := Posting{
		vectorSize: int(vectorSize),
		vectors:    make([]Vector, len(list)),
	}

	for i, v := range list {
		posting.vectors[i] = Vector(v)
	}

	return &posting, nil
}

func (p *PostingStore) MultiGet(ctx context.Context, postingIDs []uint64) ([]*Posting, error) {
	vectorSize := p.vectorSize.Load()
	if vectorSize == 0 {
		// the store is empty
		return nil, errors.WithStack(ErrPostingNotFound)
	}

	postings := make([]*Posting, 0, len(postingIDs))
	keys := make([][]byte, len(postingIDs))

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

func (p *PostingStore) Put(ctx context.Context, postingID uint64, posting *Posting) error {
	start := time.Now()
	defer p.metrics.StorePutDuration(start)

	if posting == nil {
		return errors.New("posting cannot be nil")
	}

	var buf [12]byte
	binary.LittleEndian.PutUint64(buf[:8], postingID)
	version, err := p.getVersion(ctx, postingID)
	if err != nil {
		return errors.Wrapf(err, "failed to get version for posting %d", postingID)
	}
	binary.LittleEndian.PutUint32(buf[8:12], version)

	p.locks.Lock(postingID)
	defer p.locks.Unlock(postingID)

	err = p.bucket.SetDeleteKey(buf[:])
	if err != nil {
		return errors.Wrapf(err, "failed to clear existing posting %d", postingID)
	}

	version, err = p.postingVersions.Inc(ctx, postingID, 1)
	if err != nil {
		return errors.Wrapf(err, "failed to get posting %d", postingID)
	}

	set := make([][]byte, len(posting.vectors))
	for i, v := range posting.vectors {
		set[i] = []byte(v)
	}

	newKey := make([]byte, 12)
	binary.LittleEndian.PutUint64(newKey[:8], postingID)
	binary.LittleEndian.PutUint32(newKey[8:12], version)
	return p.bucket.SetAdd(newKey, set)
}

func (p *PostingStore) Append(ctx context.Context, postingID uint64, vector Vector) error {
	start := time.Now()
	defer p.metrics.StoreAppendDuration(start)

	var buf [12]byte
	binary.LittleEndian.PutUint64(buf[:], postingID)
	version, err := p.getVersion(ctx, postingID)
	if err != nil {
		return errors.Wrapf(err, "failed to get version for posting %d", postingID)
	}
	binary.LittleEndian.PutUint32(buf[8:12], version)

	p.locks.Lock(postingID)
	defer p.locks.Unlock(postingID)

	return p.bucket.SetAdd(buf[:], [][]byte{vector})
}

func postingBucketName(id string) string {
	return fmt.Sprintf("hfresh_postings_%s", id)
}

func postingVersionName(id string) string {
	return fmt.Sprintf("hfresh_posting_versions_%s", id)
}
