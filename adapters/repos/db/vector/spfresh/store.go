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
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// type LSMStore struct {
// 	store      *lsmkv.Store
// 	bucket     *lsmkv.Bucket
// 	vectorSize atomic.Int32
// 	locks      *common.ShardedRWLocks
// }

// func NewLSMStore(store *lsmkv.Store, bucketName string) (*LSMStore, error) {
// 	err := store.CreateOrLoadBucket(context.Background(), bucketName, lsmkv.WithStrategy(lsmkv.StrategySetCollection))
// 	if err != nil {
// 		return nil, errors.Wrapf(err, "failed to create or load bucket %s", bucketName)
// 	}

// 	return &LSMStore{
// 		store:  store,
// 		bucket: store.Bucket(bucketName),
// 		locks:  common.NewShardedRWLocks(512),
// 	}, nil
// }

// // Init is called by the index upon receiving the first vector and
// // determining the vector size.
// // Prior to calling this method, the store will assume the index is empty.
// func (l *LSMStore) Init(size int32) {
// 	l.vectorSize.Store(size)
// }

// func (l *LSMStore) Get(ctx context.Context, postingID uint64) (*Posting, error) {
// 	l.locks.RLock(postingID)
// 	defer l.locks.RUnlock(postingID)

// 	vectorSize := l.vectorSize.Load()
// 	if vectorSize == 0 {
// 		// the store is empty
// 		return nil, errors.WithStack(ErrPostingNotFound)
// 	}

// 	var buf [8]byte
// 	binary.LittleEndian.PutUint64(buf[:], postingID)
// 	list, err := l.bucket.SetList(buf[:])
// 	if err != nil {
// 		return nil, errors.Wrapf(err, "failed to get posting %d", postingID)
// 	}
// 	if len(list) == 0 {
// 		return nil, errors.WithStack(ErrPostingNotFound)
// 	}

// 	posting := Posting{
// 		vectorSize: int(vectorSize),
// 		data:       make([]byte, 0, len(list)*(8+1+int(vectorSize))),
// 	}

// 	for _, v := range list {
// 		posting.data = append(posting.data, v...)
// 	}

// 	return &posting, nil
// }

// func (l *LSMStore) MultiGet(ctx context.Context, postingIDs []uint64) ([]*Posting, error) {
// 	vectorSize := l.vectorSize.Load()
// 	if vectorSize == 0 {
// 		// the store is empty
// 		return nil, errors.WithStack(ErrPostingNotFound)
// 	}

// 	postings := make([]*Posting, 0, len(postingIDs))

// 	for _, id := range postingIDs {
// 		posting, err := l.Get(ctx, id)
// 		if err != nil {
// 			return nil, errors.Wrapf(err, "failed to get posting %d", id)
// 		}
// 		postings = append(postings, posting)
// 	}

// 	return postings, nil
// }

// func (l *LSMStore) Put(ctx context.Context, postingID uint64, posting *Posting) error {
// 	l.locks.Lock(postingID)
// 	defer l.locks.Unlock(postingID)

// 	if posting == nil {
// 		return errors.New("posting cannot be nil")
// 	}
// 	if posting.vectorSize != int(l.vectorSize.Load()) {
// 		return errors.Errorf("posting vector size %d does not match store vector size %d", posting.vectorSize, l.vectorSize.Load())
// 	}

// 	var buf [8]byte
// 	binary.LittleEndian.PutUint64(buf[:], postingID)

// 	set := make([][]byte, posting.Len())
// 	for i, v := range posting.Iter() {
// 		set[i] = v
// 	}

// 	list, err := l.bucket.SetList(buf[:])
// 	if err != nil {
// 		return errors.Wrapf(err, "failed to get posting %d", postingID)
// 	}
// 	for _, v := range list {
// 		err = l.bucket.SetDeleteSingle(buf[:], v)
// 		if err != nil {
// 			return errors.Wrapf(err, "failed to delete old vector from posting %d", postingID)
// 		}
// 	}

// 	return l.bucket.SetAdd(buf[:], set)
// }

// func (l *LSMStore) Merge(ctx context.Context, postingID uint64, vector Vector) error {
// 	l.locks.Lock(postingID)
// 	defer l.locks.Unlock(postingID)

// 	var buf [8]byte
// 	binary.LittleEndian.PutUint64(buf[:], postingID)

// 	return l.bucket.SetAdd(buf[:], [][]byte{vector})
// }

func bucketName(id string) string {
	return fmt.Sprintf("spfresh_postings_%s", id)
}

type LSMStore struct {
	m          sync.RWMutex
	store      map[uint64]*Posting
	vectorSize atomic.Int32
}

func NewLSMStore(store *lsmkv.Store, bucketName string) (*LSMStore, error) {

	return &LSMStore{
		store: make(map[uint64]*Posting),
	}, nil
}

// Init is called by the index upon receiving the first vector and
// determining the vector size.
// Prior to calling this method, the store will assume the index is empty.
func (l *LSMStore) Init(size int32) {
	l.vectorSize.Store(size)
}

func (l *LSMStore) Get(ctx context.Context, postingID uint64) (*Posting, error) {
	l.m.RLock()
	defer l.m.RUnlock()

	vectorSize := l.vectorSize.Load()
	if vectorSize == 0 {
		// the store is empty
		return nil, errors.WithStack(ErrPostingNotFound)
	}

	posting, ok := l.store[postingID]
	if !ok {
		return nil, errors.WithStack(ErrPostingNotFound)
	}

	return &Posting{
		vectorSize: posting.vectorSize,
		data:       append([]byte(nil), posting.data...),
	}, nil
}

func (l *LSMStore) MultiGet(ctx context.Context, postingIDs []uint64) ([]*Posting, error) {
	vectorSize := l.vectorSize.Load()
	if vectorSize == 0 {
		// the store is empty
		return nil, errors.WithStack(ErrPostingNotFound)
	}

	postings := make([]*Posting, 0, len(postingIDs))

	for _, id := range postingIDs {
		posting, err := l.Get(ctx, id)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get posting %d", id)
		}
		postings = append(postings, posting)
	}

	return postings, nil
}

func (l *LSMStore) Put(ctx context.Context, postingID uint64, posting *Posting) error {
	l.m.Lock()
	defer l.m.Unlock()

	if posting == nil {
		return errors.New("posting cannot be nil")
	}
	if posting.vectorSize != int(l.vectorSize.Load()) {
		return errors.Errorf("posting vector size %d does not match store vector size %d", posting.vectorSize, l.vectorSize.Load())
	}

	l.store[postingID] = &Posting{
		vectorSize: posting.vectorSize,
		data:       append([]byte(nil), posting.data...),
	}

	return nil
}

func (l *LSMStore) Merge(ctx context.Context, postingID uint64, vector Vector) error {
	l.m.Lock()
	defer l.m.Unlock()

	p, ok := l.store[postingID]
	if !ok {
		p = &Posting{
			vectorSize: int(l.vectorSize.Load()),
		}
		l.store[postingID] = p
	}

	p.data = append(p.data, vector...)

	return nil
}
