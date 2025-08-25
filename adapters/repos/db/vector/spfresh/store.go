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

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

type LSMStore struct {
	store      *lsmkv.Store
	bucket     *lsmkv.Bucket
	vectorSize int
}

func NewLSMStore(store *lsmkv.Store, vectorSize int32, bucketName string) (*LSMStore, error) {
	err := store.CreateOrLoadBucket(context.TODO(), bucketName, lsmkv.WithStrategy(lsmkv.StrategySetCollection))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create or load bucket %s", bucketName)
	}

	return &LSMStore{
		store:      store,
		bucket:     store.Bucket(bucketName),
		vectorSize: int(vectorSize),
	}, nil
}

func (l *LSMStore) Get(ctx context.Context, postingID uint64) (*Posting, error) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], postingID)
	list, err := l.bucket.SetList(buf[:])
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get posting %d", postingID)
	}
	if len(list) == 0 {
		return nil, ErrPostingNotFound
	}

	posting := Posting{
		vectorSize: int(l.vectorSize),
		data:       make([]byte, 0, len(list)*(8+1+l.vectorSize)),
	}

	for _, v := range list {
		posting.data = append(posting.data, v...)
	}

	return &posting, nil
}

func (l *LSMStore) MultiGet(ctx context.Context, postingIDs []uint64) ([]*Posting, error) {
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
	if posting == nil {
		return errors.New("posting cannot be nil")
	}
	if posting.vectorSize != l.vectorSize {
		return errors.Errorf("posting vector size %d does not match store vector size %d", posting.vectorSize, l.vectorSize)
	}

	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], postingID)

	set := make([][]byte, posting.Len())
	for i, v := range posting.Iter() {
		set[i] = v
	}

	// TODO: this is slow and not atomic, this might impact recall
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

func (l *LSMStore) Merge(ctx context.Context, postingID uint64, vector Vector) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], postingID)

	return l.bucket.SetAdd(buf[:], [][]byte{vector})
}
