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
	store  *lsmkv.Store
	bucket *lsmkv.Bucket
	dims   int32
}

func NewLSMStore(store *lsmkv.Store, dims int32, bucketName string) *LSMStore {
	return &LSMStore{
		store:  store,
		bucket: store.Bucket(bucketName),
		dims:   dims,
	}
}

func (l *LSMStore) Get(ctx context.Context, postingID uint64) (*Posting, error) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], postingID)
	list, err := l.bucket.SetList(buf[:])
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get posting %d", postingID)
	}

	posting := Posting{
		dims: l.dims,
		data: make([]byte, 0, int32(len(list))*(8+1+l.dims*4)),
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
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], postingID)

	// TODO: this is a very slow way of replacing the entire posting.
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

	set := make([][]byte, posting.Len())

	for i, v := range posting.Iter() {
		set[i] = v
	}

	return l.bucket.SetAdd(buf[:], set)
}

func (l *LSMStore) Merge(ctx context.Context, postingID uint64, vector Vector) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], postingID)

	return l.bucket.SetAdd(buf[:], [][]byte{vector})
}
