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
	"bytes"
	"context"
	"encoding/binary"
	"sync"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

type LSMStore struct {
	store   *lsmkv.Store
	bucket  *lsmkv.Bucket
	encoder *PostingEncoder
}

func NewLSMStore(store *lsmkv.Store, dims int) *LSMStore {
	return &LSMStore{
		store:   store,
		bucket:  store.Bucket(bucketName),
		encoder: NewPostingEncoder(dims),
	}
}

func (l *LSMStore) Get(ctx context.Context, postingID uint64) (Posting, error) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], postingID)
	list, err := l.bucket.SetList(buf[:])
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get posting %d", postingID)
	}

	posting := make(Posting, 0, len(list))

	for _, val := range list {
		vector, err := l.encoder.DecodeVector(val)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to decode posting %d", postingID)
		}
		posting = append(posting, *vector)
	}

	return posting, nil
}

func (l *LSMStore) Put(ctx context.Context, postingID uint64, posting Posting) error {
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

	encoded := make([][]byte, len(posting))
	for i := range posting {
		encoded[i] = l.encoder.EncodeVector(&posting[i])
	}
	return l.bucket.SetAdd(buf[:], encoded)
}

func (l *LSMStore) Merge(ctx context.Context, postingID uint64, vector Vector) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], postingID)

	return l.bucket.SetAdd(buf[:], [][]byte{l.encoder.EncodeVector(&vector)})
}

// PostingEncoder encodes and decodes postings
type PostingEncoder struct {
	Dimensions int // Number of dimensions of the vectors to encode/decode

	bufPool sync.Pool // Buffer pool for reusing byte slices
}

func NewPostingEncoder(dimensions int) *PostingEncoder {
	return &PostingEncoder{
		Dimensions: dimensions,
		bufPool: sync.Pool{
			New: func() any {
				return new(bytes.Buffer)
			},
		},
	}
}

func (e *PostingEncoder) EncodeVector(vector *Vector) []byte {
	buf := make([]byte, 8+1+e.Dimensions)
	binary.LittleEndian.PutUint64(buf, vector.ID)
	buf[8] = byte(vector.Version)
	copy(buf[9:], vector.Data)
	return buf
}

func (e *PostingEncoder) DecodeVector(encoded []byte) (*Vector, error) {
	if len(encoded) < 8+1+e.Dimensions {
		return nil, errors.New("encoded vector is too short")
	}

	var id uint64
	var version VectorVersion
	err := binary.Read(bytes.NewReader(encoded[:8]), binary.LittleEndian, &id)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read vector ID")
	}
	err = binary.Read(bytes.NewReader(encoded[8:9]), binary.LittleEndian, &version)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read vector version")
	}

	data := encoded[9 : 9+e.Dimensions]
	return &Vector{
		ID:      id,
		Version: version,
		Data:    data,
	}, nil
}
