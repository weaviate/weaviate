//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package tachiomfresh

import (
	"context"
	"encoding/binary"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hfresh"
)

// tokenCountStore is the bucket that holds the token count of every document
// the index has placed tokens of. An insert writes the count before the
// first token. Restore counts each document's live entries
// and takes the document as indexed only when the two agree, so a document
// that a crash left partly placed is not in the doc set, and the shard sends
// it again. A count without the tokens, or tokens without the count, both
// read as partial. The key is the doc id (8 bytes big-endian, as the staging
// bucket's); the value is the count minus one (u16, little-endian): a
// document has 1 to 65,536 tokens.
type tokenCountStore struct {
	store  *lsmkv.Store
	bucket string
}

func newTokenCountStore(store *lsmkv.Store, id string, cfg hfresh.StoreConfig) (*tokenCountStore, error) {
	name := helpers.TachiomFreshTokenCountsBucketName(id)
	err := store.CreateOrLoadBucket(context.Background(), name,
		cfg.MakeBucketOptions(lsmkv.StrategyReplace)...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create or load bucket %s", name)
	}
	return &tokenCountStore{store: store, bucket: name}, nil
}

// acquire pins the bucket for one operation, as the posting store's.
func (s *tokenCountStore) acquire() (*lsmkv.Bucket, func(), error) {
	bucket, release := s.store.AcquireBucketForRead(s.bucket)
	if bucket == nil {
		release()
		return nil, nil, errors.Wrapf(lsmkv.ErrBucketNotFound, "bucket %s", s.bucket)
	}
	return bucket, release, nil
}

func (s *tokenCountStore) put(docID uint64, tokens int) error {
	if tokens < 1 || tokens > 1<<16 {
		return errors.Errorf("doc %d: a token count of %d does not fit", docID, tokens)
	}
	bucket, release, err := s.acquire()
	if err != nil {
		return err
	}
	defer release()
	val := make([]byte, 2)
	binary.LittleEndian.PutUint16(val, uint16(tokens-1))
	return bucket.Put(stagingKey(docID), val)
}

// iterate calls fn with every stored count, doc ids ascending, until fn
// returns an error.
func (s *tokenCountStore) iterate(fn func(docID uint64, tokens int) error) error {
	bucket, release, err := s.acquire()
	if err != nil {
		return err
	}
	defer release()
	c := bucket.Cursor()
	defer c.Close()
	for key, val := c.First(); key != nil; key, val = c.Next() {
		if len(key) != 8 || len(val) != 2 {
			return errors.Errorf("token count entry of %d and %d bytes", len(key), len(val))
		}
		if err := fn(binary.BigEndian.Uint64(key), int(binary.LittleEndian.Uint16(val))+1); err != nil {
			return err
		}
	}
	return nil
}
