//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
)

func (s *Shard) DeleteObject(ctx context.Context, id strfmt.UUID) error {
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}

	idBytes, err := uuid.MustParse(id.String()).MarshalBinary()
	if err != nil {
		return err
	}

	var docID uint64
	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)
	existing, err := bucket.Get([]byte(idBytes))
	if err != nil {
		return fmt.Errorf("unexpected error on previous lookup: %w", err)
	}

	if existing == nil {
		// nothing to do
		return nil
	}

	// we need the doc ID so we can clean up inverted indices currently
	// pointing to this object
	docID, err = storobj.DocIDFromBinary(existing)
	if err != nil {
		return fmt.Errorf("get existing doc id from object binary: %w", err)
	}

	err = bucket.Delete(idBytes)
	if err != nil {
		return fmt.Errorf("delete object from bucket: %w", err)
	}

	err = s.cleanupInvertedIndexOnDelete(existing, docID)
	if err != nil {
		return fmt.Errorf("delete object from bucket: %w", err)
	}

	if err = s.queue.Delete(docID); err != nil {
		return fmt.Errorf("delete from vector index: %w", err)
	}

	if err = s.store.WriteWALs(); err != nil {
		return fmt.Errorf("flush all buffered WALs: %w", err)
	}

	if err = s.VectorIndex().Flush(); err != nil {
		return fmt.Errorf("flush all vector index buffered WALs: %w", err)
	}

	if err = s.ChangeObjectCountBy(-1); err != nil {
		return fmt.Errorf("subtract prop lengths: %w", err)
	}

	return nil
}

func (s *Shard) canDeleteOne(ctx context.Context, id strfmt.UUID) (bucket *lsmkv.Bucket, obj, uid []byte, docID uint64, err error) {
	if uid, err = parseBytesUUID(id); err != nil {
		return nil, nil, uid, 0, err
	}

	bucket = s.store.Bucket(helpers.ObjectsBucketLSM)
	existing, err := bucket.Get(uid)
	if err != nil {
		return nil, nil, uid, 0, fmt.Errorf("get previous object: %w", err)
	}

	if existing == nil {
		return bucket, nil, uid, 0, nil
	}

	// we need the doc ID so we can clean up inverted indices currently
	// pointing to this object
	docID, err = storobj.DocIDFromBinary(existing)
	if err != nil {
		return bucket, nil, uid, 0, fmt.Errorf("get existing doc id from object binary: %w", err)
	}
	return bucket, existing, uid, docID, nil
}

func (s *Shard) deleteOne(ctx context.Context, bucket *lsmkv.Bucket, obj, idBytes []byte, docID uint64) error {
	if obj == nil || bucket == nil {
		return nil
	}
	err := bucket.Delete(idBytes)
	if err != nil {
		return fmt.Errorf("delete object from bucket: %w", err)
	}

	err = s.cleanupInvertedIndexOnDelete(obj, docID)
	if err != nil {
		return fmt.Errorf("delete object from bucket: %w", err)
	}

	if err = s.queue.Delete(docID); err != nil {
		return fmt.Errorf("delete from vector index: %w", err)
	}

	if err = s.store.WriteWALs(); err != nil {
		return fmt.Errorf("flush all buffered WALs: %w", err)
	}

	if err = s.VectorIndex().Flush(); err != nil {
		return fmt.Errorf("flush all vector index buffered WALs: %w", err)
	}

	return nil
}

func (s *Shard) cleanupInvertedIndexOnDelete(previous []byte, docID uint64) error {
	previousObject, err := storobj.FromBinary(previous)
	if err != nil {
		return fmt.Errorf("unmarshal previous object: %w", err)
	}

	// TODO text_rbm_inverted_index null props cleanup?
	previousInvertProps, _, err := s.AnalyzeObject(previousObject)
	if err != nil {
		return fmt.Errorf("analyze previous object: %w", err)
	}

	if err = s.subtractPropLengths(previousInvertProps); err != nil {
		return fmt.Errorf("subtract prop lengths: %w", err)
	}

	err = s.deleteFromInvertedIndicesLSM(previousInvertProps, docID)
	if err != nil {
		return fmt.Errorf("put inverted indices props: %w", err)
	}

	if s.index.Config.TrackVectorDimensions {
		err = s.removeDimensionsLSM(len(previousObject.Vector), docID)
		if err != nil {
			return fmt.Errorf("track dimensions (delete): %w", err)
		}
	}

	return nil
}
