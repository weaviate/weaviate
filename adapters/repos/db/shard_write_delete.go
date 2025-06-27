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
	"encoding/binary"
	"fmt"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/storobj"
)

func (s *Shard) DeleteObject(ctx context.Context, id strfmt.UUID, deletionTime time.Time) error {
	if err := s.isReadOnly(); err != nil {
		return err
	}

	idBytes, err := uuid.MustParse(id.String()).MarshalBinary()
	if err != nil {
		return err
	}

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
	docID, updateTime, err := storobj.DocIDAndTimeFromBinary(existing)
	if err != nil {
		return fmt.Errorf("get existing doc id from object binary: %w", err)
	}

	if deletionTime.IsZero() {
		err = bucket.Delete(idBytes)
	} else {
		err = bucket.DeleteWith(idBytes, deletionTime)
	}
	if err != nil {
		return fmt.Errorf("delete object from bucket: %w", err)
	}

	err = s.cleanupInvertedIndexOnDelete(existing, docID)
	if err != nil {
		return fmt.Errorf("delete object from bucket: %w", err)
	}

	if err = s.store.WriteWALs(); err != nil {
		return fmt.Errorf("flush all buffered WALs: %w", err)
	}

	err = s.ForEachVectorQueue(func(targetVector string, queue *VectorIndexQueue) error {
		if err = queue.Delete(docID); err != nil {
			return fmt.Errorf("delete from vector index of vector %q: %w", targetVector, err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	err = s.ForEachVectorQueue(func(targetVector string, queue *VectorIndexQueue) error {
		if err = queue.Flush(); err != nil {
			return fmt.Errorf("flush all vector index buffered WALs of vector %q: %w", targetVector, err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	if err = s.mayDeleteObjectHashTree(idBytes, updateTime); err != nil {
		return fmt.Errorf("object deletion in hashtree: %w", err)
	}

	return nil
}

func (s *Shard) cleanupInvertedIndexOnDelete(previous []byte, docID uint64) error {
	previousObject, err := storobj.FromBinary(previous)
	if err != nil {
		return fmt.Errorf("unmarshal previous object: %w", err)
	}

	previousProps, previousNilProps, err := s.AnalyzeObject(previousObject)
	if err != nil {
		return fmt.Errorf("analyze previous object: %w", err)
	}

	if err = s.subtractPropLengths(previousProps); err != nil {
		return fmt.Errorf("subtract prop lengths: %w", err)
	}

	err = s.deleteFromInvertedIndicesLSM(previousProps, previousNilProps, docID)
	if err != nil {
		return fmt.Errorf("put inverted indices props: %w", err)
	}

	if s.index.Config.TrackVectorDimensions {
		err = previousObject.IterateThroughVectorDimensions(func(targetVector string, dims int) error {
			if err = s.removeDimensionsLSM(dims, docID, targetVector); err != nil {
				return fmt.Errorf("remove dimension tracking for vector %q: %w", targetVector, err)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Shard) mayDeleteObjectHashTree(uuidBytes []byte, updateTime int64) error {
	s.asyncReplicationRWMux.RLock()
	defer s.asyncReplicationRWMux.RUnlock()

	if s.hashtree == nil {
		return nil
	}

	return s.deleteObjectHashTree(uuidBytes, updateTime)
}

func (s *Shard) deleteObjectHashTree(uuidBytes []byte, updateTime int64) error {
	if len(uuidBytes) != 16 {
		return fmt.Errorf("invalid object uuid")
	}

	if updateTime < 1 {
		return fmt.Errorf("invalid object update time")
	}

	leaf := s.hashtreeLeafFor(uuidBytes)

	var objectDigest [16 + 8]byte

	copy(objectDigest[:], uuidBytes)
	binary.BigEndian.PutUint64(objectDigest[16:], uint64(updateTime))

	// object deletion is treated as non-existent,
	// that because deletion time or tombstone may not be available

	s.hashtree.AggregateLeafWith(leaf, objectDigest[:])

	return nil
}
