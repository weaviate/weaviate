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

package db

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/storobj"
)

func (s *Shard) DeleteObject(ctx context.Context, id strfmt.UUID, deletionTime time.Time) error {
	_, err := s.deleteObject(ctx, id, deletionTime, fromChangeLogReplay(ctx))
	return err
}

// deleteObject tombstones the object (reporting whether it did); with skipIfLocalNewer it keeps a live copy at least as new as deletionTime, since lsmkv does not timestamp-arbitrate and an older repair tombstone would otherwise clobber a newer write.
func (s *Shard) deleteObject(ctx context.Context, id strfmt.UUID, deletionTime time.Time,
	skipIfLocalNewer bool,
) (bool, error) {
	if err := s.isReadOnly(); err != nil {
		return false, err
	}

	// Wait outside the RLock; see shard_write_put.go.
	if err := s.waitForMinimalHashTreeInitialization(ctx); err != nil {
		return false, err
	}

	s.asyncReplicationRWMux.RLock()
	defer s.asyncReplicationRWMux.RUnlock()

	idBytes, err := uuid.MustParse(id.String()).MarshalBinary()
	if err != nil {
		return false, err
	}

	bucket, release, err := s.objectsBucket()
	if err != nil {
		return false, err
	}
	defer release()

	// see comment in shard_write_put.go::putObjectLSM
	lock := &s.docIdLock[s.uuidToIdLockPoolId(idBytes)]

	lock.Lock()
	defer lock.Unlock()

	existing, err := bucket.Get([]byte(idBytes))
	if err != nil {
		return false, fmt.Errorf("unexpected error on previous lookup: %w", err)
	}

	if existing == nil {
		// nothing to do
		return false, nil
	}

	// we need the doc ID so we can clean up inverted indices currently
	// pointing to this object
	docID, updateTime, err := storobj.DocIDAndTimeFromBinary(existing)
	if err != nil {
		return false, fmt.Errorf("get existing doc id from object binary: %w", err)
	}

	if skipIfLocalNewer && !deletionTime.IsZero() && updateTime >= deletionTime.UnixMilli() {
		return false, nil // live local object is newer; keep it (TimeBased)
	}

	w, err := s.prepareDeleteInvertedWrite(bucket, existing, docID)
	if err != nil {
		return false, fmt.Errorf("delete object from bucket: %w", err)
	}
	defer w.release()

	docIDBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(docIDBytes, docID)
	withSecondary := lsmkv.WithSecondaryKey(helpers.ObjectsBucketLSMDocIDSecondaryIndex, docIDBytes)
	if deletionTime.IsZero() {
		err = bucket.Delete(idBytes, withSecondary)
	} else {
		err = bucket.DeleteWith(idBytes, deletionTime, withSecondary)
	}
	if err != nil {
		return false, fmt.Errorf("delete object from bucket: %w", err)
	}

	// Never time.Now() — the target's LWW replay compares this against its
	// local object's updateTime.
	logTime := updateTime
	if !deletionTime.IsZero() {
		logTime = deletionTime.UnixMilli()
	}
	s.AppendChangeLogDelete(idBytes, logTime)

	if err = s.mayDeleteObjectHashTree(idBytes, updateTime, logTime); err != nil {
		return false, fmt.Errorf("object deletion in hashtree: %w", err)
	}

	err = w.apply()
	if err != nil {
		return false, fmt.Errorf("delete object from bucket: %w", err)
	}
	// Bucket.Shutdown waits for these pins, so release them before WriteWALs and the vector-index deletes.
	w.release()

	if err = s.store.WriteWALs(); err != nil {
		return false, fmt.Errorf("flush all buffered WALs: %w", err)
	}

	err = s.ForEachVectorQueue(func(targetVector string, queue *VectorIndexQueue) error {
		if err = queue.Delete(docID); err != nil {
			return fmt.Errorf("delete from vector index of vector %q: %w", targetVector, err)
		}
		return nil
	})
	if err != nil {
		return false, err
	}

	err = s.ForEachGeoQueue(func(propName string, queue *VectorIndexQueue) error {
		if err = queue.Delete(docID); err != nil {
			return fmt.Errorf("delete from geo index queue of prop %q: %w", propName, err)
		}
		return nil
	})
	if err != nil {
		return false, err
	}

	err = s.ForEachVectorQueue(func(targetVector string, queue *VectorIndexQueue) error {
		if err = queue.Flush(); err != nil {
			return fmt.Errorf("flush all vector index buffered WALs of vector %q: %w", targetVector, err)
		}
		return nil
	})
	if err != nil {
		return false, err
	}

	err = s.ForEachGeoQueue(func(propName string, queue *VectorIndexQueue) error {
		if err = queue.Flush(); err != nil {
			return fmt.Errorf("flush geo index queue WALs of prop %q: %w", propName, err)
		}
		return nil
	})
	if err != nil {
		return false, err
	}

	return true, nil
}

// prepareDeleteInvertedWrite refuses only on a missing bucket, so an object it cannot read or analyze stays deletable.
func (s *Shard) prepareDeleteInvertedWrite(objects *lsmkv.Bucket, previous []byte, docID uint64) (*invertedWrite, error) {
	w, err := s.prepareDeleteOf(objects, previous, docID)
	if err != nil && !errors.Is(err, lsmkv.ErrBucketNotFound) {
		return &invertedWrite{s: s, unreadable: err}, nil
	}
	return w, err
}

func (s *Shard) prepareDeleteOf(objects *lsmkv.Bucket, previous []byte, docID uint64) (*invertedWrite, error) {
	className, err := objects.ClassName()
	if err != nil {
		return nil, fmt.Errorf("getting bucket class name: %w", err)
	}
	previousObject, err := storobj.FromBinaryDisk(previous, className)
	if err != nil {
		return nil, fmt.Errorf("unmarshal previous object: %w", err)
	}
	return s.prepareInvertedWrite(nil, previousObject, objectInsertStatus{oldDocID: docID})
}

func (s *Shard) mayDeleteObjectHashTree(uuidBytes []byte, updateTime, deletionEventMs int64) error {
	if s.hashtree == nil {
		return nil
	}

	return s.deleteObjectHashTree(uuidBytes, updateTime, deletionEventMs)
}

func (s *Shard) deleteObjectHashTree(uuidBytes []byte, updateTime, deletionEventMs int64) error {
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

	// object deletion is treated as non-existent because the deletion time or
	// tombstone may not be available
	s.hashtree.AggregateLeafWith(leaf, objectDigest[:])

	// Fold a ≤cutoff delete out of the checkpoint. Require BOTH the event and the stored version
	// ≤cutoff: erasing a >cutoff version the clone never held would inject a phantom term.
	if cpht := s.asyncCheckpointHashtree; cpht != nil && deletionEventMs <= s.asyncCheckpointCutoff && updateTime <= s.asyncCheckpointCutoff {
		if err := cpht.AggregateLeafWith(leaf, objectDigest[:]); err != nil {
			return err
		}
	}

	return nil
}
