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

package db

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
)

func (s *Shard) DeleteObject(ctx context.Context, id strfmt.UUID, deletionTime time.Time) error {
	if err := s.isReadOnly(); err != nil {
		return err
	}

	s.asyncReplicationRWMux.RLock()
	defer s.asyncReplicationRWMux.RUnlock()

	err := s.waitForMinimalHashTreeInitialization(ctx)
	if err != nil {
		return err
	}

	idBytes, err := uuid.MustParse(id.String()).MarshalBinary()
	if err != nil {
		return err
	}

	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)

	// see comment in shard_write_put.go::putObjectLSM
	lock := &s.docIdLock[s.uuidToIdLockPoolId(idBytes)]

	lock.Lock()
	defer lock.Unlock()

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

	if err = s.mayDeleteObjectHashTree(idBytes, updateTime); err != nil {
		return fmt.Errorf("object deletion in hashtree: %w", err)
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

func (s *Shard) DeleteObjectsExpired(ctx context.Context, expirationThreshold time.Time, deleteOnPropName string) error {
	// ticker triggers cleanup, based on env vars
	// get all classes with ttl enabled
	// for each class, for each shard run delete
	// use pool of routines to go through all eligible shards
	// all method on shard (class already known, pass ttl and prop name)

	filter := &filters.LocalFilter{Root: &filters.Clause{
		Operator: filters.OperatorLessThanEqual,
		Value: &filters.Value{
			Value: expirationThreshold,
			Type:  schema.DataTypeDate,
		},
		On: &filters.Path{
			Class:    s.index.Config.ClassName,
			Property: schema.PropertyName(deleteOnPropName),
		},
	}}

	allowlist, err := s.buildAllowList(ctx, filter, additional.Properties{})
	if err != nil {
		return fmt.Errorf("filter expired objects: %w", err)
	}

	fmt.Printf("  ==> allowlist %v\n", allowlist.Slice())
	fmt.Printf("  ==> threshold %s\n\n", expirationThreshold)

	if allowlist.IsEmpty() {
		return nil
	}

	deletionTime := time.Now()
	ec := errorcompounder.NewSafe()
	it := allowlist.Iterator()

	eg := errors.NewErrorGroupWrapper(s.index.logger)
	eg.SetLimit(concurrency.NUMCPU)

	for docID, ok := it.Next(); ok; docID, ok = it.Next() {
		docID := docID
		if ctx.Err() != nil {
			break
		}
		eg.Go(func() error {
			if ctx.Err() != nil {
				return nil
			}
			if err := s.deleteObjectExpiredByDocId(ctx, docID, deleteOnPropName, expirationThreshold, deletionTime); err != nil {
				ec.Add(fmt.Errorf("deleteObjectExpiredByDocID %d: %w", docID, err))
			}
			return nil
		})
	}

	eg.Wait()
	if err := ec.ToError(); err != nil {
		return err // TODO aliszka:ttl wrap
	}
	if err := ctx.Err(); err != nil {
		return err // TODO aliszka:ttl wrap, check if loop really broken due to ctx
	}

	return nil
}

func (s *Shard) deleteObjectExpiredByDocId(ctx context.Context, docID uint64, deleteOnPropName string,
	expirationThreshold time.Time, deletionTime time.Time,
) error {
	docIDBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(docIDBytes, docID)

	// TODO aliszka:ttl get once for all calls
	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)
	// TODO aliszka:ttl with reusable buffer?
	uuidObjBytes, err := bucket.GetBySecondary(ctx, helpers.ObjectsBucketLSMDocIDSecondaryIndex, docIDBytes)
	if err != nil {
		return fmt.Errorf("get existing object for uuid: %w", err)
	}
	if uuidObjBytes == nil {
		// TODO aliszka:ttl debug log?
		fmt.Printf("  ==> object not found by docID[%d]\n\n", docID)
		return nil
	}

	uuidObj, err := storobj.FromBinaryUUIDOnly(uuidObjBytes)
	if err != nil {
		return fmt.Errorf("unmarshalling uuid of object: %w", err)
	}
	uuidBytes, err := uuid.MustParse(uuidObj.ID().String()).MarshalBinary()
	if err != nil {
		return err
	}

	// TODO aliszka:ttl lock once for all calls?
	s.asyncReplicationRWMux.RLock()
	defer s.asyncReplicationRWMux.RUnlock()

	if err := s.waitForMinimalHashTreeInitialization(ctx); err != nil {
		return err
	}

	lock := &s.docIdLock[s.uuidToIdLockPoolId(uuidBytes)]
	lock.Lock()
	defer lock.Unlock()

	existingObjBytes, err := bucket.Get(uuidBytes)
	if err != nil {
		return fmt.Errorf("get existing object: %w", err)
	}
	if existingObjBytes == nil {
		// TODO aliszka:ttl debug log?
		fmt.Printf("  ==> object not found by uuid docID[%d] uuid[%s]\n\n", docID, uuidObj.ID().String())
		return nil
	}

	var updateMillis int64
	var deleteOnTime time.Time
	var curDocID uint64

	switch deleteOnPropName {
	case filters.InternalPropCreationTimeUnix, filters.InternalPropLastUpdateTimeUnix:
		obj, err := storobj.FromBinaryUUIDOnly(existingObjBytes)
		if err != nil {
			return fmt.Errorf("unmarshalling object with timestamps: %w", err)
		}
		curDocID = obj.DocID
		updateMillis = obj.LastUpdateTimeUnix()
		deleteOnMillis := updateMillis
		if deleteOnPropName == filters.InternalPropCreationTimeUnix {
			deleteOnMillis = obj.CreationTimeUnix()
		}
		deleteOnTime = time.UnixMilli(deleteOnMillis)

	default:
		// TODO aliszka:ttl extract to create once
		propExtraction := storobj.NewPropExtraction()
		propExtraction.Add(deleteOnPropName)

		obj, err := storobj.FromBinaryOptional(existingObjBytes, additional.Properties{}, propExtraction)
		if err != nil {
			return fmt.Errorf("unmarshalling object with props: %w", err)
		}

		curDocID = obj.DocID
		updateMillis = obj.LastUpdateTimeUnix()
		props := obj.Properties().(map[string]any)
		val, ok := props[deleteOnPropName]
		if !ok {
			// TODO aliszka:ttl debug log?
			fmt.Printf("  ==> no prop find in object docID[%d] uuid[%s]\n\n", docID, uuidObj.ID().String())
			return nil
		}
		deleteOnTimeStr, ok := val.(string)
		if !ok {
			return fmt.Errorf("date as string expected, got %T", val)
		}
		deleteOnTime, err = time.Parse(time.RFC3339, deleteOnTimeStr)
		if err != nil {
			return fmt.Errorf("parse date: %w", err)
		}
	}

	fmt.Printf("  ==> updateMillis[%d] deleteOnTime [%s] curDocID [%d]\ndocID [%d] uuid [%s]\n\n",
		updateMillis, deleteOnTime, curDocID, docID, uuidObj.ID().String())

	if deleteOnTime.After(expirationThreshold) {
		// TODO aliszka:ttl debug log?
		fmt.Printf("  ==> final check - not to be deleted yet! docID[%d] uuid[%s] docID[%d]\n\n", docID, uuidObj.ID().String(), curDocID)
		return nil
	}

	binary.LittleEndian.PutUint64(docIDBytes, curDocID)
	secKey := lsmkv.WithSecondaryKey(helpers.ObjectsBucketLSMDocIDSecondaryIndex, docIDBytes)

	if deletionTime.IsZero() {
		err = bucket.Delete(uuidBytes, secKey)
	} else {
		err = bucket.DeleteWith(uuidBytes, deletionTime, secKey)
	}
	if err != nil {
		return fmt.Errorf("delete object from bucket: %w", err)
	}

	if err = s.mayDeleteObjectHashTree(uuidBytes, updateMillis); err != nil {
		return fmt.Errorf("object deletion in hashtree: %w", err)
	}

	err = s.cleanupInvertedIndexOnDelete(existingObjBytes, curDocID)
	if err != nil {
		return fmt.Errorf("delete object from indexes: %w", err)
	}

	// TODO aliszka:ttl run once?
	if err = s.store.WriteWALs(); err != nil {
		return fmt.Errorf("flush all buffered WALs: %w", err)
	}

	// TODO aliszka:ttl run once?
	err = s.ForEachVectorQueue(func(targetVector string, queue *VectorIndexQueue) error {
		if err = queue.Delete(curDocID); err != nil {
			return fmt.Errorf("delete from vector index of vector %q: %w", targetVector, err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	// TODO aliszka:ttl run once?
	err = s.ForEachVectorQueue(func(targetVector string, queue *VectorIndexQueue) error {
		if err = queue.Flush(); err != nil {
			return fmt.Errorf("flush all vector index buffered WALs of vector %q: %w", targetVector, err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}
