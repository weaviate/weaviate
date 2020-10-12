//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package db

import (
	"context"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
	"github.com/semi-technologies/weaviate/usecases/kinds"
)

// return value map[int]error gives the error for the index as it received it
func (s *Shard) putObjectBatch(ctx context.Context, objects []*storobj.Object) map[int]error {
	beforeBatch := time.Now()
	defer s.metrics.BatchObject(beforeBatch, len(objects))

	maxPerTransaction := 30

	m := &sync.Mutex{}
	statuses := map[strfmt.UUID]objectInsertStatus{}
	errs := map[int]error{} // int represents original index

	duplicates := findDuplicatesInBatchObjects(objects)
	_ = duplicates

	beforeObjectStore := time.Now()
	wg := &sync.WaitGroup{}
	for i := 0; i < len(objects); i += maxPerTransaction {
		end := i + maxPerTransaction
		if end > len(objects) {
			end = len(objects)
		}

		batch := objects[i:end]
		wg.Add(1)
		go func(i int, batch []*storobj.Object) {
			defer wg.Done()
			var affectedIndices []int
			if err := s.db.Batch(func(tx *bolt.Tx) error {
				if err := ctx.Err(); err != nil {
					return errors.Wrapf(err, "begin transaction %d of batch", i)
				}

				for j := range batch {
					// so we can reference potential errors
					affectedIndices = append(affectedIndices, i+j)
				}

				for j, object := range batch {
					if _, ok := duplicates[i+j]; ok {
						continue
					}
					uuidParsed, err := uuid.Parse(object.ID().String())
					if err != nil {
						return errors.Wrap(err, "invalid id")
					}

					idBytes, err := uuidParsed.MarshalBinary()
					if err != nil {
						return err
					}

					status, err := s.putObjectInTx(tx, object, idBytes)
					if err != nil {
						return err
					}

					m.Lock()
					statuses[object.ID()] = status
					m.Unlock()

					if err := ctx.Err(); err != nil {
						return errors.Wrapf(err, "end transaction %d of batch", i)
					}
				}
				return nil
			}); err != nil {
				m.Lock()
				err = errors.Wrap(err, "bolt batch tx")
				for _, affected := range affectedIndices {
					errs[affected] = err
				}
				m.Unlock()
			}
		}(i, batch)
	}
	wg.Wait()
	s.metrics.ObjectStore(beforeObjectStore)

	if err := ctx.Err(); err != nil {
		for i, err := range errs {
			if err == nil {
				// already has an error, ignore
				continue
			}

			errs[i] = errors.Wrapf(err,
				"inverted indexing complete, about to start vector indexing")
		}
	}

	beforeVectorIndex := time.Now()
	wg = &sync.WaitGroup{}
	for i, object := range objects {
		m.Lock()
		_, ok := errs[i]
		m.Unlock()
		if ok {
			// had an error prior, ignore
			continue
		}
		if _, ok := duplicates[i]; ok {
			// is a duplicate, ignore
			continue
		}

		wg.Add(1)
		status := statuses[object.ID()]
		go func(object *storobj.Object, status objectInsertStatus, index int) {
			defer wg.Done()
			if err := ctx.Err(); err != nil {
				m.Lock()
				errs[index] = errors.Wrap(err, "insert to vector index")
				m.Unlock()
			}

			if err := s.updateVectorIndex(object.Vector, status); err != nil {
				m.Lock()
				errs[index] = errors.Wrap(err, "insert to vector index")
				m.Unlock()
			}
		}(object, status, i)
	}
	wg.Wait()
	s.metrics.VectorIndex(beforeVectorIndex)

	return errs
}

// returns the originalIndexIDs to be ignored
func findDuplicatesInBatchObjects(in []*storobj.Object) map[int]struct{} {
	count := map[strfmt.UUID]int{}
	for _, obj := range in {
		count[obj.ID()] = count[obj.ID()] + 1
	}

	ignore := map[int]struct{}{}
	for i, obj := range in {
		if c := count[obj.ID()]; c > 1 {
			count[obj.ID()] = c - 1
			ignore[i] = struct{}{}
		}
	}

	return ignore
}

// return value map[int]error gives the error for the index as it received it
func (s *Shard) addReferencesBatch(ctx context.Context,
	refs kinds.BatchReferences) map[int]error {
	maxPerTransaction := 30

	m := &sync.Mutex{}
	errs := map[int]error{} // int represents original index

	wg := &sync.WaitGroup{}
	for i := 0; i < len(refs); i += maxPerTransaction {
		end := i + maxPerTransaction
		if end > len(refs) {
			end = len(refs)
		}

		batch := refs[i:end]
		wg.Add(1)
		go func(i int, batch kinds.BatchReferences) {
			defer wg.Done()
			var affectedIndices []int
			if err := s.db.Batch(func(tx *bolt.Tx) error {
				for j := range batch {
					// so we can reference potential errors
					affectedIndices = append(affectedIndices, i+j)
				}

				for _, ref := range batch {
					uuidParsed, err := uuid.Parse(ref.From.TargetID.String())
					if err != nil {
						return errors.Wrap(err, "invalid id")
					}

					idBytes, err := uuidParsed.MarshalBinary()
					if err != nil {
						return err
					}

					mergeDoc := mergeDocFromBatchReference(ref)
					_, err = s.mergeObjectInTx(tx, mergeDoc, idBytes)
					if err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				m.Lock()
				err = errors.Wrap(err, "bolt batch tx")
				for _, affected := range affectedIndices {
					errs[affected] = err
				}
				m.Unlock()
			}
		}(i, batch)
	}
	wg.Wait()

	// adding references can not alter the vector position, so no need to alter
	// the vector index

	return errs
}

func mergeDocFromBatchReference(ref kinds.BatchReference) kinds.MergeDocument {
	return kinds.MergeDocument{
		Kind:       ref.From.Kind,
		Class:      ref.From.Class.String(),
		ID:         ref.From.TargetID,
		UpdateTime: time.Now().UnixNano(),
		References: kinds.BatchReferences{ref},
	}
}
