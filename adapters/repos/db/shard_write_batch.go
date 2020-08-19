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

	"github.com/boltdb/bolt"
	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
)

// return value map[int]error gives the error for the index as it received it
func (s *Shard) putObjectBatch(ctx context.Context, objects []*storobj.Object) map[int]error {
	maxPerTransaction := 30

	m := &sync.Mutex{}
	docIDs := map[strfmt.UUID]uint32{}
	errs := map[int]error{} // int represents original index

	var wg = &sync.WaitGroup{}
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
				for j := range batch {
					// so we can reference potential errors
					affectedIndices = append(affectedIndices, i+j)
				}

				for _, object := range batch {
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
					docIDs[object.ID()] = status.docID
					m.Unlock()
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

	// TODO: is it smart to let them all run in parallel? wouldn't it be better
	// to open no more threads than we have cpu cores?
	wg = &sync.WaitGroup{}
	for i, object := range objects {
		if _, ok := errs[i]; ok {
			// had an error prior, ignore
			continue
		}

		wg.Add(1)
		docID := int(docIDs[object.ID()])
		go func(object *storobj.Object, docID int, index int) {
			defer wg.Done()

			if err := s.vectorIndex.Add(docID, object.Vector); err != nil {
				m.Lock()
				errs[index] = errors.Wrap(err, "insert to vector index")
				m.Unlock()
			}
		}(object, docID, i)
	}
	wg.Wait()

	return errs
}
