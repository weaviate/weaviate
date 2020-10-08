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

	"github.com/boltdb/bolt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/kinds"
)

func (s *Shard) mergeObject(ctx context.Context, merge kinds.MergeDocument) error {
	idBytes, err := uuid.MustParse(merge.ID.String()).MarshalBinary()
	if err != nil {
		return err
	}

	var status objectInsertStatus

	if err := s.db.Batch(func(tx *bolt.Tx) error {
		s, err := s.mergeObjectInTx(tx, merge, idBytes)
		if err != nil {
			return err
		}

		status = s
		return nil
	}); err != nil {
		return errors.Wrap(err, "bolt batch tx")
	}

	if err := s.updateVectorIndex(merge.Vector, status); err != nil {
		return errors.Wrap(err, "update vector index")
	}

	return nil
}

func (s *Shard) mergeObjectInTx(tx *bolt.Tx, merge kinds.MergeDocument,
	idBytes []byte) (objectInsertStatus, error) {
	bucket := tx.Bucket(helpers.ObjectsBucket)
	previous := bucket.Get([]byte(idBytes))

	nextObj, err := s.mergeObjectData(previous, merge)
	if err != nil {
		return objectInsertStatus{}, errors.Wrap(err, "merge object data")
	}

	status, err := s.determineInsertStatus(previous, nextObj)
	if err != nil {
		return status, errors.Wrap(err, "check insert/update status")
	}

	nextObj.SetIndexID(status.docID)
	nextBytes, err := nextObj.MarshalBinary()
	if err != nil {
		return status, errors.Wrapf(err, "marshal object %s to binary", nextObj.ID())
	}

	if err := s.upsertObjectData(bucket, idBytes, nextBytes); err != nil {
		return status, errors.Wrap(err, "upsert object data")
	}

	// build indexID->UUID lookup
	// uuid is immutable, so this does not need to be altered, cleaned on
	// updates, but is simply idempotent
	if err := s.addIndexIDLookup(tx, idBytes, status.docID); err != nil {
		return status, errors.Wrap(err, "add docID->UUID index")
	}

	if err := s.updateInvertedIndex(tx, nextObj, status, previous); err != nil {
		return status, errors.Wrap(err, "udpate inverted indices")
	}

	return status, nil
}

func (s *Shard) mergeObjectData(previous []byte,
	merge kinds.MergeDocument) (*storobj.Object, error) {
	var previousObj *storobj.Object
	if len(previous) == 0 {
		// DocID must be overwrite after status check, simply set to initial
		// value
		previousObj = storobj.New(merge.Kind, 0)
		previousObj.SetClass(merge.Class)
		previousObj.SetID(merge.ID)
	} else {
		p, err := storobj.FromBinary(previous)
		if err != nil {
			return nil, errors.Wrap(err, "unmarshal previous")
		}

		previousObj = p
	}

	return mergeProps(previousObj, merge), nil
}

func mergeProps(previous *storobj.Object,
	merge kinds.MergeDocument) *storobj.Object {
	next := *previous
	schema, ok := next.Schema().(map[string]interface{})
	if !ok {
		schema = map[string]interface{}{}
	}

	for propName, value := range merge.PrimitiveSchema {
		// for primtive props, we simply need to overwrite
		schema[propName] = value
	}

	for _, ref := range merge.References {
		propName := ref.From.Property.String()
		prop := schema[propName]
		propParsed, ok := prop.(models.MultipleRef)
		if !ok {
			propParsed = models.MultipleRef{}
		}
		propParsed = append(propParsed, ref.To.SingleRef())
		schema[propName] = propParsed
	}

	if merge.Vector == nil {
		next.Vector = previous.Vector
	} else {
		next.Vector = merge.Vector
	}

	return &next
}
