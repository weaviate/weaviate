//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package inverted

import (
	"bytes"
	"encoding/binary"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	bolt "go.etcd.io/bbolt"
)

type deleteFn func(b *bolt.Bucket, item Countable, docIDs []uint64, hasFrequency bool) error

type Cleaner struct {
	db            *bolt.DB
	class         *models.Class
	deletedDocIDs []uint64
	deleteFn      deleteFn
}

func NewCleaner(db *bolt.DB, class *models.Class, deletedDocIDs []uint64, deleteFn deleteFn) *Cleaner {
	return &Cleaner{db, class, deletedDocIDs, deleteFn}
}

func (c *Cleaner) getDocumentKey(documentID uint64) []byte {
	keyBuf := bytes.NewBuffer(nil)
	binary.Write(keyBuf, binary.LittleEndian, &documentID)
	key := keyBuf.Bytes()
	return key
}

func (c *Cleaner) propHasFrequency(p *models.Property) bool {
	for i := range p.DataType {
		if dt := schema.DataType(p.DataType[i]); dt == schema.DataTypeText {
			return true
		}
	}
	return false
}

func (c *Cleaner) cleanupProperty(tx *bolt.Tx, p *models.Property) error {
	hasFrequency := c.propHasFrequency(p)
	id := helpers.BucketFromPropName(p.Name)
	if err := c.cleanupBucket(tx, id, hasFrequency, p.Name); err != nil {
		return err
	}

	if !schema.IsRefDataType(p.DataType) {
		// we are done
		return nil
	}

	countName := helpers.MetaCountProp(p.Name)
	id = helpers.BucketFromPropName(countName)
	if err := c.cleanupBucket(tx, id, false, countName); err != nil {
		return err
	}

	return nil
}

func (c *Cleaner) cleanupBucket(tx *bolt.Tx, id []byte,
	hasFrequency bool, propName string,
) error {
	propsBucket := tx.Bucket(id)
	if propsBucket == nil {
		return nil
	}
	err := propsBucket.ForEach(func(item, data []byte) error {
		return c.deleteFn(propsBucket, Countable{Data: item},
			c.deletedDocIDs, hasFrequency)
	})
	if err != nil {
		return errors.Wrapf(err, "cleanup property %s row", propName)
	}

	return nil
}

// docID
func (c *Cleaner) deleteDocID(tx *bolt.Tx, documentID uint64) bool {
	key := c.getDocumentKey(documentID)
	docsBucket := tx.Bucket(helpers.DocIDBucket)
	if docsBucket == nil {
		return false
	}
	err := docsBucket.Delete(key)
	return err == nil
}

// Cleanup cleans up properties for given documents
func (c *Cleaner) Cleanup() ([]uint64, error) {
	performedDeletion := make([]uint64, 0)
	err := c.db.Update(func(tx *bolt.Tx) error {
		// cleanup properties
		for _, p := range c.class.Properties {
			err := c.cleanupProperty(tx, p)
			if err != nil {
				return err
			}
		}
		for _, documentID := range c.deletedDocIDs {
			// delete document
			if c.deleteDocID(tx, documentID) {
				performedDeletion = append(performedDeletion, documentID)
			}
		}
		return nil
	})
	if err != nil {
		return performedDeletion, err
	}
	return performedDeletion, nil
}
