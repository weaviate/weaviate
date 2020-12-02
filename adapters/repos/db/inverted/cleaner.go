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

package inverted

import (
	"bytes"
	"encoding/binary"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
)

type deleteFn func(b *bolt.Bucket, item Countable, docIDs []uint32, hasFrequency bool) error

type Cleaner struct {
	db            *bolt.DB
	class         *models.Class
	deletedDocIDs []uint32
	deleteFn      deleteFn
}

func NewCleaner(db *bolt.DB, class *models.Class, deletedDocIDs []uint32, deleteFn deleteFn) *Cleaner {
	return &Cleaner{db, class, deletedDocIDs, deleteFn}
}

func (c *Cleaner) getDocumentKey(documentID uint32) []byte {
	keyBuf := bytes.NewBuffer(make([]byte, 4))
	binary.Write(keyBuf, binary.LittleEndian, &documentID)
	key := keyBuf.Bytes()
	return key
}

func (c *Cleaner) propHasFrequency(p *models.Property) bool {
	for i := range p.DataType {
		if dt := schema.DataType(p.DataType[i]); dt == schema.DataTypeString || dt == schema.DataTypeText {
			return true
		}
	}
	return false
}

func (c *Cleaner) cleanupProperty(tx *bolt.Tx, p *models.Property) error {
	hasFrequency := c.propHasFrequency(p)
	id := helpers.BucketFromPropName(p.Name)
	propsBucket := tx.Bucket(id)
	if propsBucket == nil {
		return nil
	}
	err := propsBucket.ForEach(func(item, data []byte) error {
		return c.deleteFn(propsBucket, Countable{Data: item}, c.deletedDocIDs, hasFrequency)
	})
	if err != nil {
		return errors.Wrapf(err, "cleanup property %s row", p.Name)
	}
	return nil
}

// docID
func (c *Cleaner) deleteDocID(tx *bolt.Tx, documentID uint32) bool {
	key := c.getDocumentKey(documentID)
	docsBucket := tx.Bucket(helpers.DocIDBucket)
	if docsBucket == nil {
		return false
	}
	err := docsBucket.Delete(key)
	return err == nil
}

// Cleanup cleans up properties for given documents
func (c *Cleaner) Cleanup() ([]uint32, error) {
	performedDeletion := make([]uint32, 0)
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
