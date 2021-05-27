//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package db

import (
	"context"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/docid"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/sirupsen/logrus"
	bolt "go.etcd.io/bbolt"
)

func (s *Shard) deleteObject(ctx context.Context, id strfmt.UUID) error {
	idBytes, err := uuid.MustParse(id.String()).MarshalBinary()
	if err != nil {
		return err
	}

	var docID uint64
	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)
	existing, err := bucket.Get([]byte(idBytes))
	if err != nil {
		return errors.Wrap(err, "unexpected error on previous lookup")
	}

	if existing == nil {
		// nothing to do
		return nil
	}

	// we need the doc ID so we can clean up inverted indices currently
	// pointing to this object
	docID, err = storobj.DocIDFromBinary(existing)
	if err != nil {
		return errors.Wrap(err, "get existing doc id from object binary")
	}

	err = bucket.Delete(idBytes)
	if err != nil {
		return errors.Wrap(err, "delete object from bucket")
	}

	// in-mem
	// TODO: do we still need this?
	s.deletedDocIDs.Add(docID)

	// on disk
	err = docid.MarkDeletedLSM(s.store, docID)
	if err != nil {
		return errors.Wrap(err, "delete docID->uuid lookup")
	}

	if err := s.vectorIndex.Delete(docID); err != nil {
		return errors.Wrap(err, "delete from vector index")
	}

	return nil
}

func (s *Shard) periodicCleanup(batchSize int, batchCleanupInterval time.Duration) error {
	batchCleanupTicker := time.Tick(batchCleanupInterval)
	docIDs := s.deletedDocIDs.GetAll()
	if len(docIDs) == 0 {
		return nil
	}

	batches := (len(docIDs) / batchSize)
	if len(docIDs)%batchSize > 0 {
		batches = batches + 1
	}

	s.index.logger.WithFields(logrus.Fields{
		"action":          "shard_doc_id_periodic_cleanup",
		"class":           s.index.Config.ClassName,
		"total_found":     len(docIDs),
		"batch_size":      batchSize,
		"batches_created": batches,
		"batch_interval":  batchCleanupInterval,
	}).Debug("found doc ids to be deleted")

	for indx := 0; indx < batches; indx++ {
		start := indx * batchSize
		end := start + batchSize
		if end > len(docIDs) {
			end = len(docIDs)
		}
		err := s.performCleanup(docIDs[start:end])
		if err != nil {
			return err
		}
		<-batchCleanupTicker
	}
	return nil
}

func (s *Shard) performCleanup(deletedDocIDs []uint64) error {
	before := time.Now()
	defer s.metrics.InvertedDeleteDelta(before)

	className := s.index.Config.ClassName
	schemaModel := s.index.getSchema.GetSchemaSkipAuth().Objects
	class, err := schema.GetClassByName(schemaModel, className.String())
	if err != nil {
		return errors.Wrapf(err, "get class %s", className)
	}

	cleaner := inverted.NewCleaner(
		s.db,
		class,
		deletedDocIDs,
		func(b *bolt.Bucket, item inverted.Countable, docIDs []uint64,
			hasFrequency bool) error {
			return s.tryDeleteFromInvertedIndicesProp(b, item, docIDs, hasFrequency)
		},
	)
	deletedIDs, err := cleaner.Cleanup()
	if err != nil {
		return errors.Wrapf(err, "perform cleanup %v", deletedDocIDs)
	}

	s.deletedDocIDs.BulkRemove(deletedIDs)
	s.index.logger.WithFields(logrus.Fields{
		"action": "shard_doc_id_periodic_cleanup_batch_complete",
		"class":  className,
		"total":  len(deletedDocIDs),
		"took":   time.Since(before),
	}).Debug("completed doc id batch deletion")
	return nil
}

// func (s *Shard) deleteIndexIDLookup(tx *bolt.Tx, docID uint32) error {
// 	keyBuf := bytes.NewBuffer(make([]byte, 4))
// 	binary.Write(keyBuf, binary.LittleEndian, &docID)
// 	key := keyBuf.Bytes()

// 	b := tx.Bucket(helpers.IndexIDBucket)
// 	if b == nil {
// 		return fmt.Errorf("no index id bucket found")
// 	}

// 	if err := b.Delete(key); err != nil {
// 		return errors.Wrap(err, "delete uuid for index id")
// 	}

// 	return nil
// }
