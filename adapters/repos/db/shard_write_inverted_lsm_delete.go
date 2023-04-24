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

package db

import (
	"encoding/binary"
	"fmt"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

func (s *Shard) deleteFromInvertedIndicesLSM(props []inverted.Property,
	docID uint64,
) error {
	for _, prop := range props {
		hashBucket := s.store.Bucket(helpers.HashBucketFromPropNameLSM(prop.Name))
		if hashBucket == nil {
			return fmt.Errorf("no hash bucket for prop '%s' found", prop.Name)
		}

		if prop.IsFilterable {
			bucket := s.store.Bucket(helpers.BucketFromPropNameLSM(prop.Name))
			if bucket == nil {
				return fmt.Errorf("no bucket for prop '%s' found", prop.Name)
			}

			for _, item := range prop.Items {
				if err := s.deleteInvertedIndexItemLSM(bucket, hashBucket, item,
					docID); err != nil {
					return errors.Wrapf(err, "delete item '%s' from index",
						string(item.Data))
				}
			}
		}

		if prop.IsSearchable {
			bucket := s.store.Bucket(helpers.BucketSearchableFromPropNameLSM(prop.Name))
			if bucket == nil {
				return fmt.Errorf("no bucket searchable for prop '%s' found", prop.Name)
			}

			for _, item := range prop.Items {
				if err := s.deleteInvertedIndexItemWithFrequencyLSM(bucket, hashBucket, item,
					docID); err != nil {
					return errors.Wrapf(err, "delete item '%s' from index",
						string(item.Data))
				}
			}
		}
	}

	return nil
}

func (s *Shard) deleteInvertedIndexItemWithFrequencyLSM(bucket, hashBucket *lsmkv.Bucket,
	item inverted.Countable, docID uint64,
) error {
	lsmkv.CheckExpectedStrategy(bucket.Strategy(), lsmkv.StrategyMapCollection)

	hash, err := s.generateRowHash()
	if err != nil {
		return err
	}

	if err := hashBucket.Put(item.Data, hash); err != nil {
		return err
	}

	docIDBytes := make([]byte, 8)
	// Shard Index version 2 requires BigEndian for sorting, if the shard was
	// built prior assume it uses LittleEndian
	if s.versioner.Version() < 2 {
		binary.LittleEndian.PutUint64(docIDBytes, docID)
	} else {
		binary.BigEndian.PutUint64(docIDBytes, docID)
	}

	return bucket.MapDeleteKey(item.Data, docIDBytes)
}

func (s *Shard) deleteInvertedIndexItemLSM(bucket, hashBucket *lsmkv.Bucket,
	item inverted.Countable, docID uint64,
) error {
	lsmkv.CheckExpectedStrategy(bucket.Strategy(), lsmkv.StrategySetCollection, lsmkv.StrategyRoaringSet)

	hash, err := s.generateRowHash()
	if err != nil {
		return err
	}

	if err := hashBucket.Put(item.Data, hash); err != nil {
		return err
	}

	if bucket.Strategy() == lsmkv.StrategyRoaringSet {
		return bucket.RoaringSetRemoveOne(item.Data, docID)
	}

	docIDBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(docIDBytes, docID)

	return bucket.SetDeleteSingle(item.Data, docIDBytes)
}
