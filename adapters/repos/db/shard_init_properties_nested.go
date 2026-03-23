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
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
)

// createNestedPropertyBuckets creates the value and metadata buckets for a
// nested (object/object[]) property. Both use StrategyRoaringSet with keys
// prefixed by hash8(path) to multiplex all sub-properties into shared buckets.
func (s *Shard) createNestedPropertyBuckets(ctx context.Context, prop *models.Property,
	makeBucketOptions lsmkv.MakeBucketOptions,
) error {
	if err := s.isReadOnly(); err != nil {
		return err
	}

	if inverted.HasNestedFilterableIndex(prop) {
		if err := s.store.CreateOrLoadBucket(ctx,
			helpers.BucketNestedFromPropNameLSM(prop.Name),
			makeBucketOptions(lsmkv.StrategyRoaringSet)...,
		); err != nil {
			return fmt.Errorf("init nested prop %q: value bucket: %w", prop.Name, err)
		}
	}

	if inverted.HasNestedSearchableIndex(prop) {
		// TODO: create nested searchable bucket (Phase 2)
	}

	if inverted.HasNestedRangeableIndex(prop) {
		// TODO: create nested rangeable bucket (Phase 3)
	}

	if inverted.HasAnyNestedInvertedIndex(prop) {
		if err := s.store.CreateOrLoadBucket(ctx,
			helpers.BucketNestedMetaFromPropNameLSM(prop.Name),
			makeBucketOptions(lsmkv.StrategyRoaringSet)...,
		); err != nil {
			return fmt.Errorf("init nested prop %q: meta bucket: %w", prop.Name, err)
		}
	}

	return nil
}
