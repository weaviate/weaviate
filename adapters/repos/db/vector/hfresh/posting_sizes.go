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

package hfresh

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// These constants define the prefixes used in the
// lsmkv bucket to namespace different types of data.
const (
	versionMapBucketPrefix      = 'v'
	metadataBucketPrefix        = 'm'
	postingMetadataBucketPrefix = 'p'
	reassignBucketKey           = "pending_reassignments"
)

// NewSharedBucket creates a shared lsmkv bucket for the HFresh index.
// This bucket is used to store metadata in namespaced regions of the bucket.
func NewSharedBucket(store *lsmkv.Store, indexID string, cfg StoreConfig) (*lsmkv.Bucket, error) {
	bName := sharedBucketName(indexID)
	err := store.CreateOrLoadBucket(context.Background(),
		bName,
		cfg.MakeBucketOptions(lsmkv.StrategyReplace, lsmkv.WithForceCompaction(true))...,
	)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create or load bucket %s", bName)
	}

	return store.Bucket(bName), nil
}

func sharedBucketName(id string) string {
	return fmt.Sprintf("hfresh_shared_%s", id)
}
