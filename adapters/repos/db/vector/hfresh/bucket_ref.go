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
	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// bucketRef names a bucket instead of holding it, so every operation resolves
// it afresh.
//
// The stores in this package are built once and live as long as the index. A
// bucket pointer captured at construction outlives the bucket itself: a shard
// teardown deregisters buckets while requests are still in flight, and reads
// through the stale pointer neither observe that nor fail — the segment list
// is emptied on shutdown, so they quietly return whatever the memtable still
// holds. Resolving per operation turns that silent wrong answer into
// [lsmkv.ErrBucketNotFound].
type bucketRef struct {
	store *lsmkv.Store
	name  string
}

func newBucketRef(store *lsmkv.Store, name string) bucketRef {
	return bucketRef{store: store, name: name}
}

// get resolves the bucket, or reports that the store no longer holds it.
func (r bucketRef) get() (*lsmkv.Bucket, error) {
	bucket := r.store.Bucket(r.name)
	if bucket == nil {
		return nil, errors.Wrapf(lsmkv.ErrBucketNotFound, "bucket %s", r.name)
	}
	return bucket, nil
}
