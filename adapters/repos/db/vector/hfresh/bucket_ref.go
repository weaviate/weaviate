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
// and pins it afresh.
//
// The stores in this package are built once and live as long as the index, but
// the buckets under them do not: a shard teardown deregisters a bucket and
// frees its mmap'd segments while requests are still in flight. A bucket
// pointer captured at construction therefore outlives the bucket, and reading
// through it is a use-after-free, not merely a stale read.
//
// Resolving by name is necessary but not sufficient. [lsmkv.Bucket.Shutdown]
// waits only for pins taken through [lsmkv.Store.AcquireBucketForRead] before
// it frees segments, so an unpinned pointer — however freshly resolved — can
// still be freed mid-operation. Every access goes through [bucketRef.acquire],
// which holds that pin for the caller's whole operation, cursor iteration
// included. A bucket already gone at resolve time reports
// [lsmkv.ErrBucketNotFound].
type bucketRef struct {
	store *lsmkv.Store
	name  string
}

func newBucketRef(store *lsmkv.Store, name string) bucketRef {
	return bucketRef{store: store, name: name}
}

// acquire resolves the bucket and pins it against teardown, or reports that
// the store no longer holds it. The pin blocks a concurrent bucket shutdown,
// so callers MUST call the returned release exactly once — deferring it at the
// call site — and MUST NOT retain the bucket beyond it.
func (r bucketRef) acquire() (*lsmkv.Bucket, func(), error) {
	bucket, release := r.store.AcquireBucketForRead(r.name)
	if bucket == nil {
		release()
		return nil, nil, errors.Wrapf(lsmkv.ErrBucketNotFound, "bucket %s", r.name)
	}
	return bucket, release, nil
}
