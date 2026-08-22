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
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// armedMirror is what one registration armed: the properties it mirrors, and
// the bucket each of them was armed on. The bucket is not bookkeeping — it is
// the only thing that tells this record's flip apart from someone tearing its
// staged bucket down. Both leave the staged name unresolvable.
type armedMirror struct {
	props   map[string]struct{}
	buckets map[string]*lsmkv.Bucket
}

// resolveScopedDoubleWriteBucket is the shared prologue for every strategy's
// double-write callback: scope-filters the property, then resolves the bucket
// the mirror writes into. skip=true means the callback must no-op.
//
// The canonical name is a fallback and not a second choice. A mirror stays
// armed until the edge that ends its record's chance of becoming live disarms
// it, and SwapBucketPointer deletes the staged-name entry at the flip, after
// which the canonical name denotes the very bucket this mirror was arming
// (weaviate/weaviate#11688). It denotes something else entirely when the
// staged bucket was shut down instead — a discard, a supersession retirement,
// a cancel sweep — and following the name there writes this migration's
// target-form rows into live source-form data. So the fallback is taken only
// when the canonical name resolves to the bucket this mirror armed on.
func resolveScopedDoubleWriteBucket(shard *Shard, property *inverted.Property,
	armed armedMirror, bucketNamer, sourceBucketName func(string) string,
) (bucket *lsmkv.Bucket, bucketName string, skip bool) {
	if _, ok := armed.props[property.Name]; !ok {
		return nil, "", true
	}
	bucketName = bucketNamer(property.Name)
	if bucket = shard.store.Bucket(bucketName); bucket != nil {
		return bucket, bucketName, false
	}

	fallback := shard.store.Bucket(sourceBucketName(property.Name))
	if fallback == nil || fallback != armed.buckets[property.Name] {
		return nil, bucketName, true
	}
	return fallback, bucketName, false
}
