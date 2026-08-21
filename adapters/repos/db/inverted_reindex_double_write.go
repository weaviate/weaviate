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

// resolveScopedDoubleWriteBucket is the shared prologue for every strategy's
// double-write callback: scope-filters the property, then resolves the bucket
// the mirror writes into. skip=true means the callback must no-op.
//
// The canonical name is the fallback and never a bare second choice: a mirror
// stays armed until the edge that ends its record's chance of becoming live
// disarms it, but SwapBucketPointer deletes the staged-name entry at the flip,
// after which the canonical name denotes the same physical bucket
// (weaviate/weaviate#11688).
func resolveScopedDoubleWriteBucket(shard *Shard, property *inverted.Property,
	propsByName map[string]struct{}, bucketNamer, sourceBucketName func(string) string,
) (bucket *lsmkv.Bucket, bucketName string, skip bool) {
	if _, ok := propsByName[property.Name]; !ok {
		return nil, "", true
	}
	bucketName = bucketNamer(property.Name)
	bucket = shard.store.Bucket(bucketName)
	if bucket == nil {
		bucket = shard.store.Bucket(sourceBucketName(property.Name))
	}
	return bucket, bucketName, bucket == nil
}
