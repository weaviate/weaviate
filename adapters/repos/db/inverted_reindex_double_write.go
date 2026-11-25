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

type armedMirror struct {
	props   map[string]struct{}
	buckets map[string]*lsmkv.Bucket
}

func resolveScopedDoubleWriteBucket(shard *Shard, property *inverted.Property,
	armed armedMirror, bucketNamer, sourceBucketName func(string) string,
) (bucket *lsmkv.Bucket, bucketName string, skip bool) {
	if _, ok := armed.props[property.Name]; !ok {
		return nil, "", true
	}
	bucketName = bucketNamer(property.Name)
	if bucket = shard.store.Bucket(bucketName); bucket != nil {
		if armedBucket, known := armed.buckets[property.Name]; known && bucket != armedBucket {
			return nil, bucketName, true
		}
		return bucket, bucketName, false
	}

	fallback := shard.store.Bucket(sourceBucketName(property.Name))
	if fallback == nil || fallback != armed.buckets[property.Name] {
		return nil, bucketName, true
	}
	return fallback, bucketName, false
}
