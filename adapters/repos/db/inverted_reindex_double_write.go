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

// resolveDoubleWriteBucket resolves the bucket a double-write callback should
// mirror into. Callers MUST use this instead of a bare store.Bucket(sidecarName)
// lookup: callbacks stay armed until disableCallbacks runs at the end of
// runtimeSwap, but SwapBucketPointer deletes the sidecar-name entry at the
// flip, so a bare lookup can resolve nil and panic (weaviate/weaviate#11688).
//
//   - sidecar resolves (pre-swap): mirror into it.
//   - sidecar gone + swapFallbackName set (ingest phase): resolve the
//     canonical name instead — post-flip it denotes the same physical bucket.
//   - sidecar gone + no fallback (backup phase): nil means skip the mirror.
func resolveDoubleWriteBucket(shard *Shard, sidecarName, swapFallbackName string) *lsmkv.Bucket {
	if b := shard.store.Bucket(sidecarName); b != nil {
		return b
	}
	if swapFallbackName == "" {
		return nil
	}
	return shard.store.Bucket(swapFallbackName)
}

// resolveScopedDoubleWriteBucket is the shared prologue for every strategy's
// double-write callback: scope-filters the property, then resolves the bucket
// via [resolveDoubleWriteBucket] (forTargetStrategy arms the swap fallback).
// skip=true means the callback must no-op.
func resolveScopedDoubleWriteBucket(shard *Shard, property *inverted.Property,
	propsByName map[string]struct{}, bucketNamer, sourceBucketName func(string) string,
	forTargetStrategy bool,
) (bucket *lsmkv.Bucket, bucketName string, skip bool) {
	if _, ok := propsByName[property.Name]; !ok {
		return nil, "", true
	}
	bucketName = bucketNamer(property.Name)
	var swapFallback string
	if forTargetStrategy {
		swapFallback = sourceBucketName(property.Name)
	}
	bucket = resolveDoubleWriteBucket(shard, bucketName, swapFallback)
	return bucket, bucketName, bucket == nil
}
