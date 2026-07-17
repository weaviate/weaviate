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
	"fmt"

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
//
// A nil bucket is the caller's single stop signal (`if bucket == nil { return
// err }`): err==nil is a benign no-op (scoped out, or an expected post-swap
// sidecar teardown in the backup phase), while err!=nil means the target phase
// resolved no bucket at all and must fail loudly rather than silently drop the
// write. Folding both no-op and error into `bucket == nil` keeps every call
// site to one guard — the skip bool was always just bucket == nil anyway.
func resolveScopedDoubleWriteBucket(shard *Shard, property *inverted.Property,
	propsByName map[string]struct{}, bucketNamer, sourceBucketName func(string) string,
	forTargetStrategy bool,
) (bucket *lsmkv.Bucket, bucketName string, err error) {
	if _, ok := propsByName[property.Name]; !ok {
		return nil, "", nil
	}
	bucketName = bucketNamer(property.Name)
	var swapFallback string
	if forTargetStrategy {
		swapFallback = sourceBucketName(property.Name)
	}
	if bucket = resolveDoubleWriteBucket(shard, bucketName, swapFallback); bucket != nil {
		return bucket, bucketName, nil
	}
	// Backup phase: a gone sidecar is expected post-swap teardown, so no-op.
	if !forTargetStrategy {
		return nil, bucketName, nil
	}
	// Target phase: this state is unreachable through a healthy swap, so
	// error loudly instead of silently dropping the write.
	return nil, bucketName, fmt.Errorf(
		"double-write target resolved no bucket for property %q: neither ingest sidecar %q nor canonical fallback %q exists",
		property.Name, bucketName, swapFallback)
}

// withRetokenizeDoubleWrite is the shared body of both retokenize strategies'
// add and delete callbacks. It index-scopes the property, resolves the scoped
// bucket, selects the countables (re-tokenizing RawValues with the target
// tokenization on the target phase, mirroring live property.Items otherwise),
// then hands the resolved bucket and items to apply. Only that per-item op
// differs between add/delete and between the searchable/filterable strategies,
// so factoring the rest here keeps the four callbacks from diverging or
// duplicating. analyzer may be nil when forTargetStrategy is false: it is only
// dereferenced on the target-and-RawValues path.
func withRetokenizeDoubleWrite(shard *Shard, property *inverted.Property, hasIndex bool,
	propsByName map[string]struct{}, bucketNamer, sourceBucketName func(string) string,
	forTargetStrategy bool, analyzer *inverted.Analyzer, targetTokenization string,
	apply func(bucket *lsmkv.Bucket, bucketName string, items []inverted.Countable) error,
) error {
	if !hasIndex {
		return nil
	}
	bucket, bucketName, err := resolveScopedDoubleWriteBucket(shard, property,
		propsByName, bucketNamer, sourceBucketName, forTargetStrategy)
	if bucket == nil {
		return err
	}
	items := property.Items
	if forTargetStrategy && len(property.RawValues) > 0 {
		items = analyzer.TextArray(targetTokenization, property.RawValues, property.Name, nil)
	}
	return apply(bucket, bucketName, items)
}
