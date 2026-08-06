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
	"sync"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// doubleWriteScope is the shared input every strategy's double-write
// callbacks need to resolve the bucket a mirror write goes into. Built once
// per registration by
// [ShardReindexTaskGeneric.registerDoubleWriteCallbacks] and captured by
// the callback closures.
type doubleWriteScope struct {
	// bucketNamer maps a property name to the sidecar bucket this pair of
	// callbacks mirrors into.
	bucketNamer func(string) string

	// sourceBucketName maps a property name to the canonical bucket the
	// sidecar takes over at the swap.
	sourceBucketName func(string) string

	// propsByName limits the callbacks to the migration's properties.
	propsByName map[string]struct{}

	// forTargetStrategy is true for the ingest-phase pair (mirrors
	// target-encoded data, and arms the canonical-name fallback) and false
	// for the backup-phase pair.
	forTargetStrategy bool

	// swapStarted reports whether this task's runtimeSwap has begun. A nil
	// func reads as "started": the fallback it gates exists to stop the
	// lost writes of weaviate/weaviate#11688, and an unwired construction
	// path losing writes is worse than one mirroring too eagerly.
	swapStarted func() bool

	// warnOnce bounds the skipped-mirror warning to one line per
	// registration. The condition is a property of the task, not of the
	// individual write, so repeating it per write adds nothing.
	warnOnce *sync.Once
}

func (s doubleWriteScope) hasSwapStarted() bool {
	return s.swapStarted == nil || s.swapStarted()
}

// resolveDoubleWriteBucket resolves the bucket a double-write callback should
// mirror into. Callers MUST use this instead of a bare store.Bucket(sidecarName)
// lookup: callbacks stay armed until disableCallbacks runs at the end of
// runtimeSwap, but SwapBucketPointer deletes the sidecar-name entry at the
// flip, so a bare lookup can resolve nil and panic (weaviate/weaviate#11688).
//
//   - sidecar resolves (pre-swap): mirror into it.
//   - sidecar gone + swapFallbackName set + this task's swap has started
//     (ingest phase): resolve the canonical name instead — post-flip it
//     denotes the same physical bucket.
//   - sidecar gone + no swap started: nil means skip the mirror. The sidecar
//     name disappears for exactly two reasons — SwapBucketPointer and
//     cleanup's ShutdownBucket — so with no swap on this task the sidecar
//     was torn down by cleanup and the task is dead. Falling back there
//     writes target-encoded postings into a canonical bucket the schema
//     still reads as source-encoded.
//   - sidecar gone + no fallback (backup phase): nil means skip the mirror.
func resolveDoubleWriteBucket(shard *Shard, sidecarName, swapFallbackName string,
	swapStarted bool,
) *lsmkv.Bucket {
	if b := shard.store.Bucket(sidecarName); b != nil {
		return b
	}
	if swapFallbackName == "" || !swapStarted {
		return nil
	}
	return shard.store.Bucket(swapFallbackName)
}

// resolveScopedDoubleWriteBucket is the shared prologue for every strategy's
// double-write callback: scope-filters the property, then resolves the bucket
// via [resolveDoubleWriteBucket]. skip=true means the callback must no-op.
func resolveScopedDoubleWriteBucket(shard *Shard, property *inverted.Property,
	scope doubleWriteScope,
) (bucket *lsmkv.Bucket, bucketName string, skip bool) {
	if _, ok := scope.propsByName[property.Name]; !ok {
		return nil, "", true
	}
	bucketName = scope.bucketNamer(property.Name)
	var swapFallback string
	if scope.forTargetStrategy {
		swapFallback = scope.sourceBucketName(property.Name)
	}
	swapStarted := scope.hasSwapStarted()
	bucket = resolveDoubleWriteBucket(shard, bucketName, swapFallback, swapStarted)
	if bucket == nil && swapFallback != "" && !swapStarted && scope.warnOnce != nil {
		scope.warnOnce.Do(func() {
			shard.index.logger.WithField("shard", shard.Name()).
				WithField("sidecar_bucket", bucketName).
				Warn("reindex double-write: the sidecar bucket is gone and no swap ran on this task; " +
					"skipping the mirror instead of writing target-encoded data into the canonical bucket")
		})
	}
	return bucket, bucketName, bucket == nil
}
