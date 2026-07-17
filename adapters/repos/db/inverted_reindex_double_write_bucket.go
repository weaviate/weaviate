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

import "github.com/weaviate/weaviate/adapters/repos/db/lsmkv"

// resolveDoubleWriteBucket looks up the destination bucket for a live
// double-write callback (registerDoubleWriteCallbacks / MakeAddCallback /
// MakeDeleteCallback) fired while a reindex migration on this shard may be
// mid-swap.
//
// The callback is wired against bucketName (typically the per-generation
// ingest/backup bucket name - see ShardReindexTaskGeneric.ingestBucketName /
// backupBucketName) and stays registered until [ShardReindexTaskGeneric.
// disableCallbacks] fires, which is deferred to the very end of
// [ShardReindexTaskGeneric.runtimeSwap] - AFTER every property's
// Store.SwapBucketPointer call (Phase 2a), the old-bucket shutdown+rename
// loop (Phase 2b), markSwapped, markTidied, OnMigrationComplete, and trim.
// For a multi-property migration that window is milliseconds-to-seconds,
// not "atomic".
//
// Store.SwapBucketPointer(canonicalName, bucketName) re-registers
// bucketsByName[canonicalName] = <the bucket bucketName used to resolve to>
// and deletes bucketsByName[bucketName] inside ONE bucketAccessLock critical
// section (see store.go). So there is never a window where BOTH names miss:
// once bucketName stops resolving, the swap for this property has already
// committed and the exact same bucket - with the write's destination
// already correct - is reachable at canonicalName instead. Falling back
// keeps the posting flowing to the right place. The alternatives are worse:
// skip-with-no-error silently drops the write (data loss), and the pre-fix
// behavior of using bucketName unconditionally panics on the nil bucket
// (GH weaviate/weaviate#12206, CI run 29553875773 - orphan stack frame at
// inverted_reindex_strategy_rangeable.go:129 inside a shard's swap
// sequence, recovered per-request by net/http, surfaced to the replicating
// coordinator as EOF -> "cannot reach enough replicas").
//
// A nil return means neither name resolves: not a swap race (that always
// leaves exactly one name valid) but a genuine bug - the bucket was never
// loaded, or the store has closed. Callers MUST treat a nil return as an
// error, not as "nothing to do".
func resolveDoubleWriteBucket(store *lsmkv.Store, bucketName, canonicalName string) *lsmkv.Bucket {
	if bucket := store.Bucket(bucketName); bucket != nil {
		return bucket
	}
	if canonicalName == bucketName {
		return nil
	}
	return store.Bucket(canonicalName)
}
