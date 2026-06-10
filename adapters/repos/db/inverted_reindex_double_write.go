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

// resolveDoubleWriteBucket resolves the bucket a runtime-reindex
// double-write callback should mirror into. Every
// [MigrationStrategy.MakeAddCallback] / MakeDeleteCallback implementation
// MUST use this instead of a bare store.Bucket(sidecarName) lookup.
//
// Why: the double-write callbacks stay registered across [runtimeSwap]'s
// Phase 2a (their disable runs in a defer at the END of runtimeSwap, after
// OnMigrationComplete). [lsmkv.Store.SwapBucketPointer] re-registers the
// ingest bucket under the canonical (main) name and DELETES its
// sidecar-name entry — so from the pointer flip until the callbacks are
// disabled, store.Bucket(ingestName) returns nil for a live write that is
// concurrently being mirrored. Pre-fix, every strategy dereferenced that
// nil bucket: the write goroutine panicked inside the callback
// (lsmkv.MustBeExpectedStrategy on a nil *Bucket), the REST panic
// middleware swallowed the panic WITHOUT writing a response (the client
// sees an empty 200), and the object update was silently half-applied —
// the object bucket had the new value while every inverted write of that
// call was lost. One of the loss mechanisms behind
// weaviate/weaviate#11688.
//
// Resolution semantics:
//
//   - sidecar name resolves (the common case, pre-swap): mirror into the
//     sidecar (ingest/backup) bucket. If the pointer flip lands right
//     after this lookup, writing through the returned *Bucket is still
//     correct — it IS the bucket that just became main.
//   - sidecar gone + swapFallbackName given (ingest-phase callbacks):
//     resolve the canonical main name instead. Post-flip that is the same
//     physical bucket the ingest name used to denote, so the mirror write
//     lands in the surviving bucket. The canonical name is never
//     unregistered, so this lookup cannot miss. Without the fallback, a
//     write whose direct leg resolved the main name BEFORE the flip (old,
//     to-be-discarded bucket) and whose mirror leg resolved the sidecar
//     AFTER the flip would lose the value entirely.
//   - sidecar gone + no fallback (backup-phase callbacks): return nil —
//     the backup bucket has been tidied; rollback is no longer possible
//     and the canonical bucket already receives direct writes (the schema
//     flag is flipped by then). Callers MUST treat nil as "skip the
//     mirror write", never dereference.
func resolveDoubleWriteBucket(shard *Shard, sidecarName, swapFallbackName string) *lsmkv.Bucket {
	if b := shard.store.Bucket(sidecarName); b != nil {
		return b
	}
	if swapFallbackName == "" {
		return nil
	}
	return shard.store.Bucket(swapFallbackName)
}
