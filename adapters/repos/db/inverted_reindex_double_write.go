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

// resolveDoubleWriteBucket resolves the bucket a runtime-reindex double-write
// callback should mirror into. Every strategy MakeAddCallback/MakeDeleteCallback
// MUST use this instead of a bare store.Bucket(sidecarName) lookup.
//
// Why: the double-write callbacks stay registered across the swap —
// [ShardReindexTaskGeneric.disableCallbacks] runs in a defer at the END of
// runtimeSwap, after the per-prop pointer flips. [lsmkv.Store.SwapBucketPointer]
// re-registers the ingest bucket under the canonical (main) name and DELETES
// its sidecar-name entry (store.go: `delete(s.bucketsByName, sourceName)`), so
// from the flip until the callbacks disable, store.Bucket(ingestName) is nil.
// Pre-fix every strategy dereferenced that nil (lsmkv.MustBeExpectedStrategy on
// b.Strategy()): the write goroutine panicked, the REST panic middleware
// recovered without writing a response (client sees an empty 200), and the
// inverted writes of that call were silently lost while the objects bucket kept
// the new value. On this branch the inline callback is suppressed for scope
// props, so the write depends ENTIRELY on this resolution — skipping = loss.
// One of the loss mechanisms behind weaviate/weaviate#11688.
//
// Resolution semantics:
//   - sidecar resolves (common case, pre-swap): mirror into it. If the flip
//     lands right after this lookup, the returned *Bucket IS the bucket that
//     just became main — writing through it is still correct.
//   - sidecar gone + swapFallbackName given (ingest-phase callbacks): resolve
//     the canonical main name, which post-flip denotes the same physical bucket
//     and is never unregistered. Also closes the sub-window where the direct
//     leg resolved main pre-flip (old, discarded bucket) and the mirror leg
//     resolved the sidecar post-flip.
//   - sidecar gone + no fallback (backup-phase callbacks): return nil — the
//     backup is tidied, rollback is impossible, and the canonical bucket
//     already gets direct writes. Callers MUST treat nil as "skip the mirror".
func resolveDoubleWriteBucket(shard *Shard, sidecarName, swapFallbackName string) *lsmkv.Bucket {
	if b := shard.store.Bucket(sidecarName); b != nil {
		return b
	}
	if swapFallbackName == "" {
		return nil
	}
	return shard.store.Bucket(swapFallbackName)
}
