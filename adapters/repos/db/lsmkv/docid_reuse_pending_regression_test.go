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

package lsmkv

import "testing"

// docID-reuse regression suite — pending slots.
//
// This file is the index of the suite's not-yet-landed members. They are kept
// as skipped placeholders (not omitted) so the suite's intended shape and the
// red/green table stay honest about what is covered and what is outstanding.
// Each will be replaced by a runnable, verified-red test and then moved to its
// correct package. See the docid-reuse-jero-audit memory and finding annex.

// TestDocIDReuse_C1_Jero_RealShard is Jero's verbatim C1 repro
// (docid_reuse_repro_integration_test.go, "drop into f03e86a0fd, no production
// changes"; real shard lifecycle + rescore-path interleaving; audit recorded
// RED 30/30, losses 38-63/100, mode 48). It needs the reuse stack
// (#12894/#13029/#12886/#13036), which is not merged into this branch's base
// yet, so it lives verbatim (package db, integrationTest) on the stack-based
// branch tests/docid-reuse-regression-stack (off f03e86a0fd), verified RED
// there. Fold it in here once the stack merges into integration.
func TestDocIDReuse_C1_Jero_RealShard(t *testing.T) {
	t.Skip("MOVED: lives on branch tests/docid-reuse-regression-stack (needs the reuse stack, absent from this base); verified RED there as TestDocIDReuseRepro_DuplicateSecondaryKeyHidesReusedObject. Verbatim source in audit/pr13036-docid-reuse-review.md")
}

// TestDocIDReuse_C2_Jero_Retombstone is Jero's verbatim C2 repro
// (docid_reuse_retombstone_repro_test.go; audit recorded RED 20/20 both modes:
// dirty-freed-id + destroyed-new-owner). Same stack dependency as C1 above; it
// lives verbatim (package hnsw) on tests/docid-reuse-regression-stack, verified
// RED there. Fold it in here once the stack merges into integration.
func TestDocIDReuse_C2_Jero_Retombstone(t *testing.T) {
	t.Skip("MOVED: lives on branch tests/docid-reuse-regression-stack (needs the reuse stack, absent from this base); verified RED there as TestDocIDReuseRepro_StaleRescoreRetombstonesFreedID. Verbatim source in audit/pr13036-docid-reuse-review.md")
}

// TestDocIDReuse_H1_ChunkWatermarkEscape pins H1 (CONFIRMED by the audit): an
// insert queued after the docIdLock is released escapes the delete's chunk
// watermark via chunk rotation, landing the old vector under the new id.
// PENDING: no repro file exists in either audit set (H1 was confirmed by
// analysis); this needs a NEW pinning test. Real home: package db / queue.
func TestDocIDReuse_H1_ChunkWatermarkEscape(t *testing.T) {
	t.Skip("PENDING (H1) — its pinning test lands TDD-style in the gen-tagged fix PR that implements the chunk-watermark check site; see docid-reuse-jero-audit")
}

// TestDocIDReuse_H2_QueueDurableBeforeWAL pins the H2-adjacent hole (audit H2
// PARTIAL): the queue chunk becomes durable before store.WriteWALs, so a
// restart re-frees the id and replay applies dead life. The literal H2 claim
// was refuted (Load() defends it); this is the real hole. PENDING: no repro
// file exists; needs a NEW restart/replay pinning test. Real home: package db.
func TestDocIDReuse_H2_QueueDurableBeforeWAL(t *testing.T) {
	t.Skip("PENDING (H2-adjacent) — its pinning test lands TDD-style in the gen-tagged fix PR that implements the queue-durability-before-WAL check site; see docid-reuse-jero-audit")
}

// TestDocIDReuse_H3_HarvestWindowNoIdentity pins H3 (CONFIRMED): there is no
// identity check at reuse resolution, and the window needs a ~5-min harvest
// cycle to open. PENDING: no repro file exists; needs a NEW (likely
// time/harvest-driven) pinning test. Real home: package db (free list).
func TestDocIDReuse_H3_HarvestWindowNoIdentity(t *testing.T) {
	t.Skip("PENDING (H3) — its pinning test lands TDD-style in the gen-tagged fix PR that implements the reuse-resolution identity check site; see docid-reuse-jero-audit")
}
