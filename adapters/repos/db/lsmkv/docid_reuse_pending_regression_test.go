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
// RED 30/30, losses 38-63/100, mode 48). PENDING: the verbatim source lives in
// Jero's review markdown (~/Downloads/pr13036-docid-reuse-review.md), which is
// currently unreadable (macOS Downloads TCC blocks terminal + tool access).
// Real home: package db (integrationTest), not lsmkv.
func TestDocIDReuse_C1_Jero_RealShard(t *testing.T) {
	t.Skip("PENDING verbatim source (C1) — awaiting readable copy of Jero's review md; see docid-reuse-jero-audit")
}

// TestDocIDReuse_C2_Jero_Retombstone is Jero's verbatim C2 repro
// (docid_reuse_retombstone_repro_test.go; audit recorded RED 20/20 both modes:
// dirty-freed-id + destroyed-new-owner). PENDING: same blocked source as above.
// Real home: package hnsw.
func TestDocIDReuse_C2_Jero_Retombstone(t *testing.T) {
	t.Skip("PENDING verbatim source (C2) — awaiting readable copy of Jero's review md; see docid-reuse-jero-audit")
}

// TestDocIDReuse_H1_ChunkWatermarkEscape pins H1 (CONFIRMED by the audit): an
// insert queued after the docIdLock is released escapes the delete's chunk
// watermark via chunk rotation, landing the old vector under the new id.
// PENDING: no repro file exists in either audit set (H1 was confirmed by
// analysis); this needs a NEW pinning test. Real home: package db / queue.
func TestDocIDReuse_H1_ChunkWatermarkEscape(t *testing.T) {
	t.Skip("PENDING new repro (H1) — no verbatim source exists; confirmed by analysis only; see docid-reuse-jero-audit")
}

// TestDocIDReuse_H2_QueueDurableBeforeWAL pins the H2-adjacent hole (audit H2
// PARTIAL): the queue chunk becomes durable before store.WriteWALs, so a
// restart re-frees the id and replay applies dead life. The literal H2 claim
// was refuted (Load() defends it); this is the real hole. PENDING: no repro
// file exists; needs a NEW restart/replay pinning test. Real home: package db.
func TestDocIDReuse_H2_QueueDurableBeforeWAL(t *testing.T) {
	t.Skip("PENDING new repro (H2-adjacent) — no verbatim source exists; confirmed by analysis only; see docid-reuse-jero-audit")
}

// TestDocIDReuse_H3_HarvestWindowNoIdentity pins H3 (CONFIRMED): there is no
// identity check at reuse resolution, and the window needs a ~5-min harvest
// cycle to open. PENDING: no repro file exists; needs a NEW (likely
// time/harvest-driven) pinning test. Real home: package db (free list).
func TestDocIDReuse_H3_HarvestWindowNoIdentity(t *testing.T) {
	t.Skip("PENDING new repro (H3) — no verbatim source exists; confirmed by analysis only; see docid-reuse-jero-audit")
}
