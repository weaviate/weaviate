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

package backup

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsImmutableFile(t *testing.T) {
	tests := []struct {
		relPath string
		want    bool
		desc    string
	}{
		// Immutable files (hard-linked and dedup-eligible):
		{"myclass/shard1/lsm/objects/segment-0001.db", true, "LSM segment (immutable)"},
		{"myclass/shard1/lsm/objects/segment-0001.bloom", true, "LSM bloom filter (immutable)"},
		{"myclass/shard1/lsm/objects/segment-0001.cna", true, "LSM count-net-additions (immutable)"},
		{"myclass/shard1/lsm/objects/segment-0001.metadata", true, "LSM combined metadata (immutable)"},
		{"myclass/shard1/main.hnsw.commitlog.d/1709203456.condensed", true, "condensed HNSW commitlog (immutable)"},
		{"myclass/shard1/main.hnsw.snapshot.d/1709203456.snapshot", true, "HNSW snapshot (immutable)"},

		// Mutable files (copied and re-uploaded every incremental):
		{"myclass/shard1/lsm/objects/segment-123.wal", false, "LSM WAL"},
		{"myclass/shard1/main/meta.db", false, "flat index BoltDB (single vector)"},
		{"myclass/shard1/main_custom/meta_custom.db", false, "flat index BoltDB (multi-vector)"},
		{"myclass/shard1/main.hnsw.commitlog.d/1709203456", false, "non-condensed HNSW commitlog"},
		{"myclass/shard1/main.queue.d/chunk-1709203456000000.bin", false, "async indexing queue chunk"},
		{"myclass/shard1/index.db", false, "dynamic vector index BoltDB"},

		// Unknown files default to mutable (safe — copied and re-uploaded):
		{"myclass/shard1/lsm/.migrations/m1", false, "migration file (unknown, copied)"},
		{"myclass/shard1/lsm/objects/segment.db.tmp", false, "tmp file (unknown, copied)"},
		{"myclass/shard1/hashtree_uuid/hashtree-abc.ht", false, "hashtree (unknown, copied)"},
		{"myclass/shard1/some-new-file.bin", false, "unknown file type (copied by default)"},
	}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			got := IsImmutableFile(tc.relPath)
			assert.Equal(t, tc.want, got, "IsImmutableFile(%q)", tc.relPath)
		})
	}
}
