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
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
)

// This file pins weaviate/0-weaviate-issues#320: after a runtime reindex
// task FINISHES (its swap is committed via CommitSwapOnShard), the live
// post-swap searchable bucket must serve from the CANONICAL on-disk dir
// (property_<name>_searchable), NOT from its ..._ingest_<N> sidecar.
//
// On current code the ingest→canonical dir rename is deferred to the next
// process restart (OnBeforeLsmInit → recoverRuntimeSwapBuckets /
// FinalizeCompletedMigrations). Between task completion and that restart the
// canonical dir does not exist on disk at all: the data sits in the ingest
// sidecar and the in-memory bucket pointer covers the gap. That makes
// "restart before you downgrade" a load-bearing operational precondition for
// on-disk convergence, and it is the root cause behind the weaviate/weaviate#11987
// downgrade gap (v1.38→v1.37: v1.37 opens the canonical name, finds no dir,
// creates an empty bucket, and BM25 on the property silently returns 0 hits).
//
// TestReindexStagedSwap_CommitFinalizesCanonicalDirOnDisk_NoRestart is the
// RED pin: it drives a shard through STAGE (RunSwapOnShard) → COMMIT
// (CommitSwapOnShard) with NO restart in between and asserts that the
// canonical dir exists, the ingest sidecar is gone, and the in-memory bucket
// points at the canonical dir. It fails on current code (the rename is still
// deferred to restart) and will pass once finalization runs at task
// completion. It reuses the fix/220 staged-swap harness verbatim; the sibling
// TestReindexStagedSwap_CommitSingleShardDegenerate drives the identical
// STAGE→COMMIT path but asserts only the in-memory pointer + backup trim, so
// this pin closes the on-disk-durability gap that test leaves open.
func TestReindexStagedSwap_CommitFinalizesCanonicalDirOnDisk_NoRestart(t *testing.T) {
	ctx := testCtx()
	const propName = "title"
	searchBucketName := helpers.BucketSearchableFromPropNameLSM(propName)

	shard, task, _, _ := prepShardToSwapBoundaryRFC220(t, ctx, "CommitFinalize320_"+uuid.NewString()[:8])
	defer shard.Shutdown(ctx)

	// STAGE (pointer flip + OLD→backup rename; nothing destroyed), then COMMIT
	// (OnMigrationComplete + tidied + trim) — the full task-completion path,
	// with NO restart. This is exactly the drive
	// TestReindexStagedSwap_CommitSingleShardDegenerate uses.
	require.NoError(t, task.RunSwapOnShard(ctx, shard))
	require.NoError(t, task.CommitSwapOnShard(ctx, shard))

	lsmPath := shard.pathLSM()
	// SourceBucketName(prop) is the canonical (gen-0, unsuffixed) searchable
	// bucket name; ingestBucketName(prop) is the ..._ingest_<gen> sidecar the
	// live data physically occupies until finalization.
	canonicalDir := filepath.Join(lsmPath, task.strategy.SourceBucketName(propName))
	ingestDir := filepath.Join(lsmPath, task.ingestBucketName(propName))

	// PIN 0-wi#320 (a): the canonical searchable dir must exist on disk after
	// the task FINISHED — a downgrade opens this name and must find the data.
	require.True(t, dirExists(canonicalDir),
		"0-wi#320: canonical searchable dir %q must exist on disk after task FINISHED (no restart); "+
			"currently the ingest→canonical rename is deferred to restart, so a v1.38→v1.37 downgrade "+
			"finds no dir and BM25 silently returns 0 hits", canonicalDir)

	// PIN 0-wi#320 (a): the ingest sidecar must be renamed away.
	require.False(t, dirExists(ingestDir),
		"0-wi#320: ingest sidecar dir %q must be renamed to the canonical name after task FINISHED (no restart)",
		ingestDir)

	// PIN 0-wi#320: the in-memory bucket must serve from the canonical dir, not
	// the ingest sidecar — i.e. the reopen/rename must have rewritten the
	// bucket's on-disk path atomically (like SwapBucketPointer keeps the
	// pointer valid), so live queries keep hitting valid mmap'd segments.
	require.Equal(t, canonicalDir, shard.store.Bucket(searchBucketName).GetDir(),
		"0-wi#320: live searchable bucket must serve from the canonical dir after task FINISHED, "+
			"not the ingest sidecar")
}
