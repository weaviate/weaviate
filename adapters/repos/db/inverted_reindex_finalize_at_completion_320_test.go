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

// TestReindexStagedSwap_CommitFinalizesCanonicalDirOnDisk_NoRestart pins
// 0-wi#320: CommitSwapOnShard must finalize the ingest→canonical rename live
// (root cause of the weaviate/weaviate#11987 downgrade gap).
func TestReindexStagedSwap_CommitFinalizesCanonicalDirOnDisk_NoRestart(t *testing.T) {
	ctx := testCtx()
	const propName = "title"
	searchBucketName := helpers.BucketSearchableFromPropNameLSM(propName)

	shard, task, _, _ := prepShardToSwapBoundaryRFC220(t, ctx, "CommitFinalize320_"+uuid.NewString()[:8])
	defer shard.Shutdown(ctx)

	require.NoError(t, task.RunSwapOnShard(ctx, shard))
	require.NoError(t, task.CommitSwapOnShard(ctx, shard))

	lsmPath := shard.pathLSM()
	canonicalDir := filepath.Join(lsmPath, task.strategy.SourceBucketName(propName))
	ingestDir := filepath.Join(lsmPath, task.ingestBucketName(propName))

	require.True(t, dirExists(canonicalDir),
		"0-wi#320: canonical searchable dir %q must exist on disk after task FINISHED (no restart); "+
			"currently the ingest→canonical rename is deferred to restart, so a v1.38→v1.37 downgrade "+
			"finds no dir and BM25 silently returns 0 hits", canonicalDir)

	require.False(t, dirExists(ingestDir),
		"0-wi#320: ingest sidecar dir %q must be renamed to the canonical name after task FINISHED (no restart)",
		ingestDir)

	require.Equal(t, canonicalDir, shard.store.Bucket(searchBucketName).GetDir(),
		"0-wi#320: live searchable bucket must serve from the canonical dir after task FINISHED, "+
			"not the ingest sidecar")
}

// TestReindexStagedSwap_CrashAfterLiveFinalize_ConvergesOnNextBoot pins
// 0-wi#320: a crash after live finalize but before tracker retirement must
// converge on next boot without resurrecting the ingest sidecar.
func TestReindexStagedSwap_CrashAfterLiveFinalize_ConvergesOnNextBoot(t *testing.T) {
	ctx := testCtx()
	const propName = "title"

	shard, task, _, _ := prepShardToSwapBoundaryRFC220(t, ctx, "CrashAfterFinalize320_"+uuid.NewString()[:8])

	require.NoError(t, task.RunSwapOnShard(ctx, shard))
	require.NoError(t, task.CommitSwapOnShard(ctx, shard))

	lsmPath := shard.pathLSM()
	canonicalDir := filepath.Join(lsmPath, task.strategy.SourceBucketName(propName))
	ingestDir := filepath.Join(lsmPath, task.ingestBucketName(propName))
	migDir := filepath.Join(lsmPath, ".migrations", task.strategy.MigrationDirName())

	require.True(t, dirExists(canonicalDir), "canonical dir promoted live at commit")
	require.False(t, dirExists(ingestDir), "ingest sidecar already renamed away at commit")
	require.True(t, dirExists(migDir), "tracker dir lingers post-commit until the next-boot sweep")

	// Tag so we can confirm the boot sweep leaves this dir untouched.
	tagDirRFC220(t, canonicalDir, "FINALIZED")

	// Simulate a crash + reboot.
	require.NoError(t, shard.Shutdown(ctx))
	FinalizeCompletedMigrationsWithVerdict(lsmPath, task.logger,
		func(payload *ReindexTaskPayload) (bool, bool) { return true, true })

	require.True(t, dirExists(canonicalDir), "canonical dir must survive the boot sweep")
	require.Equal(t, "FINALIZED", readDirTagRFC220(t, canonicalDir),
		"the live-finalized data must be untouched by the boot sweep")
	require.False(t, dirExists(ingestDir), "the boot sweep must not resurrect the ingest sidecar")
	require.False(t, dirExists(migDir), "the tracker dir must be retired by the boot sweep")
}
