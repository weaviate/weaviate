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
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestCancelAfterCommittedSwap_LeavesBucketsAheadOfSchemaAcrossRestart
// documents what cancelling a reindex actually does to a node that already
// committed its bucket swap. It is a description of today's behavior, not
// an endorsement: the end state it pins is a bucket↔schema inversion, and
// it exists so the operator-facing wording in [ReindexGateRemedy] can be
// checked against something other than an assumption.
//
// The sequence, in the order the code runs it:
//
//  1. The shard's swap committed. On disk that is a tracker dir carrying
//     merged.mig + tidied.mig plus its ingest sidecar, which holds the
//     data under the NEW tokenization.
//  2. Cancel fires OnTaskCompleted with status CANCELLED. It skips the
//     cluster-wide schema flip, so the schema still says OLD, and calls
//     CleanStalePartialReindexState, which deliberately preserves the
//     committed swap (wiping it is #10675-shape data loss).
//  3. Queries stay correct only because Shard.tokenizationOverlay says NEW.
//     That map is in-memory.
//  4. Restart. FinalizeCompletedMigrations promotes the sidecar to the
//     canonical bucket name, removes the tracker dir, and nothing
//     reconstructs the overlay — there is no on-disk state left to
//     reconstruct it from.
//
// Result: canonical buckets hold NEW-format data under a schema that says
// OLD, with no in-memory mask and no marker on disk. Note that
// FinalizeCompletedMigrations' own godoc justifies the promotion on the
// premise that "the cluster-wide schema flip has likely already
// committed" — on the cancel path it has not.
//
// The durable fix (persist the overlay, or flip the schema when a cancel
// finds committed swaps) is a design change in the reindex layer, not a
// wording change, and is tracked separately.
func TestCancelAfterCommittedSwap_LeavesBucketsAheadOfSchemaAcrossRestart(t *testing.T) {
	const (
		propName  = "descr"
		indexType = "searchable"
		oldTok    = "word"
		newTok    = "field"

		tracker   = "searchable_retokenize_descr_1"
		sidecar   = "property_descr_searchable__retokenize_ingest_1"
		canonical = "property_descr_searchable"
	)

	ctx := testCtx()
	className := "CancelCommittedSwap_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{propName})
	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)
	lsm := shard.pathLSM()

	// Step 1: a swap this node already committed.
	mkTrackerDir(t, lsm, tracker,
		"started.mig", "merged.mig", "swapped.mig", "tidied.mig")
	require.NoError(t, os.WriteFile(
		filepath.Join(lsm, ".migrations", tracker, "properties.mig"),
		[]byte(propName), 0o644))
	mkSidecarDir(t, lsm, sidecar)
	shard.SetTokenizationOverlay(propName, newTok)

	// Step 2: the cleanup the CANCELLED path runs on every node.
	require.NoError(t, shard.CleanStalePartialReindexState(ctx, propName, indexType))

	require.True(t, dirExistsAt(t, lsm, sidecar),
		"cancel must not wipe a committed swap's sidecar — it backs the live bucket pointer")
	require.DirExists(t, filepath.Join(lsm, ".migrations", tracker),
		"the committed swap's tracker survives cancel cleanup")

	// Step 3: before restart the in-memory overlay masks the mismatch, so
	// queries on this node are still correct.
	require.Equal(t, newTok, shard.TokenizationFor(propName, oldTok),
		"the overlay is the only thing keeping queries correct after cancel")

	// Step 4: restart. This runs before bucket loading on every startup.
	FinalizeCompletedMigrations(lsm, logrus.New())

	require.True(t, dirExistsAt(t, lsm, canonical),
		"finalize promotes the cancelled-but-committed sidecar to canonical, "+
			"exactly as it would for a task that succeeded")
	require.False(t, dirExistsAt(t, lsm, sidecar),
		"the sidecar was renamed, not copied")
	require.NoDirExists(t, filepath.Join(lsm, ".migrations", tracker),
		"the tracker dir is removed, so nothing on disk records that this "+
			"shard is ahead of the schema")

	// A restarted process starts with an empty overlay and no way to
	// rebuild it: the tracker dir finalize just removed was the last
	// on-disk trace of the swap.
	restarted := &Shard{}
	require.Equal(t, oldTok, restarted.TokenizationFor(propName, oldTok),
		"post-restart the mask is gone while the buckets still hold %s data "+
			"under a schema that says %s", newTok, oldTok)
}
