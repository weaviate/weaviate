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

// TestCancelAfterMergedGeneration_LeavesBucketsAheadOfSchemaAcrossRestart
// documents what cancelling a reindex actually does to a node that already
// merged its new-format data. It is a description of today's behavior, not
// an endorsement: the end state it pins is a bucket↔schema inversion, and
// it exists so the operator-facing wording in [ReindexGateRemedy] can be
// checked against something other than an assumption.
//
// The sequence, in the order the code runs it:
//
//  1. The shard merged its new-format data into the ingest sidecar. On disk
//     that is a tracker dir carrying merged.mig, plus the sidecar.
//  2. Cancel fires OnTaskCompleted with status CANCELLED. It skips the
//     cluster-wide schema flip, so the schema still says OLD, and calls
//     CleanStalePartialReindexState, which deliberately preserves that
//     generation (wiping it is #10675-shape data loss).
//  3. Restart. FinalizeCompletedMigrations promotes the sidecar to the
//     canonical bucket name and removes the tracker dir.
//
// Result: canonical buckets hold NEW-format data under a schema that says
// OLD, with no marker on disk. FinalizeCompletedMigrations' own godoc
// justifies the promotion on the premise that "the cluster-wide schema flip
// has likely already committed" — on the cancel path it has not.
//
// The window opens at the MERGE, which is what the two rows are for.
// Finalize promotes on merged.mig alone, and merged.mig is written during
// PREPARING, before any shard swaps. The PREPARING row therefore ends in the
// same inversion as the SWAPPING row, on a node that never set
// [Shard.tokenizationOverlay] — so there is no in-memory mask to lose there,
// and making that overlay survive restart would not close this on its own.
// Tracked in https://github.com/weaviate/weaviate/issues/12575 .
func TestCancelAfterMergedGeneration_LeavesBucketsAheadOfSchemaAcrossRestart(t *testing.T) {
	const (
		propName  = "descr"
		indexType = "searchable"
		oldTok    = "word"
		newTok    = "field"

		tracker   = "searchable_retokenize_descr_1"
		sidecar   = "property_descr_searchable__retokenize_ingest_1"
		canonical = "property_descr_searchable"
	)

	cases := []struct {
		name string
		// sentinels the tracker dir carries when cancel arrives.
		sentinels []string
		// whether this node ever set the tokenization overlay. Only the
		// swap sets it, so a cancel at PREPARING has no mask at all.
		hasOverlay bool
	}{
		{
			name:       "cancelled at PREPARING, merged but never swapped",
			sentinels:  []string{"started.mig", "merged.mig"},
			hasOverlay: false,
		},
		{
			name:       "cancelled at SWAPPING, swap committed",
			sentinels:  []string{"started.mig", "merged.mig", "swapped.mig", "tidied.mig"},
			hasOverlay: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "CancelMergedGen_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{propName})
			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)
			lsm := shard.pathLSM()

			// Step 1: the work this node had already done when cancel arrived.
			mkTrackerDir(t, lsm, tracker, tc.sentinels...)
			require.NoError(t, os.WriteFile(
				filepath.Join(lsm, ".migrations", tracker, "properties.mig"),
				[]byte(propName), 0o644))
			mkSidecarDir(t, lsm, sidecar)
			if tc.hasOverlay {
				shard.SetTokenizationOverlay(propName, newTok)
			}

			// The generation is promotion-eligible from the merge onward,
			// which is the evidence the CANCELLED repair guidance is gated on.
			require.True(t, idx.HasPromotableReindexState(propName, indexType),
				"a merged generation is what the next restart promotes")

			// Step 2: the cleanup the CANCELLED path runs on every node.
			require.NoError(t, shard.CleanStalePartialReindexState(ctx, propName, indexType))

			require.True(t, dirExistsAt(t, lsm, sidecar),
				"cancel must not wipe a merged generation's sidecar")
			require.DirExists(t, filepath.Join(lsm, ".migrations", tracker),
				"the merged generation's tracker survives cancel cleanup")

			// Step 3: only a committed swap leaves an in-memory mask. At
			// PREPARING the canonical bucket still holds OLD data, so there
			// is nothing to mask yet.
			wantBefore := oldTok
			if tc.hasOverlay {
				wantBefore = newTok
			}
			require.Equal(t, wantBefore, shard.TokenizationFor(propName, oldTok))

			// Step 4: restart. This runs before bucket loading on every startup.
			FinalizeCompletedMigrations(lsm, logrus.New())

			require.True(t, dirExistsAt(t, lsm, canonical),
				"finalize promotes the cancelled generation to canonical, "+
					"exactly as it would for a task that succeeded")
			require.False(t, dirExistsAt(t, lsm, sidecar),
				"the sidecar was renamed, not copied")
			require.NoDirExists(t, filepath.Join(lsm, ".migrations", tracker),
				"the tracker dir is removed, so nothing on disk records that "+
					"this shard is ahead of the schema")

			// A restarted process starts with an empty overlay and no way to
			// rebuild it: the tracker dir finalize just removed was the last
			// on-disk trace of the migration.
			restarted := &Shard{}
			require.Equal(t, oldTok, restarted.TokenizationFor(propName, oldTok),
				"post-restart the buckets hold %s data under a schema that says %s",
				newTok, oldTok)
		})
	}
}

// TestHasPromotableReindexState pins the predicate the CANCELLED repair
// guidance is gated on: promotion starts at the merge, so a task cancelled
// before that leaves nothing for a restart to promote and nothing for the
// operator to repair.
func TestHasPromotableReindexState(t *testing.T) {
	const (
		propName  = "descr"
		indexType = "searchable"
		tracker   = "searchable_retokenize_descr_1"
	)

	cases := []struct {
		name      string
		sentinels []string
		want      bool
	}{
		{"no tracker dir at all", nil, false},
		{"started, nothing written yet", []string{"started.mig"}, false},
		{"merged during PREPARING", []string{"started.mig", "merged.mig"}, true},
		{"swapped and tidied", []string{"started.mig", "merged.mig", "swapped.mig", "tidied.mig"}, true},
		{"tidied without merged", []string{"started.mig", "tidied.mig"}, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "PromotableState_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{propName})
			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)

			if len(tc.sentinels) > 0 {
				mkTrackerDir(t, shard.pathLSM(), tracker, tc.sentinels...)
			}
			require.Equal(t, tc.want, idx.HasPromotableReindexState(propName, indexType))
		})
	}
}
