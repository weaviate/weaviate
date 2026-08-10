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

// TestCancelAfterMergedGeneration_LeavesBucketsAheadOfSchemaAcrossRestart pins
// that cancelling once a generation has merged (PREPARING or later) leaves
// canonical buckets NEW-tokenized under an unflipped OLD schema after
// restart: cancel skips the schema flip but FinalizeCompletedMigrations
// still promotes on merged.mig alone. Tracked at
// weaviate/weaviate#12575.
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

			// State already on disk when cancel arrives.
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

			require.NoError(t, shard.CleanStalePartialReindexState(ctx, propName, indexType))

			require.True(t, dirExistsAt(t, lsm, sidecar),
				"cancel must not wipe a merged generation's sidecar")
			require.DirExists(t, filepath.Join(lsm, ".migrations", tracker),
				"the merged generation's tracker survives cancel cleanup")

			// Only a committed swap leaves an in-memory mask; at PREPARING
			// the canonical bucket still holds OLD data.
			wantBefore := oldTok
			if tc.hasOverlay {
				wantBefore = newTok
			}
			require.Equal(t, wantBefore, shard.TokenizationFor(propName, oldTok))

			// Restart: FinalizeCompletedMigrations runs before bucket loading.
			FinalizeCompletedMigrations(lsm, logrus.New())

			require.True(t, dirExistsAt(t, lsm, canonical),
				"finalize promotes the cancelled generation to canonical, "+
					"exactly as it would for a task that succeeded")
			require.False(t, dirExistsAt(t, lsm, sidecar),
				"the sidecar was renamed, not copied")
			require.NoDirExists(t, filepath.Join(lsm, ".migrations", tracker),
				"the tracker dir is removed, so nothing on disk records that "+
					"this shard is ahead of the schema — a restarted process "+
					"has no way to rebuild the overlay, and the buckets hold "+
					"%s data under a schema that says %s", newTok, oldTok)
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
