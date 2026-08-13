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

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// Pins that cancelling after a merge leaves buckets NEW-tokenized under an
// unflipped OLD schema after restart (weaviate/weaviate#12575).
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
		// hasOverlay: only the swap sets the tokenization overlay.
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
			shard, idx := newReindexTestShard(t, "CancelMergedGen", propName)
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
			require.True(t, idx.anyPromotableReindexState(propName, indexType, ReindexTypeChangeTokenization, nil),
				"a merged generation is what the next restart promotes")

			_, cleanErr := shard.CleanStalePartialReindexState(ctx, propName, indexType)
			require.NoError(t, cleanErr)

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

// Pins that promotion starts at the merge: a task cancelled before that
// leaves nothing for a restart to promote or the operator to repair.
func TestHasPromotableReindexState(t *testing.T) {
	const (
		propName  = "descr"
		indexType = "searchable"
		tracker   = "searchable_retokenize_descr_1"
	)

	cases := []struct {
		name string
		// tracker dir to create; defaults to the per-property one.
		tracker string
		// migrationType being asked about; defaults to change-tokenization,
		// which is what the per-property tracker above belongs to.
		migrationType ReindexMigrationType
		sentinels     []string
		want          bool
	}{
		{name: "no tracker dir at all", want: false},
		{name: "started, nothing written yet", sentinels: []string{"started.mig"}, want: false},
		{name: "merged during PREPARING", sentinels: []string{"started.mig", "merged.mig"}, want: true},
		{
			name:      "swapped and tidied",
			sentinels: []string{"started.mig", "merged.mig", "swapped.mig", "tidied.mig"},
			want:      true,
		},
		{name: "tidied without merged", sentinels: []string{"started.mig", "tidied.mig"}, want: true},
		{
			// change-algorithm's tracker is class-level, not <prefix>_<prop>.
			name:          "merged in the class-level blockmax tracker",
			tracker:       MigrationDirSearchableMapToBlockmax + genSuffix(1),
			migrationType: ReindexTypeChangeAlgorithm,
			sentinels:     []string{"started.mig", "merged.mig"},
			want:          true,
		},
		{
			// The class-level dir outlives the migration that wrote it, so a
			// finished change-algorithm must not answer for a later
			// retokenize that left nothing of its own behind.
			name:          "another type's class-level tracker is not this task's evidence",
			tracker:       MigrationDirSearchableMapToBlockmax + genSuffix(1),
			migrationType: ReindexTypeChangeTokenization,
			sentinels:     []string{"started.mig", "merged.mig"},
			want:          false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			shard, idx := newReindexTestShard(t, "PromotableState", propName)

			if len(tc.sentinels) > 0 {
				dir := tc.tracker
				if dir == "" {
					dir = tracker
				}
				mkTrackerDir(t, shard.pathLSM(), dir, tc.sentinels...)
			}
			mt := tc.migrationType
			if mt == "" {
				mt = ReindexTypeChangeTokenization
			}
			require.Equal(t, tc.want, idx.anyPromotableReindexState(propName, indexType, mt, nil))
		})
	}
}

// Pins the one fail-open case: no local index means no shard to promote,
// so true here would emit repair guidance for state on other nodes.
func TestDBHasPromotableReindexStateWithoutLocalIndex(t *testing.T) {
	db := &DB{indices: map[string]*Index{}}

	require.False(t, db.anyPromotableReindexState("C", "descr", "searchable", ReindexTypeChangeTokenization, nil))
}
