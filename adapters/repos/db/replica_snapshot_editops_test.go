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
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/editops"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

const editOpsSnapshotOpID = "00000000-0000-0000-0000-0000000000e0"

// armObjectsEditOp flushes the objects bucket so the write lands in a segment,
// then arms an edit op over it — the state a drop-vector strip is in between
// arming and the cleanup draining its pending rows.
func armObjectsEditOp(t *testing.T, shard *Shard) *lsmkv.Bucket {
	t.Helper()
	bucket := shard.store.Bucket(helpers.ObjectsBucketLSM)
	require.NotNil(t, bucket)
	require.True(t, bucket.HasEditOps(),
		"the objects bucket must carry the edit-ops sidecar or this test proves nothing")

	require.NoError(t, bucket.FlushAndSwitch())
	require.NoError(t, bucket.RegisterEditOp(editOpsSnapshotOpID, editops.OpDescriptor{
		Type:      editops.OpTypeRemoveTargetVectors,
		Targets:   []string{"vec"},
		CreatedAt: 1,
	}))

	pending, err := bucket.EditOpPending(editOpsSnapshotOpID)
	require.NoError(t, err)
	require.NotEmpty(t, pending, "arming must leave pending rows, else the veto has nothing to see")
	return bucket
}

// TestReplicaSnapshotDefersWhileEditOpsPending pins the veto that keeps replica
// movement from outrunning an in-flight drop-vector strip. The edit-ops sidecar
// is deliberately excluded from the copied file list, so a shard moved with
// pending rows lands its unstripped bytes on the target with nothing recording
// that they still need stripping.
//
// It also pins the half that makes the deferral survivable: the shard must be
// left RESUMED. HaltForTransfer suspends the very cleanup cycle that drains the
// rows, so a deferral that returned while still holding the halt could never
// reach a state where a retry succeeds.
func TestReplicaSnapshotDefersWhileEditOpsPending(t *testing.T) {
	for _, tc := range []struct {
		name            string
		forceNoHardlink bool
	}{
		{name: "hardlink mode", forceNoHardlink: false},
		{name: "fallback halt-for-duration mode", forceNoHardlink: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.forceNoHardlink {
				t.Setenv("WEAVIATE_TEST_FORCE_NO_HARDLINK", "true")
			}
			index, shard := newSharedHaltTestShard(t)
			ctx := context.Background()

			putSharedHaltObject(t, index, strfmt.UUID("40d3be3e-2ecc-49c8-b37c-d8983164848b"), 0)

			// No edit op armed: the snapshot proceeds and produces a file list.
			files, err := index.IncomingCreateReplicaSnapshot(ctx, "shard1", editOpsSnapshotOpID+"-clean")
			require.NoError(t, err, "a shard with no edit-op rows must snapshot normally")
			require.NotEmpty(t, files)
			require.NoError(t, index.IncomingReleaseReplicaSnapshot(ctx, editOpsSnapshotOpID+"-clean"))

			bucket := armObjectsEditOp(t, shard)

			_, err = index.IncomingCreateReplicaSnapshot(ctx, "shard1", editOpsSnapshotOpID+"-armed")
			require.Error(t, err)
			require.Contains(t, err.Error(), "in-flight drop-vector strip")
			// Without the sentinel the refusal is codes.Internal, the consumer's
			// isShardBusyError misses it, and every attempt registers against
			// MaxErrors until the FSM CANCELS the movement. The contract is what
			// makes this a deferral rather than a kill.
			require.ErrorIs(t, err, enterrors.ErrShardBusyStructuralOp,
				"the refusal must defer the movement, not burn its error budget")

			halted := shard.haltForTransferCount.Load()
			require.Zero(t, halted,
				"the deferral must leave the shard resumed: the halt suspends the cleanup that drains the rows, "+
					"so holding it across the refusal would make a retry unable to ever succeed")

			// The rows are what a retry waits on, and they are still there.
			deleted, pending, _, err := bucket.DeleteEditOpIfDrained(editOpsSnapshotOpID)
			require.NoError(t, err)
			require.False(t, deleted)
			require.Positive(t, pending, "the strip is still owed these segments")
		})
	}
}

// TestReplicaSnapshotDefersOnEditOpsReadError pins the direction of the
// failure: an unreadable sidecar must defer the snapshot, never wave it
// through. Proceeding on an unknown answer is the one outcome that loses data —
// the shard would move with rows nobody could see.
func TestReplicaSnapshotDefersOnEditOpsReadError(t *testing.T) {
	index, shard := newSharedHaltTestShard(t)
	ctx := context.Background()

	putSharedHaltObject(t, index, strfmt.UUID("40d3be3e-2ecc-49c8-b37c-d8983164848b"), 0)
	bucket := armObjectsEditOp(t, shard)

	// Close the sidecar, then corrupt the file so the next open fails.
	require.NoError(t, bucket.Shutdown(ctx))
	var sidecar string
	require.NoError(t, filepath.WalkDir(index.Config.RootPath,
		func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() && d.Name() == "segment_edit_ops.db.bolt" {
				sidecar = path
			}
			return nil
		}))
	require.NotEmpty(t, sidecar, "arming must have created the edit-ops sidecar")
	require.NoError(t, os.WriteFile(sidecar, []byte("not a bolt database"), 0o600))

	_, err := index.IncomingCreateReplicaSnapshot(ctx, "shard1", editOpsSnapshotOpID+"-snap3")
	require.Error(t, err, "an unreadable sidecar must defer, not proceed")
	require.ErrorIs(t, err, enterrors.ErrShardBusyStructuralOp,
		"an unknown answer must defer the movement, not cancel it")
	require.Contains(t, err.Error(), "inspect edit-ops",
		"the deferral must come from the edit-ops probe itself, not some unrelated failure")
}
