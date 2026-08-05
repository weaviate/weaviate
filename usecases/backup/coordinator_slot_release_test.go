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
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/usecases/config"
)

// TestCoordinatorRestoreReleaseOnlyClearsItsOwnSlot pins the release half of
// the slot-ownership invariant: the restore goroutine gives the slot back only
// while it still holds it. A newer restore can take the slot over while the
// old goroutine is still finishing (cancellation frees it, then another
// restore claims it), and clearing that claim would report the node idle to
// [NodeActivityProbe] while a restore is live.
func TestCoordinatorRestoreReleaseOnlyClearsItsOwnSlot(t *testing.T) {
	t.Parallel()
	var (
		backendName = "s3"
		backupID    = "1"
		ctx         = context.Background()
		anyArg      = mock.Anything
		nodes       = []string{"N1", "N2"}
		classes     = []string{"C1"}
		cresp       = &CanCommitResponse{Method: OpRestore, ID: backupID, Timeout: 1}
		sReq        = &StatusRequest{OpRestore, backupID, backendName, "", "", ""}
		sresp       = &StatusResponse{Status: backup.Success, ID: backupID, Method: OpRestore}
	)

	tests := []struct {
		name string
		// steal reproduces a newer restore taking the slot over while the
		// goroutine of the previous one is still in its final PutMeta.
		steal      bool
		wantSlotID string
	}{
		{name: "slot still held by this restore", steal: false, wantSlotID: ""},
		{name: "slot taken over by a newer restore", steal: true, wantSlotID: "live-restore"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fc := newFakeCoordinator(newFakeNodeResolver(nodes))
			desc := &backup.DistributedBackupDescriptor{
				ID:            backupID,
				Status:        backup.Success,
				Version:       Version,
				ServerVersion: config.ServerVersion,
				Nodes: map[string]*backup.NodeDescriptor{
					nodes[0]: {Classes: classes, Status: backup.Success},
					nodes[1]: {Classes: classes, Status: backup.Success},
				},
			}
			for _, node := range nodes {
				fc.client.On("CanCommit", anyArg, node, anyArg).Return(cresp, nil)
				fc.client.On("Commit", anyArg, node, sReq).Return(nil)
				fc.client.On("Status", anyArg, node, sReq).Return(sresp, nil)
			}
			fc.backend.On("HomeDir", anyArg, anyArg, backupID).Return("bucket/" + backupID)
			fc.backend.On("GetObject", ctx, backupID, GlobalRestoreFile).Return(nil, backup.ErrNotFound{})

			var (
				puts    atomic.Int32
				blocked = make(chan struct{})
				release = make(chan struct{})
			)
			// The third and last PutMeta is the goroutine's final round trip, so
			// holding it there parks the goroutine right before it releases.
			fc.backend.On("PutObject", anyArg, backupID, GlobalRestoreFile, anyArg).
				Return(nil).
				Run(func(mock.Arguments) {
					if puts.Add(1) != 3 {
						return
					}
					close(blocked)
					<-release
				})

			c := fc.coordinator()
			req := newReq(nil, backendName, backupID)
			store := coordStore{objectStore{fc.backend, backupID, "", "", ""}}
			require.NoError(t, c.Restore(ctx, store, &req, desc, nil))

			select {
			case <-blocked:
			case <-time.After(20 * time.Second):
				t.Fatal("restore goroutine never reached its final PutMeta")
			}

			if tc.steal {
				c.lastOp.set(backup.Cancelled)
				require.True(t, c.lastOp.resetIfCancelled(backupID))
				require.Empty(t, c.lastOp.renew("live-restore", "path", "", ""))
			}
			close(release)

			probe := NewNodeActivityProbe(nil)
			probe.AttachScheduler(&Scheduler{restorer: c})
			if tc.steal {
				require.Never(t, func() bool {
					return c.lastOp.get().ID != tc.wantSlotID
				}, time.Second, 10*time.Millisecond,
					"the finished restore released a slot a newer restore owns")
				require.Equal(t,
					NodeActivity{Busy: true, Kind: NodeActivityKindRestore, ID: tc.wantSlotID},
					probe.Activity())
				return
			}
			require.Eventually(t, func() bool {
				return c.lastOp.get().ID == tc.wantSlotID
			}, 10*time.Second, 10*time.Millisecond,
				"the finished restore never released its own slot")
			require.Equal(t, NodeActivity{}, probe.Activity())
		})
	}
}
