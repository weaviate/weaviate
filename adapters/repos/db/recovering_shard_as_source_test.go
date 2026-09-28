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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/replication/changelog"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	esync "github.com/weaviate/weaviate/entities/sync"
)

func TestRecoveringShardRefusesToServeAsSource(t *testing.T) {
	ctx := context.Background()
	calls := []struct {
		name           string
		call           func(idx *Index) error
		wantRecovering bool
	}{
		{
			name:           "start change capture",
			call:           func(idx *Index) error { return idx.IncomingStartChangeCapture(ctx, "S", "7") },
			wantRecovering: true,
		},
		{
			name: "create replica snapshot",
			call: func(idx *Index) error {
				_, err := idx.IncomingCreateReplicaSnapshot(ctx, "S", "7")
				return err
			},
			wantRecovering: true,
		},
		{
			name: "snapshot change-log LSN",
			call: func(idx *Index) error {
				_, err := idx.IncomingSnapshotChangeLogLSN(ctx, "S", "7")
				return err
			},
		},
		{
			name: "finalize change log",
			call: func(idx *Index) error {
				_, err := idx.IncomingFinalizeChangeLog(ctx, "S", "7")
				return err
			},
		},
		{
			name: "tail change log",
			call: func(idx *Index) error {
				_, err := idx.IncomingGetChangeLog(ctx, "S", "7", 10)
				return err
			},
		},
	}
	for _, tc := range calls {
		t.Run(tc.name, func(t *testing.T) {
			idx := newRecoveringIndex(t)
			idx.replicaSnapshotOpLocks = esync.NewKeyRWLocker()
			rec, ok := idx.shards.Load("S").(*RecoveringShard)
			require.True(t, ok)

			err := tc.call(idx)
			require.Error(t, err)
			require.NotContains(t, err.Error(), changelog.ErrMsgNoActiveLog)
			require.NotContains(t, err.Error(), changelog.ErrMsgNoActiveChangeCaptureLog)
			if tc.wantRecovering {
				require.True(t, enterrors.IsShardRecovering(err), "got %v", err)
			}
			require.NoDirExists(t, shardPath(idx.path(), "S"))
			require.Same(t, rec, idx.shards.Load("S"))
			require.True(t, rec.IsRecovering())
		})
	}
}
