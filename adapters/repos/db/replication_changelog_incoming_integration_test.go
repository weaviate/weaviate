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

//go:build integrationTest

package db

import (
	"context"
	"errors"
	"io"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/replication/changelog"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

// incomingChangeLogEndpoints are the change-capture endpoints a movement's
// target calls on the source. errorsOnMissingShard splits them: the three
// drain reads must report a shard they cannot serve, while stop is a teardown
// an unloaded shard has already satisfied.
var incomingChangeLogEndpoints = []struct {
	name                 string
	errorsOnMissingShard bool
	call                 func(ctx context.Context, idx *Index, shardName, opID string) error
}{
	{
		name:                 "get change log",
		errorsOnMissingShard: true,
		call: func(ctx context.Context, idx *Index, shardName, opID string) error {
			tailer, err := idx.IncomingGetChangeLog(ctx, shardName, opID, math.MaxUint64)
			if tailer != nil {
				_ = tailer.Close()
			}
			return err
		},
	},
	{
		name:                 "snapshot change-log LSN",
		errorsOnMissingShard: true,
		call: func(ctx context.Context, idx *Index, shardName, opID string) error {
			_, err := idx.IncomingSnapshotChangeLogLSN(ctx, shardName, opID)
			return err
		},
	},
	{
		name:                 "finalize change log",
		errorsOnMissingShard: true,
		call: func(ctx context.Context, idx *Index, shardName, opID string) error {
			_, err := idx.IncomingFinalizeChangeLog(ctx, shardName, opID)
			return err
		},
	},
	{
		name: "stop change capture",
		call: func(ctx context.Context, idx *Index, shardName, opID string) error {
			return idx.IncomingStopChangeCapture(ctx, shardName, opID)
		},
	},
}

func changelogPath(idx *Index, shardName, opID string) string {
	return filepath.Join(shardPath(idx.path(), shardName), changelogDirName, opID+changelogFileExtension)
}

func replayTestObject(idStr string) *storobj.Object {
	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:                 strfmt.UUID(idStr),
			Class:              "ReplayClass",
			Properties:         map[string]interface{}{"stringProp": "x"},
			LastUpdateTimeUnix: 1_000,
		},
	}
}

// Loading a shard sweeps its changelog dir, so a drain read that force-loads
// deletes the very log it was asked for — and the op registration it would
// need died with the unload anyway. Stop is in the table too, pinning the
// other half of the split: a teardown an unloaded shard has already satisfied.
func TestIncomingChangeLog_UnloadedShardKeepsLogAndStaysUnloaded(t *testing.T) {
	const opID = "op-drain-unloaded"

	for _, tc := range incomingChangeLogEndpoints {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			_, idx, shardName, _ := setupReplayShard(t)
			require.NoError(t, idx.IncomingStartChangeCapture(ctx, shardName, opID))

			logPath := changelogPath(idx, shardName, opID)
			require.FileExists(t, logPath)
			require.NoError(t, idx.UnloadLocalShard(ctx, shardName))
			require.Nil(t, idx.shards.Load(shardName), "shard must be unloaded before the call")

			err := tc.call(ctx, idx, shardName, opID)

			if tc.errorsOnMissingShard {
				require.Error(t, err)
				// The consumer reads either phrase as "the log is already sealed"
				// and marks the movement caught up. An unloaded shard is not that:
				// writes since the unload were never captured.
				require.NotContains(t, err.Error(), changelog.ErrMsgNoActiveLog)
				require.NotContains(t, err.Error(), changelog.ErrMsgNoActiveChangeCaptureLog)
			} else {
				require.NoError(t, err)
			}
			require.FileExists(t, logPath, "serving the call must not delete the log")
			require.Nil(t, idx.shards.Load(shardName), "serving the call must not load the shard")
		})
	}
}

// A shard name this node never hosted — a malformed or racing RPC — must not
// leave a shard behind.
func TestIncomingChangeLog_UnknownShardCreatesNothing(t *testing.T) {
	const opID = "op-phantom"

	for _, tc := range incomingChangeLogEndpoints {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			_, idx, _, _ := setupReplayShard(t)

			for _, shardName := range []string{"never-created-shard", ""} {
				err := tc.call(ctx, idx, shardName, opID)
				if tc.errorsOnMissingShard {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
				require.Nil(t, idx.shards.Load(shardName), "must not register shard %q", shardName)
			}

			require.NoDirExists(t, shardPath(idx.path(), "never-created-shard"),
				"must not create a shard directory")
		})
	}
}

// The whole drain sequence over a loaded shard, driven through the endpoints
// the movement's target actually calls.
func TestIncomingChangeLog_LoadedShardRoundTrip(t *testing.T) {
	ctx := testCtx()
	const (
		opID    = "op-roundtrip"
		writes  = 2
		lastLSN = uint64(writes)
	)

	repo, idx, shardName, class := setupReplayShard(t)
	require.NoError(t, idx.IncomingStartChangeCapture(ctx, shardName, opID))

	for range writes {
		obj := &models.Object{
			ID:         strfmt.UUID(uuid.NewString()),
			Class:      class.Class,
			Properties: map[string]interface{}{"stringProp": "captured"},
		}
		require.NoError(t, repo.PutObject(ctx, obj, []float32{1, 2, 3}, nil, nil, nil, 0))
	}

	snap, err := idx.IncomingSnapshotChangeLogLSN(ctx, shardName, opID)
	require.NoError(t, err)
	require.Equal(t, lastLSN, snap)

	tailer, err := idx.IncomingGetChangeLog(ctx, shardName, opID, snap)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tailer.Close() })

	var drained int
	for {
		_, err := tailer.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		drained++
	}
	require.Equal(t, writes, drained, "the tailer must emit every captured write up to the cap")

	finalLSN, err := idx.IncomingFinalizeChangeLog(ctx, shardName, opID)
	require.NoError(t, err)
	require.Equal(t, lastLSN, finalLSN)

	require.NoError(t, idx.IncomingStopChangeCapture(ctx, shardName, opID))

	// A loaded shard that no longer holds the op is the "already sealed" signal
	// the consumer acts on, and must stay distinguishable from an unloaded one.
	// Both drain reads carry it, through different constants.
	_, err = idx.IncomingSnapshotChangeLogLSN(ctx, shardName, opID)
	require.Error(t, err)
	require.Contains(t, err.Error(), changelog.ErrMsgNoActiveChangeCaptureLog)

	_, err = idx.IncomingGetChangeLog(ctx, shardName, opID, math.MaxUint64)
	require.Error(t, err)
	require.Contains(t, err.Error(), changelog.ErrMsgNoActiveLog)
}

// The seal holds a shutdown refcount, so an unload attempted mid-seal fails
// instead of tearing the shard down under it — and the seal still completes.
func TestIncomingFinalizeChangeLog_UnloadCannotTearDownMidSeal(t *testing.T) {
	ctx := testCtx()
	const opID = "op-finalize-unload"

	_, idx, shardName, _ := setupReplayShard(t)
	logger, _ := logrusTestLogger()
	require.NoError(t, idx.IncomingStartChangeCapture(ctx, shardName, opID))

	shard, ok := idx.shards.Loaded(shardName).(*Shard)
	require.True(t, ok)

	// Left uncommitted, so the seal blocks on it.
	require.Empty(t, shard.preparePutObject(ctx, "req-pending", replayTestObject(uuid.NewString())).Errors)

	finalizeDone := make(chan error, 1)
	enterrors.GoWrapper(func() {
		_, ferr := idx.IncomingFinalizeChangeLog(ctx, shardName, opID)
		finalizeDone <- ferr
	}, logger)

	// Let the seal take its refcount. If it instead returned early the drain
	// never started, so report that rather than the unload's verdict.
	time.Sleep(20 * time.Millisecond)
	select {
	case ferr := <-finalizeDone:
		t.Fatalf("seal returned before the in-flight write drained: %v", ferr)
	default:
	}

	require.Error(t, idx.UnloadLocalShard(ctx, shardName),
		"unload must not shut the shard down while a seal is in flight")

	shard.commitReplication(ctx, "req-pending")
	select {
	case ferr := <-finalizeDone:
		require.NoError(t, ferr, "seal must complete once the in-flight write drains")
	case <-time.After(30 * time.Second):
		t.Fatal("seal never completed after the in-flight write drained")
	}
}
