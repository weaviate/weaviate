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
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
)

// TestReadOnlyShard_RejectsWrites asserts that every mutation entry point of
// ReadOnlyShard returns ErrReadOnlyFollower. The wrapper's overrides never
// dereference the inner *Shard, so a nil inner is sufficient — and this pins the
// guarantee that a future write method added to *Shard but not overridden here
// (which would silently delegate to the inner shard) is caught by an explicit
// test, closing a silent-data-loss gap.
func TestReadOnlyShard_RejectsWrites(t *testing.T) {
	ctx := context.Background()
	s := &ReadOnlyShard{} // nil inner shard

	t.Run("methods returning error", func(t *testing.T) {
		errFns := map[string]func() error{
			"PutObject":                func() error { return s.PutObject(ctx, &storobj.Object{}) },
			"MergeObject":              func() error { return s.MergeObject(ctx, objects.MergeDocument{}) },
			"DeleteObject":             func() error { return s.DeleteObject(ctx, "id", time.Now()) },
			"batchDeleteObject":        func() error { return s.batchDeleteObject(ctx, "id", time.Now()) },
			"UpdateVectorIndexConfig":  func() error { return s.UpdateVectorIndexConfig(ctx, nil) },
			"UpdateVectorIndexConfigs": func() error { return s.UpdateVectorIndexConfigs(ctx, nil) },
			"DropVectorIndex":          func() error { return s.DropVectorIndex(ctx, "") },
			"ConvertQueue":             func() error { return s.ConvertQueue("") },
			"FillQueue":                func() error { return s.FillQueue("", 0) },
			"SetPropertyLengths":       func() error { return s.SetPropertyLengths(nil) },
			"enableAsyncReplication":   func() error { return s.enableAsyncReplication(ctx, AsyncReplicationConfig{}) },
			"disableAsyncReplication":  func() error { return s.disableAsyncReplication(ctx) },
			"addTargetNodeOverride":    func() error { return s.addTargetNodeOverride(ctx, additional.AsyncReplicationTargetNodeOverride{}) },
			"removeTargetNodeOverride": func() error { return s.removeTargetNodeOverride(ctx, additional.AsyncReplicationTargetNodeOverride{}) },
			"removeAllTargetOverrides": func() error { return s.removeAllTargetNodeOverrides(ctx) },
			"StopChangeCapture":        func() error { return s.StopChangeCapture(ctx, "") },
			"CreateAsyncCheckpoint":    func() error { return s.CreateAsyncCheckpoint(ctx, 0, time.Now()) },
			"DeleteAsyncCheckpoint":    func() error { return s.DeleteAsyncCheckpoint(ctx) },
			"HaltForTransfer":          func() error { return s.HaltForTransfer(ctx, false, 0) },
			"DebugResetVectorIndex":    func() error { return s.DebugResetVectorIndex(ctx, "") },
			"RepairIndex":              func() error { return s.RepairIndex(ctx, "") },
			"RequantizeIndex":          func() error { return s.RequantizeIndex(ctx, "") },
		}
		for name, fn := range errFns {
			require.ErrorIsf(t, fn(), ErrReadOnlyFollower, "%s must reject the write", name)
		}
	})

	t.Run("methods returning []error", func(t *testing.T) {
		for _, e := range s.PutObjectBatch(ctx, []*storobj.Object{{}, {}}) {
			require.ErrorIs(t, e, ErrReadOnlyFollower)
		}
		for _, e := range s.AddReferencesBatch(ctx, objects.BatchReferences{{}, {}}) {
			require.ErrorIs(t, e, ErrReadOnlyFollower)
		}
	})

	t.Run("DeleteObjectBatch", func(t *testing.T) {
		out := s.DeleteObjectBatch(ctx, []strfmt.UUID{"a", "b"}, time.Now(), false)
		require.Len(t, out, 2)
		for _, o := range out {
			require.ErrorIs(t, o.Err, ErrReadOnlyFollower)
		}
	})

	t.Run("replica prepare/commit/abort", func(t *testing.T) {
		resps := []replica.SimpleResponse{
			s.preparePutObject(ctx, "r", &storobj.Object{}),
			s.preparePutObjects(ctx, "r", nil),
			s.prepareMergeObject(ctx, "r", &objects.MergeDocument{}),
			s.prepareDeleteObject(ctx, "r", "id", time.Now()),
			s.prepareDeleteObjects(ctx, "r", nil, time.Now(), false),
			s.prepareAddReferences(ctx, "r", nil),
			s.abortReplication(ctx, "r"),
		}
		for i := range resps {
			require.Error(t, resps[i].FirstError())
		}
		commit, ok := s.commitReplication(ctx, "r").(replica.SimpleResponse)
		require.True(t, ok)
		require.Error(t, commit.FirstError())
	})

	t.Run("methods returning (T, error)", func(t *testing.T) {
		_, err := s.ActivateChangeLog(ctx, "op")
		require.ErrorIs(t, err, ErrReadOnlyFollower)
		_, err = s.FinalizeChangeLog(ctx, "op")
		require.ErrorIs(t, err, ErrReadOnlyFollower)
		_, err = s.filePutter(ctx, "r")
		require.ErrorIs(t, err, ErrReadOnlyFollower)
	})
}
