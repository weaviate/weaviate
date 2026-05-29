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

package replica_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	replicaerrors "github.com/weaviate/weaviate/usecases/replica/errors"
)

// TestConditionalWriteAbortAtRequestCL verifies that:
//
//  1. When a phase-1 precondition check fails on enough replicas to
//     prevent reaching the requested consistency level, PutObject returns
//     *objects.ErrPreconditionFailed to the caller (the precondition abort
//     propagates at the caller-supplied CL — QUORUM in the test).
//
//  2. When one replica is unavailable (N=3, 1 down), a conditional write at
//     QUORUM still completes because floor(N/2)+1 = 2 replicas are reachable
//     and the remaining 2 succeed (INV-HA-1 preserved).
//
// Neither sub-test introduces AllowQuorumConsistency or forces CL=ALL; both
// pass the caller-supplied ConsistencyLevelQuorum unmodified.
func TestConditionalWriteAbortAtRequestCL(t *testing.T) {
	const (
		cls   = "C1"
		shard = "SH1"
	)

	t.Run("PreconditionFailedPropagatesAtQUORUM", func(t *testing.T) {
		// N=3, QUORUM requires 2 successes.
		// All three replicas respond to phase-1 prepare (PutObject) with OK.
		// Both replicas that reach commit return StatusPreconditionFailed.
		// The third replica also returns StatusPreconditionFailed but may be
		// processed after QUORUM is already breached.
		// Expected: PutObject returns *objects.ErrPreconditionFailed.
		nodes := []string{"A", "B", "C"}
		f := newFakeFactory(t, cls, shard, nodes, false)
		rep := f.newReplicator()

		ctx := context.Background()
		obj := &storobj.Object{}
		prepareResp := replica.SimpleResponse{}

		precondMsg := "object already exists"
		commitResp := replica.SimpleResponse{
			Errors: []replicaerrors.Error{{
				Code: replicaerrors.StatusPreconditionFailed,
				Msg:  precondMsg,
			}},
		}

		var commitWG sync.WaitGroup
		for _, n := range nodes {
			f.WClient.On("PutObject", mock.Anything, n, cls, shard, mock.Anything, obj, uint64(0)).
				Return(prepareResp, nil)
			commitWG.Add(1)
			f.WClient.On("Commit", ctx, n, cls, shard, mock.Anything, mock.Anything).
				Return(nil).
				RunFn = func(args mock.Arguments) {
				defer commitWG.Done()
				resp := args[5].(*replica.SimpleResponse)
				*resp = commitResp
			}
		}

		err := rep.PutObject(ctx, shard, obj, types.ConsistencyLevelQuorum, 0)
		commitWG.Wait()

		assert.Error(t, err, "expected error when precondition fails")
		var precondErr *objects.ErrPreconditionFailed
		assert.True(t, errors.As(err, &precondErr),
			"expected *objects.ErrPreconditionFailed, got %T: %v", err, err)
		assert.NotErrorIs(t, err, replicaerrors.ErrReplicas,
			"a precondition failure must NOT surface as ErrReplicas (not a replica-availability problem)")
	})

	t.Run("ConditionalWriteSucceedsWithOneReplicaDown_QUORUM", func(t *testing.T) {
		// N=3, QUORUM requires 2 successes.
		// Node "C" is unavailable (PutObject returns a connection error).
		// Nodes "A" and "B" respond OK to prepare and commit with no errors.
		// Expected: PutObject returns nil (QUORUM reached, INV-HA-1 preserved).
		nodes := []string{"A", "B", "C"}
		f := newFakeFactory(t, cls, shard, nodes, false)
		rep := f.newReplicator()

		ctx := context.Background()
		obj := &storobj.Object{}
		okPrepare := replica.SimpleResponse{}
		okCommit := replica.SimpleResponse{}

		// Nodes A and B: prepare and commit succeed.
		var commitWG sync.WaitGroup
		for _, n := range []string{"A", "B"} {
			f.WClient.On("PutObject", mock.Anything, n, cls, shard, mock.Anything, obj, uint64(0)).
				Return(okPrepare, nil)
			commitWG.Add(1)
			f.WClient.On("Commit", ctx, n, cls, shard, mock.Anything, mock.Anything).
				Return(nil).
				RunFn = func(args mock.Arguments) {
				defer commitWG.Done()
				resp := args[5].(*replica.SimpleResponse)
				*resp = okCommit
			}
		}
		// Node C is down: phase-1 prepare fails with a connection error.
		f.WClient.On("PutObject", mock.Anything, "C", cls, shard, mock.Anything, obj, uint64(0)).
			Return(replica.SimpleResponse{}, errors.New("connection refused"))
		// Abort is called on nodes that replied OK during broadcast when the
		// level is not yet reached; because QUORUM = 2 and C fails, the
		// coordinator aborts C but still has A and B for QUORUM.
		// The mock uses Maybe() so unused calls don't fail the test.
		f.WClient.On("Abort", mock.Anything, "C", cls, shard, mock.Anything).
			Return(okPrepare, nil).Maybe()

		err := rep.PutObject(ctx, shard, obj, types.ConsistencyLevelQuorum, 0)
		commitWG.Wait()

		assert.NoError(t, err,
			"conditional write must succeed at QUORUM when floor(N/2)+1 replicas are up (INV-HA-1)")
	})
}
