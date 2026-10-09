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
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/replica"
)

// A write acknowledged below ALL still reaches the lagging replica after the caller leaves.
func TestReplicatorWriteReachesLaggingReplicaAfterCallerLeaves(t *testing.T) {
	var (
		id    = strfmt.UUID("123")
		cls   = "C1"
		shard = "SH1"
		nodes = []string{"A", "B", "C"}
	)

	for _, tt := range []struct {
		name string
		cl   types.ConsistencyLevel
	}{
		{name: "ONE", cl: types.ConsistencyLevelOne},
		{name: "QUORUM", cl: types.ConsistencyLevelQuorum},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeFactory(t, cls, shard, nodes, false)
			rep := f.newReplicator()
			gate, release := newGate(t)
			obj := object(id, 3)
			lagging := nodes[2]

			prepared := make(chan error, 1)
			committed := make(chan struct{})
			for _, node := range nodes {
				if node == lagging {
					// answers only once released, failing if its request was cancelled meanwhile
					f.WClient.EXPECT().PutObject(anyVal, node, cls, shard, anyVal, obj, anyVal).
						RunAndReturn(func(ctx context.Context, _, _, _, _ string,
							_ *storobj.Object, _ uint64,
						) (replica.SimpleResponse, error) {
							<-gate
							err := ctx.Err()
							prepared <- err
							return replica.SimpleResponse{}, err
						})
					f.WClient.EXPECT().Commit(anyVal, node, cls, shard, anyVal, anyVal).
						RunAndReturn(func(context.Context, string, string, string, string, interface{}) error {
							close(committed)
							return nil
						}).Maybe()
				} else {
					f.WClient.EXPECT().PutObject(anyVal, node, cls, shard, anyVal, obj, anyVal).
						Return(replica.SimpleResponse{}, nil)
					f.WClient.EXPECT().Commit(anyVal, node, cls, shard, anyVal, anyVal).
						Return(nil).Maybe()
				}
				// issued only if too few replicas prepare
				f.WClient.EXPECT().Abort(anyVal, node, cls, shard, anyVal).
					Return(replica.SimpleResponse{}, nil).Maybe()
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			done := make(chan error, 1)
			go func() { done <- rep.PutObject(ctx, shard, obj, tt.cl, 123) }()
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(incidentBudget):
				t.Fatalf("PutObject at %s must return once the healthy replicas acknowledged", tt.cl)
			}

			// the request completes and its context is cancelled while the lagging replica is still preparing
			cancel()
			release()

			select {
			case err := <-prepared:
				assert.NoErrorf(t, err, "the prepare to lagging replica %q must survive the caller's cancellation", lagging)
			case <-time.After(incidentBudget):
				t.Fatalf("lagging replica %q never answered its prepare", lagging)
			}
			select {
			case <-committed:
			case <-time.After(incidentBudget):
				t.Fatalf("lagging replica %q was never told to commit: it silently misses the write", lagging)
			}
			// every replica has answered before the mocks are asserted
			drain, cancelDrain := context.WithTimeout(context.Background(), incidentBudget)
			defer cancelDrain()
			require.NoError(t, rep.WaitForDrain(drain, shard))
		})
	}
}

// A write stays in flight for the drain until every replica has answered, not just the level.
func TestReplicatorDrainWaitsForLaggingReplica(t *testing.T) {
	var (
		id    = strfmt.UUID("123")
		cls   = "C1"
		shard = "SH1"
		nodes = []string{"A", "B", "C"}
	)

	for _, tt := range []struct {
		name  string
		cl    types.ConsistencyLevel
		phase string
	}{
		{name: "ONE lagging prepare", cl: types.ConsistencyLevelOne, phase: "prepare"},
		{name: "ONE lagging commit", cl: types.ConsistencyLevelOne, phase: "commit"},
		{name: "QUORUM lagging prepare", cl: types.ConsistencyLevelQuorum, phase: "prepare"},
		{name: "QUORUM lagging commit", cl: types.ConsistencyLevelQuorum, phase: "commit"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeFactory(t, cls, shard, nodes, false)
			rep := f.newReplicator()
			gate, release := newGate(t)
			obj := object(id, 3)
			lagging := nodes[2]

			committed := make(chan struct{})
			for _, node := range nodes {
				switch {
				case node == lagging && tt.phase == "prepare":
					f.WClient.EXPECT().PutObject(anyVal, node, cls, shard, anyVal, obj, anyVal).
						RunAndReturn(func(context.Context, string, string, string, string,
							*storobj.Object, uint64,
						) (replica.SimpleResponse, error) {
							<-gate
							return replica.SimpleResponse{}, nil
						})
					f.WClient.EXPECT().Commit(anyVal, node, cls, shard, anyVal, anyVal).
						RunAndReturn(func(context.Context, string, string, string, string, interface{}) error {
							close(committed)
							return nil
						})
				case node == lagging && tt.phase == "commit":
					f.WClient.EXPECT().PutObject(anyVal, node, cls, shard, anyVal, obj, anyVal).
						Return(replica.SimpleResponse{}, nil)
					f.WClient.EXPECT().Commit(anyVal, node, cls, shard, anyVal, anyVal).
						RunAndReturn(func(context.Context, string, string, string, string, interface{}) error {
							<-gate
							close(committed)
							return nil
						})
				default:
					f.WClient.EXPECT().PutObject(anyVal, node, cls, shard, anyVal, obj, anyVal).
						Return(replica.SimpleResponse{}, nil)
					f.WClient.EXPECT().Commit(anyVal, node, cls, shard, anyVal, anyVal).
						Return(nil).Maybe()
				}
				// issued only if too few replicas prepare
				f.WClient.EXPECT().Abort(anyVal, node, cls, shard, anyVal).
					Return(replica.SimpleResponse{}, nil).Maybe()
			}

			done := make(chan error, 1)
			go func() { done <- rep.PutObject(context.Background(), shard, obj, tt.cl, 123) }()
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(incidentBudget):
				t.Fatalf("PutObject at %s must return once the healthy replicas acknowledged", tt.cl)
			}

			short, cancelShort := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancelShort()
			assert.Errorf(t, rep.WaitForDrain(short, shard),
				"the drain must not pass while the %s to lagging replica %q is still in flight", tt.phase, lagging)

			release()
			select {
			case <-committed:
			case <-time.After(incidentBudget):
				t.Fatalf("lagging replica %q was never told to commit", lagging)
			}
			long, cancelLong := context.WithTimeout(context.Background(), incidentBudget)
			defer cancelLong()
			assert.NoError(t, rep.WaitForDrain(long, shard), "the drain must pass once every replica answered")
		})
	}
}
