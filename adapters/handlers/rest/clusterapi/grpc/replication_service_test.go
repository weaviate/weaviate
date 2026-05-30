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

package grpc

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/shared"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/replica"
	replicaerrors "github.com/weaviate/weaviate/usecases/replica/errors"
	replicaTypes "github.com/weaviate/weaviate/usecases/replica/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestReplicationErrorToGRPC(t *testing.T) {
	t.Run("nil returns nil", func(t *testing.T) {
		assert.Nil(t, replicationErrorToGRPC(nil))
	})

	t.Run("ErrUnprocessable returns FailedPrecondition", func(t *testing.T) {
		err := enterrors.NewErrUnprocessable(errors.New("shard loading"))
		grpcErr := replicationErrorToGRPC(err)
		require.NotNil(t, grpcErr)
		st, ok := status.FromError(grpcErr)
		require.True(t, ok)
		assert.Equal(t, codes.FailedPrecondition, st.Code())
		assert.Contains(t, st.Message(), "shard loading")
	})

	t.Run("other error returns Internal", func(t *testing.T) {
		err := errors.New("something went wrong")
		grpcErr := replicationErrorToGRPC(err)
		require.NotNil(t, grpcErr)
		st, ok := status.FromError(grpcErr)
		require.True(t, ok)
		assert.Equal(t, codes.Internal, st.Code())
		assert.Contains(t, st.Message(), "something went wrong")
	})
}

// TestReplicationService_PutObject_ConditionalRestored verifies that the gRPC
// server handler correctly restores storobj.Conditional from the proto fields
// before calling ReplicateObject.  This is the regression test for the
// RF>1 conditional-write bug: storobj.MarshalBinary omits Conditional, so the
// only way the receiving replica can see the precondition is via the dedicated
// proto fields added in PutObjectRequest.
//
// The test catches the bug because: if Conditional is not restored, the stub
// replicator receives an object with Conditional.IsZero() == true — the
// assertion below will fail.
func TestReplicationService_PutObject_ConditionalRestored(t *testing.T) {
	t.Parallel()

	// minimalReplicator stubs only ReplicateObject; all other methods panic (unused).
	type capturedArgs struct {
		obj *storobj.Object
	}
	type minimalReplicator struct {
		replicaTypes.Replicator
		got capturedArgs
	}

	for _, tc := range []struct {
		name            string
		onlyIfNotExists bool
		onlyIfExists    bool
		wantCond        storobj.Conditional
	}{
		{
			name:            "OnlyIfNotExists propagated",
			onlyIfNotExists: true,
			wantCond:        storobj.Conditional{OnlyIfNotExists: true},
		},
		{
			name:         "OnlyIfExists propagated",
			onlyIfExists: true,
			wantCond:     storobj.Conditional{OnlyIfExists: true},
		},
		{
			name:     "unconditional: Conditional stays zero",
			wantCond: storobj.Conditional{},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var receivedObj *storobj.Object
			stub := &fakeMinimalReplicator{
				onReplicateObject: func(_ context.Context, _, _, _ string, obj *storobj.Object, _ uint64) replica.SimpleResponse {
					receivedObj = obj
					return replica.SimpleResponse{}
				},
			}
			svc := NewReplicationService(stub)

			// Build a valid binary-serialisable object (MarshallerVersion=1 is required).
			src := &storobj.Object{MarshallerVersion: 1}
			src.Object.ID = "00000000-0000-0000-0000-000000000001"
			src.Object.Class = "TestClass"
			// Set the conditional only on the *source* object (simulates crud.go wiring).
			// The test then asserts the server restores it from the proto fields, NOT from
			// the binary payload (which would be the bug if it ever worked that way).
			src.Conditional = storobj.Conditional{
				OnlyIfNotExists: tc.onlyIfNotExists,
				OnlyIfExists:    tc.onlyIfExists,
			}

			data, err := src.MarshalBinary()
			require.NoError(t, err)

			// Wire the proto request the way the gRPC client does: Conditional in
			// dedicated fields, NOT in object_data.
			req := &pb.PutObjectRequest{
				Index:           "TestClass",
				Shard:           "shard0",
				RequestId:       "rid-1",
				ObjectData:      data,
				OnlyIfNotExists: tc.onlyIfNotExists,
				OnlyIfExists:    tc.onlyIfExists,
			}

			_, err = svc.PutObject(context.Background(), req)
			require.NoError(t, err)
			require.NotNil(t, receivedObj, "ReplicateObject must have been called")
			assert.Equal(t, tc.wantCond, receivedObj.Conditional,
				"Conditional must be restored from proto fields, not from binary payload")
		})
	}
}

// fakeMinimalReplicator is a test-only stub that implements only ReplicateObject.
// All other interface methods are forwarded to a panic-on-call embedded type so
// the compiler is satisfied without having to stub every method.
type fakeMinimalReplicator struct {
	replicaTypes.Replicator
	onReplicateObject func(ctx context.Context, className, shardName, requestID string, object *storobj.Object, schemaVersion uint64) replica.SimpleResponse
}

func (f *fakeMinimalReplicator) ReplicateObject(ctx context.Context, className, shardName, requestID string, object *storobj.Object, schemaVersion uint64) replica.SimpleResponse {
	return f.onReplicateObject(ctx, className, shardName, requestID, object, schemaVersion)
}

func TestLocalIndexNotReady(t *testing.T) {
	t.Run("empty response", func(t *testing.T) {
		assert.False(t, shared.LocalIndexNotReady(replica.SimpleResponse{}))
	})

	t.Run("non-StatusNotReady error", func(t *testing.T) {
		resp := replica.SimpleResponse{
			Errors: []replicaerrors.Error{
				{Code: replicaerrors.StatusConflict, Msg: "conflict"},
			},
		}
		assert.False(t, shared.LocalIndexNotReady(resp))
	})

	t.Run("StatusNotReady returns true", func(t *testing.T) {
		resp := replica.SimpleResponse{
			Errors: []replicaerrors.Error{
				{Code: replicaerrors.StatusNotReady, Msg: "index loading"},
			},
		}
		assert.True(t, shared.LocalIndexNotReady(resp))
	})
}
