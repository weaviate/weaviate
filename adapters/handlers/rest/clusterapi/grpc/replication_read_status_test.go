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
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	clusterTypes "github.com/weaviate/weaviate/cluster/types"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// TestReplicationErrorToGRPCSeparatesLagFromFinal pins the gRPC codes against the REST statuses
// they mirror. Collapsing both onto FailedPrecondition, as this did before, left a coordinator
// asking a replica that can never serve the shard.
func TestReplicationErrorToGRPCSeparatesLagFromFinal(t *testing.T) {
	const (
		index = "MyClass"
		shard = "tenant-7"
	)

	tests := []struct {
		name string
		err  error
		want codes.Code
	}{
		{
			name: "a class this node may still grow into is unavailable",
			err:  enterrors.NewErrUnprocessable(enterrors.ErrLocalIndexNotFound{Index: index}),
			want: codes.Unavailable,
		},
		{
			name: "waiting out the schema version is unavailable",
			err:  enterrors.NewErrUnprocessable(clusterTypes.ErrDeadlineExceeded),
			want: codes.Unavailable,
		},
		{
			name: "caught up and still missing is final",
			err: enterrors.NewErrUnprocessable(enterrors.ErrNotServedHere{
				Index: index, Shard: shard, Version: 100,
			}),
			want: codes.FailedPrecondition,
		},
		{
			name: "a miss while still behind is left to the retry ladder",
			err:  enterrors.ErrLocalShardNotFound{Shard: shard},
			want: codes.Internal,
		},
		{
			name: "an unrelated failure is a fault",
			err:  io.ErrUnexpectedEOF,
			want: codes.Internal,
		},
		{
			name: "nil stays nil",
			err:  nil,
			want: codes.OK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := replicationErrorToGRPC(tt.err)
			if tt.want == codes.OK {
				assert.NoError(t, got)
				return
			}
			st, ok := status.FromError(got)
			assert.True(t, ok, "must be a status error, got %v", got)
			assert.Equal(t, tt.want, st.Code())
		})
	}
}
