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

package clients

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/weaviate/weaviate/usecases/replica"
)

// Pins the gRPC transport to the contract the REST one gets from HTTPError.Is. Without it the
// gRPC read path never failed over at all.
func TestReadGRPCErrorCarriesTheNotReadySentinel(t *testing.T) {
	tests := []struct {
		name         string
		err          error
		wantNotReady bool
	}{
		{
			name:         "a replica behind on schema",
			err:          status.Error(codes.Unavailable, "applied schema index 90, read resolved at version 100"),
			wantNotReady: true,
		},
		{
			name:         "an unreachable peer is equally not worth asking again",
			err:          status.Error(codes.Unavailable, "connection refused"),
			wantNotReady: true,
		},
		{
			name:         "a final miss must not read as not-ready, or the replica stays in play",
			err:          status.Error(codes.FailedPrecondition, `local index "C1" has no shard "t7" at schema version 100`),
			wantNotReady: false,
		},
		{
			name:         "a fault is a fault",
			err:          status.Error(codes.Internal, "bucket is corrupt"),
			wantNotReady: false,
		},
		{
			name:         "a plain error is left alone",
			err:          errors.New("marshal replica"),
			wantNotReady: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := readGRPCError("CountObjects", tt.err)
			assert.Equal(t, tt.wantNotReady, errors.Is(got, replica.ErrReplicaNotReady))
			assert.ErrorIs(t, got, tt.err, "the cause must stay reachable for operators")
			assert.Contains(t, got.Error(), "CountObjects", "the op must stay in the message")
		})
	}
}
