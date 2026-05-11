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

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// nilIndexRepo always returns nil from GetIndexForIncomingSharding,
// simulating the case where the local node knows the request name but
// the corresponding index isn't loaded yet (eventual-consistency window
// during startup or schema-replay).
type nilIndexRepo struct{}

func (nilIndexRepo) GetIndexForIncomingSharding(_ schema.ClassName) sharding.RemoteIndexIncomingRepo {
	return nil
}

// TestListFiles_NilIndexReturnsUnavailable verifies the gRPC code
// returned when the local index is nil. NotFound would tell a
// SELF_RECOVERY probe "definitively no data" and could push the
// orchestrator into the catastrophic-wipe empty-fallback path even
// though the peer might just be mid-startup. Unavailable signals
// "transient — please retry".
func TestListFiles_NilIndexReturnsUnavailable(t *testing.T) {
	fps := NewFileReplicationService(nilIndexRepo{}, nil, 0)

	_, err := fps.ListFiles(context.Background(), &pb.ListFilesRequest{
		IndexName: "Paragraph",
		ShardName: "shard-x",
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok, "error must carry a gRPC status: %v", err)
	require.Equal(t, codes.Unavailable, st.Code(),
		"nil index must surface as Unavailable (transient), not NotFound (definitive); got %s", st.Code())
}

// TestIsShardAbsent pins the substring matcher that maps IncomingListFiles
// errors to gRPC NotFound. It must match the canonical "shard is nil" /
// "shard not found" phrasings (so a SELF_RECOVERY probe sees "definitively
// no data") but nothing broader (e.g. a missing *file* must not be
// misread as a missing shard).
func TestIsShardAbsent(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{errors.New("incoming list files: shard is nil"), true},
		{errors.New("shard not found for collection X"), true},
		{errors.New("file segment-123.db not found"), false},
		{errors.New("some other transient error"), false},
	}
	for _, tc := range cases {
		require.Equal(t, tc.want, isShardAbsent(tc.err), "isShardAbsent(%v)", tc.err)
	}
}
