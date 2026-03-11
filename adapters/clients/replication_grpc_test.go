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
	"context"
	"encoding/json"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	grpcconn "github.com/weaviate/weaviate/grpc/conn"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// ── Fake gRPC replication server ─────────────────────────────────────────────

// fakeGRPCReplicationServer dispatches on request_id to return errors or success,
// mirroring the REST fakeServer pattern in replication_test.go.
type fakeGRPCReplicationServer struct {
	pb.UnimplementedReplicationServiceServer

	t *testing.T

	// RequestError is the SimpleReplicaResponse returned for RequestError request_id.
	requestError *pb.SimpleReplicaResponse

	// internalErrorCount: if > 0, the first N calls with RequestInternalError
	// return codes.Internal, then succeed. Used for retry testing.
	internalErrorCount atomic.Int32

	// Pre-configured responses for read operations.
	fetchObjectData  []byte
	fetchObjectsData []byte
	digestResponse   []*pb.RepairResponse
	overwriteResults []*pb.RepairResponse

	// commitPayload is the JSON payload returned by Commit on success.
	commitPayload []byte
}

func newFakeGRPCReplicationServer(t *testing.T) *fakeGRPCReplicationServer {
	t.Helper()
	return &fakeGRPCReplicationServer{
		t: t,
		requestError: &pb.SimpleReplicaResponse{
			Errors: []*pb.ReplicaError{{Msg: "error"}},
		},
	}
}

func (f *fakeGRPCReplicationServer) dispatchWrite(requestID, index, shard string) (*pb.SimpleReplicaResponse, error) {
	f.t.Helper()

	assert.Equal(f.t, "C1", index, "unexpected index")
	assert.Equal(f.t, "S1", shard, "unexpected shard")

	switch requestID {
	case RequestInternalError:
		remaining := f.internalErrorCount.Add(-1)
		if remaining >= 0 {
			return nil, status.Error(codes.Internal, "internal server error")
		}
		return &pb.SimpleReplicaResponse{}, nil
	case "RIDUnavailable":
		return nil, status.Error(codes.Unavailable, "service unavailable")
	case RequestError:
		return f.requestError, nil
	case RequestSuccess:
		return &pb.SimpleReplicaResponse{}, nil
	default:
		return &pb.SimpleReplicaResponse{}, nil
	}
}

func (f *fakeGRPCReplicationServer) PutObject(_ context.Context, req *pb.PutObjectRequest) (*pb.PutObjectResponse, error) {
	resp, err := f.dispatchWrite(req.GetRequestId(), req.GetIndex(), req.GetShard())
	if err != nil {
		return nil, err
	}
	return &pb.PutObjectResponse{Response: resp}, nil
}

func (f *fakeGRPCReplicationServer) PutObjects(_ context.Context, req *pb.PutObjectsRequest) (*pb.PutObjectsResponse, error) {
	resp, err := f.dispatchWrite(req.GetRequestId(), req.GetIndex(), req.GetShard())
	if err != nil {
		return nil, err
	}
	return &pb.PutObjectsResponse{Response: resp}, nil
}

func (f *fakeGRPCReplicationServer) MergeObject(_ context.Context, req *pb.MergeObjectRequest) (*pb.MergeObjectResponse, error) {
	resp, err := f.dispatchWrite(req.GetRequestId(), req.GetIndex(), req.GetShard())
	if err != nil {
		return nil, err
	}
	return &pb.MergeObjectResponse{Response: resp}, nil
}

func (f *fakeGRPCReplicationServer) DeleteObject(_ context.Context, req *pb.DeleteObjectRequest) (*pb.DeleteObjectResponse, error) {
	resp, err := f.dispatchWrite(req.GetRequestId(), req.GetIndex(), req.GetShard())
	if err != nil {
		return nil, err
	}
	return &pb.DeleteObjectResponse{Response: resp}, nil
}

func (f *fakeGRPCReplicationServer) DeleteObjects(_ context.Context, req *pb.DeleteObjectsRequest) (*pb.DeleteObjectsResponse, error) {
	resp, err := f.dispatchWrite(req.GetRequestId(), req.GetIndex(), req.GetShard())
	if err != nil {
		return nil, err
	}
	return &pb.DeleteObjectsResponse{Response: resp}, nil
}

func (f *fakeGRPCReplicationServer) AddReferences(_ context.Context, req *pb.AddReferencesRequest) (*pb.AddReferencesResponse, error) {
	resp, err := f.dispatchWrite(req.GetRequestId(), req.GetIndex(), req.GetShard())
	if err != nil {
		return nil, err
	}
	return &pb.AddReferencesResponse{Response: resp}, nil
}

func (f *fakeGRPCReplicationServer) Commit(_ context.Context, req *pb.CommitRequest) (*pb.CommitResponse, error) {
	assert.Equal(f.t, "C1", req.GetIndex(), "unexpected index")
	assert.Equal(f.t, "S1", req.GetShard(), "unexpected shard")

	switch req.GetRequestId() {
	case RequestInternalError:
		return nil, status.Error(codes.Internal, "internal server error")
	case RequestError:
		payload, _ := json.Marshal(replica.SimpleResponse{Errors: []replica.Error{{Msg: "error"}}})
		return &pb.CommitResponse{Payload: payload}, nil
	case RequestMalFormedResponse:
		return &pb.CommitResponse{Payload: []byte(`not valid json`)}, nil
	case RequestSuccess:
		return &pb.CommitResponse{Payload: f.commitPayload}, nil
	default:
		return &pb.CommitResponse{}, nil
	}
}

func (f *fakeGRPCReplicationServer) Abort(_ context.Context, req *pb.AbortRequest) (*pb.AbortResponse, error) {
	assert.Equal(f.t, "C1", req.GetIndex(), "unexpected index")
	assert.Equal(f.t, "S1", req.GetShard(), "unexpected shard")

	switch req.GetRequestId() {
	case RequestInternalError:
		return nil, status.Error(codes.Internal, "internal server error")
	case "RIDUnavailable":
		return nil, status.Error(codes.Unavailable, "service unavailable")
	case RequestError:
		return &pb.AbortResponse{Response: f.requestError}, nil
	case RequestSuccess:
		return &pb.AbortResponse{Response: &pb.SimpleReplicaResponse{}}, nil
	default:
		return &pb.AbortResponse{Response: &pb.SimpleReplicaResponse{}}, nil
	}
}

func (f *fakeGRPCReplicationServer) FetchObject(_ context.Context, req *pb.FetchObjectRequest) (*pb.FetchObjectResponse, error) {
	assert.Equal(f.t, "C1", req.GetIndex())
	assert.Equal(f.t, "S1", req.GetShard())

	if req.GetUuid() == RequestMalFormedResponse {
		return &pb.FetchObjectResponse{ReplicaData: []byte("corrupted binary")}, nil
	}
	return &pb.FetchObjectResponse{ReplicaData: f.fetchObjectData}, nil
}

func (f *fakeGRPCReplicationServer) FetchObjects(_ context.Context, req *pb.FetchObjectsRequest) (*pb.FetchObjectsResponse, error) {
	assert.Equal(f.t, "C1", req.GetIndex())
	assert.Equal(f.t, "S1", req.GetShard())

	if len(req.GetUuids()) > 0 && req.GetUuids()[0] == RequestMalFormedResponse {
		return &pb.FetchObjectsResponse{ReplicasData: []byte("corrupted binary")}, nil
	}
	return &pb.FetchObjectsResponse{ReplicasData: f.fetchObjectsData}, nil
}

func (f *fakeGRPCReplicationServer) DigestObjects(_ context.Context, req *pb.DigestObjectsRequest) (*pb.DigestObjectsResponse, error) {
	assert.Equal(f.t, "C1", req.GetIndex())
	assert.Equal(f.t, "S1", req.GetShard())

	return &pb.DigestObjectsResponse{Digests: f.digestResponse}, nil
}

func (f *fakeGRPCReplicationServer) OverwriteObjects(_ context.Context, req *pb.OverwriteObjectsRequest) (*pb.OverwriteObjectsResponse, error) {
	assert.Equal(f.t, "C1", req.GetIndex())
	assert.Equal(f.t, "S1", req.GetShard())

	return &pb.OverwriteObjectsResponse{Results: f.overwriteResults}, nil
}

// ── Setup helper ─────────────────────────────────────────────────────────────

func setupGRPCTestServer(t *testing.T, fake *fakeGRPCReplicationServer) (*grpcReplicationClient, func()) {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	s := grpc.NewServer()
	pb.RegisterReplicationServiceServer(s, fake)

	go func() {
		if err := s.Serve(lis); err != nil {
			t.Logf("gRPC server exited: %v", err)
		}
	}()

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}

	reg := prometheus.NewPedanticRegistry()
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	connMgr, err := grpcconn.NewConnManager(10, time.Minute, reg, logger,
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	client := NewGRPCReplicationClient(connMgr)

	cleanup := func() {
		connMgr.Close()
		s.Stop()
		lis.Close()
	}

	return client, cleanup
}

// closedConnManager returns a ConnManager that is already closed, simulating connection errors.
func closedConnManager(t *testing.T) *grpcconn.ConnManager {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	logger := logrus.New()
	m, err := grpcconn.NewConnManager(10, time.Minute, reg, logger,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	m.Close()
	return m
}

// ── Write operation tests ────────────────────────────────────────────────────

func TestGRPCReplicationPutObject(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	fake := newFakeGRPCReplicationServer(t)
	client, cleanup := setupGRPCTestServer(t, fake)
	defer cleanup()

	t.Run("EncodeRequest", func(t *testing.T) {
		obj := &storobj.Object{}
		_, err := client.PutObject(ctx, "passthrough:bufnet", "C1", "S1", "RID", obj, 0)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "marshal")
	})

	obj := &storobj.Object{MarshallerVersion: 1, Object: anyObject(UUID1)}

	t.Run("ConnectionError", func(t *testing.T) {
		closedClient := NewGRPCReplicationClient(closedConnManager(t))
		_, err := closedClient.PutObject(ctx, "passthrough:bufnet", "C1", "S1", "", obj, 0)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "connection")
	})

	t.Run("Error", func(t *testing.T) {
		resp, err := client.PutObject(ctx, "passthrough:bufnet", "C1", "S1", RequestError, obj, 0)
		assert.Nil(t, err)
		assert.Equal(t, replica.SimpleResponse{Errors: []replica.Error{{Msg: "error"}}}, resp)
	})

	t.Run("ServerInternalError", func(t *testing.T) {
		fake.internalErrorCount.Store(1)
		_, err := client.PutObject(ctx, "passthrough:bufnet", "C1", "S1", RequestInternalError, obj, 0)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "Internal")
	})

	t.Run("ServerUnavailable", func(t *testing.T) {
		_, err := client.PutObject(ctx, "passthrough:bufnet", "C1", "S1", "RIDUnavailable", obj, 0)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "Unavailable")
	})
}

func TestGRPCReplicationPutObjects(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	fake := newFakeGRPCReplicationServer(t)
	client, cleanup := setupGRPCTestServer(t, fake)
	defer cleanup()

	t.Run("EncodeRequest", func(t *testing.T) {
		objs := []*storobj.Object{{}}
		_, err := client.PutObjects(ctx, "passthrough:bufnet", "C1", "S1", "RID", objs, 123)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "marshal")
	})

	objs := []*storobj.Object{
		{MarshallerVersion: 1, Object: anyObject(UUID1)},
		{MarshallerVersion: 1, Object: anyObject(UUID2)},
	}

	t.Run("ConnectionError", func(t *testing.T) {
		closedClient := NewGRPCReplicationClient(closedConnManager(t))
		_, err := closedClient.PutObjects(ctx, "passthrough:bufnet", "C1", "S1", "", objs, 123)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "connection")
	})

	t.Run("Error", func(t *testing.T) {
		resp, err := client.PutObjects(ctx, "passthrough:bufnet", "C1", "S1", RequestError, objs, 123)
		assert.Nil(t, err)
		assert.Equal(t, replica.SimpleResponse{Errors: []replica.Error{{Msg: "error"}}}, resp)
	})

	t.Run("ServerInternalError", func(t *testing.T) {
		fake.internalErrorCount.Store(1)
		_, err := client.PutObjects(ctx, "passthrough:bufnet", "C1", "S1", RequestInternalError, objs, 123)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "Internal")
	})

	t.Run("ServerUnavailable", func(t *testing.T) {
		_, err := client.PutObjects(ctx, "passthrough:bufnet", "C1", "S1", "RIDUnavailable", objs, 123)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "Unavailable")
	})
}

func TestGRPCReplicationMergeObject(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	fake := newFakeGRPCReplicationServer(t)
	client, cleanup := setupGRPCTestServer(t, fake)
	defer cleanup()

	doc := &objects.MergeDocument{ID: UUID1}

	t.Run("ConnectionError", func(t *testing.T) {
		closedClient := NewGRPCReplicationClient(closedConnManager(t))
		_, err := closedClient.MergeObject(ctx, "passthrough:bufnet", "C1", "S1", "", doc, 0)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "connection")
	})

	t.Run("Error", func(t *testing.T) {
		resp, err := client.MergeObject(ctx, "passthrough:bufnet", "C1", "S1", RequestError, doc, 0)
		assert.Nil(t, err)
		assert.Equal(t, replica.SimpleResponse{Errors: []replica.Error{{Msg: "error"}}}, resp)
	})

	t.Run("ServerInternalError", func(t *testing.T) {
		fake.internalErrorCount.Store(1)
		_, err := client.MergeObject(ctx, "passthrough:bufnet", "C1", "S1", RequestInternalError, doc, 0)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "Internal")
	})

	t.Run("ServerUnavailable", func(t *testing.T) {
		_, err := client.MergeObject(ctx, "passthrough:bufnet", "C1", "S1", "RIDUnavailable", doc, 0)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "Unavailable")
	})
}

func TestGRPCReplicationDeleteObject(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	fake := newFakeGRPCReplicationServer(t)
	client, cleanup := setupGRPCTestServer(t, fake)
	defer cleanup()

	uuid := UUID1
	deletionTime := time.Now()

	t.Run("ConnectionError", func(t *testing.T) {
		closedClient := NewGRPCReplicationClient(closedConnManager(t))
		_, err := closedClient.DeleteObject(ctx, "passthrough:bufnet", "C1", "S1", "", uuid, deletionTime, 0)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "connection")
	})

	t.Run("Error", func(t *testing.T) {
		resp, err := client.DeleteObject(ctx, "passthrough:bufnet", "C1", "S1", RequestError, uuid, deletionTime, 0)
		assert.Nil(t, err)
		assert.Equal(t, replica.SimpleResponse{Errors: []replica.Error{{Msg: "error"}}}, resp)
	})

	t.Run("ServerInternalError", func(t *testing.T) {
		fake.internalErrorCount.Store(1)
		_, err := client.DeleteObject(ctx, "passthrough:bufnet", "C1", "S1", RequestInternalError, uuid, deletionTime, 0)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "Internal")
	})

	t.Run("ServerUnavailable", func(t *testing.T) {
		_, err := client.DeleteObject(ctx, "passthrough:bufnet", "C1", "S1", "RIDUnavailable", uuid, deletionTime, 0)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "Unavailable")
	})
}

func TestGRPCReplicationDeleteObjects(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	fake := newFakeGRPCReplicationServer(t)
	client, cleanup := setupGRPCTestServer(t, fake)
	defer cleanup()

	uuids := []strfmt.UUID{strfmt.UUID("1"), strfmt.UUID("2")}
	deletionTime := time.Now()

	t.Run("ConnectionError", func(t *testing.T) {
		closedClient := NewGRPCReplicationClient(closedConnManager(t))
		_, err := closedClient.DeleteObjects(ctx, "passthrough:bufnet", "C1", "S1", "", uuids, deletionTime, false, 123)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "connection")
	})

	t.Run("Error", func(t *testing.T) {
		resp, err := client.DeleteObjects(ctx, "passthrough:bufnet", "C1", "S1", RequestError, uuids, deletionTime, false, 123)
		assert.Nil(t, err)
		assert.Equal(t, replica.SimpleResponse{Errors: []replica.Error{{Msg: "error"}}}, resp)
	})

	t.Run("ServerInternalError", func(t *testing.T) {
		fake.internalErrorCount.Store(1)
		_, err := client.DeleteObjects(ctx, "passthrough:bufnet", "C1", "S1", RequestInternalError, uuids, deletionTime, false, 123)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "Internal")
	})

	t.Run("ServerUnavailable", func(t *testing.T) {
		_, err := client.DeleteObjects(ctx, "passthrough:bufnet", "C1", "S1", "RIDUnavailable", uuids, deletionTime, false, 123)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "Unavailable")
	})
}

func TestGRPCReplicationAddReferences(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	fake := newFakeGRPCReplicationServer(t)
	client, cleanup := setupGRPCTestServer(t, fake)
	defer cleanup()

	refs := []objects.BatchReference{{OriginalIndex: 1}, {OriginalIndex: 2}}

	t.Run("ConnectionError", func(t *testing.T) {
		closedClient := NewGRPCReplicationClient(closedConnManager(t))
		_, err := closedClient.AddReferences(ctx, "passthrough:bufnet", "C1", "S1", "", refs, 0)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "connection")
	})

	t.Run("Error", func(t *testing.T) {
		resp, err := client.AddReferences(ctx, "passthrough:bufnet", "C1", "S1", RequestError, refs, 0)
		assert.Nil(t, err)
		assert.Equal(t, replica.SimpleResponse{Errors: []replica.Error{{Msg: "error"}}}, resp)
	})

	t.Run("ServerInternalError", func(t *testing.T) {
		fake.internalErrorCount.Store(1)
		_, err := client.AddReferences(ctx, "passthrough:bufnet", "C1", "S1", RequestInternalError, refs, 0)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "Internal")
	})

	t.Run("ServerUnavailable", func(t *testing.T) {
		_, err := client.AddReferences(ctx, "passthrough:bufnet", "C1", "S1", "RIDUnavailable", refs, 0)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "Unavailable")
	})
}

func TestGRPCReplicationCommit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	fake := newFakeGRPCReplicationServer(t)
	client, cleanup := setupGRPCTestServer(t, fake)
	defer cleanup()

	t.Run("ConnectionError", func(t *testing.T) {
		closedClient := NewGRPCReplicationClient(closedConnManager(t))
		resp := replica.SimpleResponse{}
		err := closedClient.Commit(ctx, "passthrough:bufnet", "C1", "S1", "", &resp)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "connection")
	})

	t.Run("Error", func(t *testing.T) {
		resp := replica.SimpleResponse{}
		err := client.Commit(ctx, "passthrough:bufnet", "C1", "S1", RequestError, &resp)
		assert.Nil(t, err)
		assert.Equal(t, replica.SimpleResponse{Errors: []replica.Error{{Msg: "error"}}}, resp)
	})

	t.Run("DecodeResponse", func(t *testing.T) {
		resp := replica.SimpleResponse{}
		err := client.Commit(ctx, "passthrough:bufnet", "C1", "S1", RequestMalFormedResponse, &resp)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("ServerInternalError", func(t *testing.T) {
		fake.internalErrorCount.Store(1)
		resp := replica.SimpleResponse{}
		err := client.Commit(ctx, "passthrough:bufnet", "C1", "S1", RequestInternalError, &resp)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "Internal")
	})
}

func TestGRPCReplicationAbort(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	fake := newFakeGRPCReplicationServer(t)
	client, cleanup := setupGRPCTestServer(t, fake)
	defer cleanup()

	t.Run("ConnectionError", func(t *testing.T) {
		closedClient := NewGRPCReplicationClient(closedConnManager(t))
		_, err := closedClient.Abort(ctx, "passthrough:bufnet", "C1", "S1", "")
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "connection")
	})

	t.Run("Error", func(t *testing.T) {
		resp, err := client.Abort(ctx, "passthrough:bufnet", "C1", "S1", RequestError)
		assert.Nil(t, err)
		assert.Equal(t, replica.SimpleResponse{Errors: []replica.Error{{Msg: "error"}}}, resp)
	})

	t.Run("ServerInternalError", func(t *testing.T) {
		fake.internalErrorCount.Store(1)
		_, err := client.Abort(ctx, "passthrough:bufnet", "C1", "S1", RequestInternalError)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "Internal")
	})

	t.Run("ServerUnavailable", func(t *testing.T) {
		_, err := client.Abort(ctx, "passthrough:bufnet", "C1", "S1", "RIDUnavailable")
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "Unavailable")
	})
}

// ── Read operation tests ─────────────────────────────────────────────────────

func TestGRPCReplicationFetchObject(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	expected := replica.Replica{
		ID: UUID1,
		Object: &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID: UUID1,
				Properties: map[string]interface{}{
					"stringProp": "abc",
				},
			},
			Vector:    []float32{1, 2, 3, 4, 5},
			VectorLen: 5,
		},
	}

	replicaData, err := expected.MarshalBinary()
	require.NoError(t, err)

	fake := newFakeGRPCReplicationServer(t)
	fake.fetchObjectData = replicaData
	client, cleanup := setupGRPCTestServer(t, fake)
	defer cleanup()

	t.Run("Success", func(t *testing.T) {
		resp, err := client.FetchObject(ctx, "passthrough:bufnet", "C1", "S1",
			expected.ID, nil, additional.Properties{}, 9)
		require.Nil(t, err)
		assert.Equal(t, expected.ID, resp.ID)
		assert.Equal(t, expected.Deleted, resp.Deleted)
		assert.EqualValues(t, expected.Object, resp.Object)
	})

	t.Run("DecodeResponse", func(t *testing.T) {
		_, err := client.FetchObject(ctx, "passthrough:bufnet", "C1", "S1",
			strfmt.UUID(RequestMalFormedResponse), nil, additional.Properties{}, 9)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("ConnectionError", func(t *testing.T) {
		closedClient := NewGRPCReplicationClient(closedConnManager(t))
		_, err := closedClient.FetchObject(ctx, "passthrough:bufnet", "C1", "S1",
			UUID1, nil, additional.Properties{}, 9)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "connection")
	})
}

func TestGRPCReplicationFetchObjects(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	expected := replica.Replicas{
		{
			ID: UUID1,
			Object: &storobj.Object{
				MarshallerVersion: 1,
				Object: models.Object{
					ID: UUID1,
					Properties: map[string]interface{}{
						"stringProp": "abc",
					},
				},
				Vector:    []float32{1, 2, 3, 4, 5},
				VectorLen: 5,
			},
		},
		{
			ID: UUID2,
			Object: &storobj.Object{
				MarshallerVersion: 1,
				Object: models.Object{
					ID: UUID2,
					Properties: map[string]interface{}{
						"floatProp": float64(123),
					},
				},
				Vector:    []float32{10, 20, 30, 40, 50},
				VectorLen: 5,
			},
		},
	}

	replicasData, err := expected.MarshalBinary()
	require.NoError(t, err)

	fake := newFakeGRPCReplicationServer(t)
	fake.fetchObjectsData = replicasData
	client, cleanup := setupGRPCTestServer(t, fake)
	defer cleanup()

	t.Run("Success", func(t *testing.T) {
		resp, err := client.FetchObjects(ctx, "passthrough:bufnet", "C1", "S1",
			[]strfmt.UUID{expected[0].ID})
		require.Nil(t, err)
		require.Len(t, resp, 2)
		assert.Equal(t, expected[0].ID, resp[0].ID)
		assert.Equal(t, expected[0].Deleted, resp[0].Deleted)
		assert.EqualValues(t, expected[0].Object, resp[0].Object)
		assert.Equal(t, expected[1].ID, resp[1].ID)
		assert.Equal(t, expected[1].Deleted, resp[1].Deleted)
		assert.EqualValues(t, expected[1].Object, resp[1].Object)
	})

	t.Run("DecodeResponse", func(t *testing.T) {
		_, err := client.FetchObjects(ctx, "passthrough:bufnet", "C1", "S1",
			[]strfmt.UUID{strfmt.UUID(RequestMalFormedResponse)})
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("ConnectionError", func(t *testing.T) {
		closedClient := NewGRPCReplicationClient(closedConnManager(t))
		_, err := closedClient.FetchObjects(ctx, "passthrough:bufnet", "C1", "S1",
			[]strfmt.UUID{UUID1})
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "connection")
	})
}

func TestGRPCReplicationDigestObjects(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	now := time.Now()
	expected := []types.RepairResponse{
		{
			ID:         UUID1.String(),
			UpdateTime: now.UnixMilli(),
			Version:    1,
		},
		{
			ID:         UUID2.String(),
			Deleted:    true,
			UpdateTime: now.UnixMilli(),
			Version:    1,
		},
	}

	fake := newFakeGRPCReplicationServer(t)
	fake.digestResponse = []*pb.RepairResponse{
		{
			Id:         UUID1.String(),
			UpdateTime: now.UnixMilli(),
			Version:    1,
		},
		{
			Id:         UUID2.String(),
			Deleted:    true,
			UpdateTime: now.UnixMilli(),
			Version:    1,
		},
	}

	client, cleanup := setupGRPCTestServer(t, fake)
	defer cleanup()

	t.Run("Success", func(t *testing.T) {
		resp, err := client.DigestObjects(ctx, "passthrough:bufnet", "C1", "S1",
			[]strfmt.UUID{
				strfmt.UUID(expected[0].ID),
				strfmt.UUID(expected[1].ID),
			}, 9)
		require.Nil(t, err)
		require.Len(t, resp, 2)
		assert.Equal(t, expected[0].ID, resp[0].ID)
		assert.Equal(t, expected[0].Deleted, resp[0].Deleted)
		assert.Equal(t, expected[0].UpdateTime, resp[0].UpdateTime)
		assert.Equal(t, expected[0].Version, resp[0].Version)
		assert.Equal(t, expected[1].ID, resp[1].ID)
		assert.Equal(t, expected[1].Deleted, resp[1].Deleted)
		assert.Equal(t, expected[1].UpdateTime, resp[1].UpdateTime)
		assert.Equal(t, expected[1].Version, resp[1].Version)
	})

	t.Run("ConnectionError", func(t *testing.T) {
		closedClient := NewGRPCReplicationClient(closedConnManager(t))
		_, err := closedClient.DigestObjects(ctx, "passthrough:bufnet", "C1", "S1",
			[]strfmt.UUID{UUID1}, 9)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "connection")
	})
}

func TestGRPCReplicationOverwriteObjects(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	now := time.Now()
	input := []*objects.VObject{
		{
			LatestObject: &models.Object{
				ID:                 UUID1,
				Class:              "C1",
				CreationTimeUnix:   now.UnixMilli(),
				LastUpdateTimeUnix: now.Add(time.Hour).UnixMilli(),
				Properties: map[string]interface{}{
					"stringProp": "abc",
				},
				Vector: []float32{1, 2, 3, 4, 5},
			},
			StaleUpdateTime: now.UnixMilli(),
			Version:         0,
		},
	}

	expected := []types.RepairResponse{
		{
			ID:         UUID1.String(),
			Version:    1,
			UpdateTime: now.Add(time.Hour).UnixMilli(),
		},
	}

	fake := newFakeGRPCReplicationServer(t)
	fake.overwriteResults = []*pb.RepairResponse{
		{
			Id:         UUID1.String(),
			Version:    1,
			UpdateTime: now.Add(time.Hour).UnixMilli(),
		},
	}

	client, cleanup := setupGRPCTestServer(t, fake)
	defer cleanup()

	t.Run("Success", func(t *testing.T) {
		resp, err := client.OverwriteObjects(ctx, "passthrough:bufnet", "C1", "S1", input)
		require.Nil(t, err)
		require.Len(t, resp, 1)
		assert.Equal(t, expected[0].ID, resp[0].ID)
		assert.Equal(t, expected[0].Version, resp[0].Version)
		assert.Equal(t, expected[0].UpdateTime, resp[0].UpdateTime)
	})

	t.Run("ConnectionError", func(t *testing.T) {
		closedClient := NewGRPCReplicationClient(closedConnManager(t))
		_, err := closedClient.OverwriteObjects(ctx, "passthrough:bufnet", "C1", "S1", input)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "connection")
	})
}

// ── Retry behavior tests ────────────────────────────────────────────────────

func TestGRPCReplicationRetryOnTransient(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Server will fail 3 times with Internal, then succeed.
	fake := newFakeGRPCReplicationServer(t)
	fake.internalErrorCount.Store(3)
	client, cleanup := setupGRPCTestServer(t, fake)
	defer cleanup()

	obj := &storobj.Object{MarshallerVersion: 1, Object: anyObject(UUID1)}

	// This test documents that the gRPC client currently does NOT retry.
	// When retries are implemented, this test should change:
	// the call should succeed because the server eventually returns success.
	_, err := client.PutObject(ctx, "passthrough:bufnet", "C1", "S1", RequestInternalError, obj, 0)
	// Currently fails because gRPC client has no retries.
	// Once retries are added, this should be: assert.Nil(t, err)
	assert.NotNil(t, err, "gRPC client does not retry transient errors yet")
}

func TestGRPCReplicationNoRetryOnPermanent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Set up a fake that returns InvalidArgument (permanent error).
	lis := bufconn.Listen(1024 * 1024)
	permanentFake := &permanentErrorServer{}
	s := grpc.NewServer()
	pb.RegisterReplicationServiceServer(s, permanentFake)

	go func() {
		if err := s.Serve(lis); err != nil {
			t.Logf("gRPC server exited: %v", err)
		}
	}()
	defer func() {
		s.Stop()
		lis.Close()
	}()

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}

	reg := prometheus.NewPedanticRegistry()
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	connMgr, err := grpcconn.NewConnManager(10, time.Minute, reg, logger,
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer connMgr.Close()

	client := NewGRPCReplicationClient(connMgr)
	obj := &storobj.Object{MarshallerVersion: 1, Object: anyObject(UUID1)}

	_, err = client.PutObject(ctx, "passthrough:bufnet", "C1", "S1", "any", obj, 0)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "InvalidArgument")
}

// permanentErrorServer always returns InvalidArgument for PutObject.
type permanentErrorServer struct {
	pb.UnimplementedReplicationServiceServer
}

func (p *permanentErrorServer) PutObject(context.Context, *pb.PutObjectRequest) (*pb.PutObjectResponse, error) {
	return nil, status.Error(codes.InvalidArgument, "bad request")
}
