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
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
	"github.com/weaviate/weaviate/cluster/replication/changelog"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// noopGetChangeLogServer satisfies grpc.ServerStreamingServer[ChangeLogStreamEntry]
// for handler tests that never reach Send.
type noopGetChangeLogServer struct {
	ctx  context.Context
	sent []*pb.ChangeLogStreamEntry
}

func (s *noopGetChangeLogServer) Context() context.Context { return s.ctx }
func (s *noopGetChangeLogServer) Send(e *pb.ChangeLogStreamEntry) error {
	s.sent = append(s.sent, e)
	return nil
}
func (s *noopGetChangeLogServer) SetHeader(metadata.MD) error  { return nil }
func (s *noopGetChangeLogServer) SendHeader(metadata.MD) error { return nil }
func (s *noopGetChangeLogServer) SetTrailer(metadata.MD)       {}
func (s *noopGetChangeLogServer) SendMsg(any) error            { return nil }
func (s *noopGetChangeLogServer) RecvMsg(any) error            { return nil }

// fakeIndex stubs the changelog methods; other interface methods panic
// via the embedded nil interface so handlers can't touch them undetected.
type fakeIndex struct {
	sharding.RemoteIndexIncomingRepo

	startErr    error
	snapshotLSN uint64
	snapshotErr error
	finalizeLSN uint64
	finalizeErr error
	stopErr     error

	startCalls    []startCall
	snapshotCalls []opCall
	finalizeCalls []opCall
	stopCalls     []opCall
	getCalls      []getCall
}

type startCall struct{ shard, opID string }

type opCall struct{ shard, opID string }

type getCall struct {
	shard, opID string
	untilLSN    uint64
}

func (f *fakeIndex) IncomingStartChangeCapture(_ context.Context, shardName, opID string) error {
	f.startCalls = append(f.startCalls, startCall{shardName, opID})
	return f.startErr
}

func (f *fakeIndex) IncomingSnapshotChangeLogLSN(_ context.Context, shardName, opID string) (uint64, error) {
	f.snapshotCalls = append(f.snapshotCalls, opCall{shardName, opID})
	return f.snapshotLSN, f.snapshotErr
}

func (f *fakeIndex) IncomingFinalizeChangeLog(_ context.Context, shardName, opID string) (uint64, error) {
	f.finalizeCalls = append(f.finalizeCalls, opCall{shardName, opID})
	return f.finalizeLSN, f.finalizeErr
}

func (f *fakeIndex) IncomingStopChangeCapture(_ context.Context, shardName, opID string) error {
	f.stopCalls = append(f.stopCalls, opCall{shardName, opID})
	return f.stopErr
}

func (f *fakeIndex) IncomingGetChangeLog(_ context.Context, shardName, opID string, untilLSN uint64) (*changelog.Tailer, error) {
	f.getCalls = append(f.getCalls, getCall{shardName, opID, untilLSN})
	return nil, errors.New("no active change-log")
}

type fakeRepo struct {
	indices map[string]*fakeIndex
}

func (r *fakeRepo) GetIndexForIncomingSharding(className schema.ClassName) sharding.RemoteIndexIncomingRepo {
	idx, ok := r.indices[string(className)]
	if !ok {
		return nil
	}
	return idx
}

// fakeSchema implements sharding.RemoteIncomingSchema. ReadOnlyClassWithVersion
// records every requested version and errors when asked for a version higher
// than `applied`, simulating a source node that has not yet applied that schema
// command — which is exactly the wait the StartChangeCapture barrier relies on.
type fakeSchema struct {
	mu        sync.Mutex
	requested []uint64
	applied   uint64 // highest applied schema version; requests above this fail
	err       error  // forced error, overrides the applied check when set
}

func (s *fakeSchema) ReadOnlyClassWithVersion(_ context.Context, class string, version uint64) (*models.Class, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requested = append(s.requested, version)
	if s.err != nil {
		return nil, s.err
	}
	if version > s.applied {
		return nil, fmt.Errorf("schema version %d not applied (have %d)", version, s.applied)
	}
	return &models.Class{Class: class}, nil
}

func (s *fakeSchema) requestedVersions() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uint64(nil), s.requested...)
}

func newService(t *testing.T, indices map[string]*fakeIndex) *FileReplicationService {
	t.Helper()
	// Permissive schema: every version is already applied, so the barrier never blocks.
	return newServiceWithSchema(t, indices, &fakeSchema{applied: math.MaxUint64})
}

func newServiceWithSchema(t *testing.T, indices map[string]*fakeIndex, sc sharding.RemoteIncomingSchema) *FileReplicationService {
	t.Helper()
	return NewFileReplicationService(&fakeRepo{indices: indices}, sc, 64*1024)
}

func TestStartChangeCapture_HappyPath(t *testing.T) {
	fi := &fakeIndex{}
	svc := newService(t, map[string]*fakeIndex{"MyClass": fi})

	resp, err := svc.StartChangeCapture(context.Background(), &pb.StartChangeCaptureRequest{
		IndexName: "MyClass",
		ShardName: "shard1",
		OpId:      "op-1",
	})
	require.NoError(t, err)
	require.Equal(t, "MyClass", resp.IndexName)
	require.Equal(t, "shard1", resp.ShardName)
	require.Equal(t, "op-1", resp.OpId)
	require.Len(t, fi.startCalls, 1)
	require.Equal(t, startCall{"shard1", "op-1"}, fi.startCalls[0])
}

func TestStartChangeCapture_UnknownIndex(t *testing.T) {
	svc := newService(t, map[string]*fakeIndex{})

	_, err := svc.StartChangeCapture(context.Background(), &pb.StartChangeCaptureRequest{
		IndexName: "GhostClass",
		ShardName: "shard1",
		OpId:      "op-1",
	})
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
}

func TestStartChangeCapture_IndexError(t *testing.T) {
	fi := &fakeIndex{startErr: errors.New("boom")}
	svc := newService(t, map[string]*fakeIndex{"MyClass": fi})

	_, err := svc.StartChangeCapture(context.Background(), &pb.StartChangeCaptureRequest{
		IndexName: "MyClass",
		ShardName: "shard1",
		OpId:      "op-1",
	})
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
}

// StartChangeCapture must wait for the source to apply the op's HOT-activation
// schema version before opening the change-capture log. When the version is
// already applied, the barrier passes and the log is activated.
func TestStartChangeCapture_WaitsForSchemaVersion(t *testing.T) {
	fi := &fakeIndex{}
	sc := &fakeSchema{applied: 4}
	svc := newServiceWithSchema(t, map[string]*fakeIndex{"MyClass": fi}, sc)

	_, err := svc.StartChangeCapture(context.Background(), &pb.StartChangeCaptureRequest{
		IndexName:     "MyClass",
		ShardName:     "shard1",
		OpId:          "op-1",
		SchemaVersion: 4,
	})
	require.NoError(t, err)
	require.Len(t, fi.startCalls, 1)
	// The handler consulted the schema with the op's version before activating.
	require.Equal(t, []uint64{4}, sc.requestedVersions())
}

// Regression for the auto-tenant-activation FINALIZING flake: when the source
// has not yet applied the op's schema version (the tenant reactivation is still
// queued), the barrier must fail BEFORE the change-capture log is opened — never
// activate it on a shard instance a pending COLD→HOT reactivation will sweep.
func TestStartChangeCapture_BlockedUntilSchemaVersionApplied(t *testing.T) {
	fi := &fakeIndex{}
	sc := &fakeSchema{applied: 3} // op needs v5 but only v3 is applied on the source
	svc := newServiceWithSchema(t, map[string]*fakeIndex{"MyClass": fi}, sc)

	_, err := svc.StartChangeCapture(context.Background(), &pb.StartChangeCaptureRequest{
		IndexName:     "MyClass",
		ShardName:     "shard1",
		OpId:          "op-1",
		SchemaVersion: 5,
	})
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
	require.Empty(t, fi.startCalls, "change-capture log must not be activated before the schema version is applied")
	require.Equal(t, []uint64{5}, sc.requestedVersions())
}

func TestFinalizeChangeLog_HappyPath(t *testing.T) {
	fi := &fakeIndex{finalizeLSN: 42}
	svc := newService(t, map[string]*fakeIndex{"MyClass": fi})

	resp, err := svc.FinalizeChangeLog(context.Background(), &pb.FinalizeChangeLogRequest{
		IndexName: "MyClass",
		ShardName: "shard1",
		OpId:      "op-1",
	})
	require.NoError(t, err)
	require.Equal(t, uint64(42), resp.FinalLsn)
	require.Equal(t, "MyClass", resp.IndexName)
	require.Equal(t, "shard1", resp.ShardName)
	require.Equal(t, "op-1", resp.OpId)
	require.Len(t, fi.finalizeCalls, 1)
}

func TestFinalizeChangeLog_UnknownIndex(t *testing.T) {
	svc := newService(t, map[string]*fakeIndex{})

	_, err := svc.FinalizeChangeLog(context.Background(), &pb.FinalizeChangeLogRequest{
		IndexName: "GhostClass",
		ShardName: "shard1",
		OpId:      "op-1",
	})
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
}

func TestFinalizeChangeLog_IndexError(t *testing.T) {
	fi := &fakeIndex{finalizeErr: errors.New("no such op")}
	svc := newService(t, map[string]*fakeIndex{"MyClass": fi})

	_, err := svc.FinalizeChangeLog(context.Background(), &pb.FinalizeChangeLogRequest{
		IndexName: "MyClass",
		ShardName: "shard1",
		OpId:      "op-1",
	})
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
}

func TestStopChangeCapture_HappyPath(t *testing.T) {
	fi := &fakeIndex{}
	svc := newService(t, map[string]*fakeIndex{"MyClass": fi})

	resp, err := svc.StopChangeCapture(context.Background(), &pb.StopChangeCaptureRequest{
		IndexName: "MyClass",
		ShardName: "shard1",
		OpId:      "op-1",
	})
	require.NoError(t, err)
	require.Equal(t, "op-1", resp.OpId)
	require.Len(t, fi.stopCalls, 1)
}

func TestStopChangeCapture_UnknownIndex(t *testing.T) {
	svc := newService(t, map[string]*fakeIndex{})

	_, err := svc.StopChangeCapture(context.Background(), &pb.StopChangeCaptureRequest{
		IndexName: "GhostClass",
		ShardName: "shard1",
		OpId:      "op-1",
	})
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
}

func TestGetChangeLog_UnknownIndex(t *testing.T) {
	svc := newService(t, map[string]*fakeIndex{})

	err := svc.GetChangeLog(&pb.GetChangeLogRequest{
		IndexName: "GhostClass",
		ShardName: "shard1",
		OpId:      "op-1",
	}, &noopGetChangeLogServer{ctx: context.Background()})
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
}

func TestGetChangeLog_NoActiveLog(t *testing.T) {
	svc := newService(t, map[string]*fakeIndex{"MyClass": {}})

	err := svc.GetChangeLog(&pb.GetChangeLogRequest{
		IndexName: "MyClass",
		ShardName: "shard1",
		OpId:      "op-1",
	}, &noopGetChangeLogServer{ctx: context.Background()})
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
}

// A regression here would silently turn a cap'd drain into an unbounded
// stream — the consumer would then block waiting for Finalize that never comes.
func TestGetChangeLog_PlumbsUntilLsn(t *testing.T) {
	fi := &fakeIndex{}
	svc := newService(t, map[string]*fakeIndex{"MyClass": fi})

	_ = svc.GetChangeLog(&pb.GetChangeLogRequest{
		IndexName: "MyClass",
		ShardName: "shard1",
		OpId:      "op-1",
		UntilLsn:  77,
	}, &noopGetChangeLogServer{ctx: context.Background()})
	require.Len(t, fi.getCalls, 1)
	require.Equal(t, uint64(77), fi.getCalls[0].untilLSN)
}

func TestSnapshotChangeLogLSN_HappyPath(t *testing.T) {
	fi := &fakeIndex{snapshotLSN: 99}
	svc := newService(t, map[string]*fakeIndex{"MyClass": fi})

	resp, err := svc.SnapshotChangeLogLSN(context.Background(), &pb.SnapshotChangeLogLSNRequest{
		IndexName: "MyClass",
		ShardName: "shard1",
		OpId:      "op-1",
	})
	require.NoError(t, err)
	require.Equal(t, uint64(99), resp.Lsn)
	require.Equal(t, "MyClass", resp.IndexName)
	require.Equal(t, "shard1", resp.ShardName)
	require.Equal(t, "op-1", resp.OpId)
	require.Len(t, fi.snapshotCalls, 1)
	require.Equal(t, opCall{"shard1", "op-1"}, fi.snapshotCalls[0])
}

func TestSnapshotChangeLogLSN_UnknownIndex(t *testing.T) {
	svc := newService(t, map[string]*fakeIndex{})

	_, err := svc.SnapshotChangeLogLSN(context.Background(), &pb.SnapshotChangeLogLSNRequest{
		IndexName: "GhostClass",
		ShardName: "shard1",
		OpId:      "op-1",
	})
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
}

func TestSnapshotChangeLogLSN_IndexError(t *testing.T) {
	fi := &fakeIndex{snapshotErr: errors.New("no such op")}
	svc := newService(t, map[string]*fakeIndex{"MyClass": fi})

	_, err := svc.SnapshotChangeLogLSN(context.Background(), &pb.SnapshotChangeLogLSNRequest{
		IndexName: "MyClass",
		ShardName: "shard1",
		OpId:      "op-1",
	})
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
}
