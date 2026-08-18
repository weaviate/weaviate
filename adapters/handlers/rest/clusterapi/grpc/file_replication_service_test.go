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
	"io"
	"math"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
	"github.com/weaviate/weaviate/cluster/replication/changelog"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/file"
	"github.com/weaviate/weaviate/usecases/namespaces"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// noopStreamServer satisfies grpc.ServerStreamingServer[T], which every streaming
// handler in this service takes, for tests that never reach Send.
type noopStreamServer[T any] struct {
	ctx  context.Context
	sent []*T
}

func (s *noopStreamServer[T]) Context() context.Context { return s.ctx }
func (s *noopStreamServer[T]) Send(e *T) error {
	s.sent = append(s.sent, e)
	return nil
}
func (s *noopStreamServer[T]) SetHeader(metadata.MD) error  { return nil }
func (s *noopStreamServer[T]) SendHeader(metadata.MD) error { return nil }
func (s *noopStreamServer[T]) SetTrailer(metadata.MD)       {}
func (s *noopStreamServer[T]) SendMsg(any) error            { return nil }
func (s *noopStreamServer[T]) RecvMsg(any) error            { return nil }

// fakeIndex stubs the changelog methods; other interface methods panic
// via the embedded nil interface so handlers can't touch them undetected.
type fakeIndex struct {
	sharding.RemoteIndexIncomingRepo

	startErr           error
	replicaSnapshotErr error
	releaseErr         error
	fileMetadataErr    error
	fileContent        string
	fileErr            error
	getErr             error
	snapshotLSN        uint64
	snapshotErr        error
	finalizeLSN        uint64
	finalizeErr        error
	stopErr            error

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

func (f *fakeIndex) IncomingCreateReplicaSnapshot(_ context.Context, _, _ string) ([]string, error) {
	return nil, f.replicaSnapshotErr
}

func (f *fakeIndex) IncomingReleaseReplicaSnapshot(_ context.Context, _ string) error {
	return f.releaseErr
}

func (f *fakeIndex) IncomingGetReplicaSnapshotFileMetadata(_ context.Context, _, _ string) (file.FileMetadata, error) {
	return file.FileMetadata{}, f.fileMetadataErr
}

func (f *fakeIndex) IncomingGetReplicaSnapshotFile(_ context.Context, _, _ string) (io.ReadCloser, error) {
	if f.fileErr != nil {
		return nil, f.fileErr
	}
	return io.NopCloser(strings.NewReader(f.fileContent)), nil
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
	if f.getErr != nil {
		return nil, f.getErr
	}
	// A nil tailer with a nil error panics the handler's deferred Close.
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

// The code a refused shard call answers with decides whether the movement waits
// or spends one of its errors: FailedPrecondition defers, anything else counts
// against the budget and auto-cancels the movement at 50.
//
// Every method the service has is listed, so one whose shard call starts taking
// the namespace check already answers with the code the movement reads. Which
// refusals defer is replication.IsReversibleRefusal's own table, so each method
// is driven with one of each kind rather than the whole list.
func TestShardCallErrorCode(t *testing.T) {
	const (
		indexName = "MyClass"
		shardName = "shard1"
		opID      = "op-1"
		fileName  = "segment-1.db"
	)
	ctx := context.Background()

	entryPoints := []struct {
		name string
		// index is the fake whose shard call fails with refusal.
		index func(refusal error) *fakeIndex
		call  func(svc *FileReplicationService) error
	}{
		{
			name:  "creating the replica snapshot",
			index: func(refusal error) *fakeIndex { return &fakeIndex{replicaSnapshotErr: refusal} },
			call: func(svc *FileReplicationService) error {
				_, err := svc.CreateReplicaSnapshot(ctx, &pb.CreateReplicaSnapshotRequest{
					IndexName: indexName, ShardName: shardName, OpId: opID,
				})
				return err
			},
		},
		{
			name:  "releasing the replica snapshot",
			index: func(refusal error) *fakeIndex { return &fakeIndex{releaseErr: refusal} },
			call: func(svc *FileReplicationService) error {
				_, err := svc.ReleaseReplicaSnapshot(ctx, &pb.ReleaseReplicaSnapshotRequest{
					IndexName: indexName, OpId: opID,
				})
				return err
			},
		},
		{
			name:  "reading the snapshot file metadata",
			index: func(refusal error) *fakeIndex { return &fakeIndex{fileMetadataErr: refusal} },
			call: func(svc *FileReplicationService) error {
				_, err := svc.GetReplicaSnapshotFileMetadata(ctx, &pb.GetReplicaSnapshotFileMetadataRequest{
					IndexName: indexName, OpId: opID, FileName: fileName,
				})
				return err
			},
		},
		{
			name:  "opening the snapshot file",
			index: func(refusal error) *fakeIndex { return &fakeIndex{fileErr: refusal} },
			call: func(svc *FileReplicationService) error {
				return svc.GetReplicaSnapshotFile(&pb.GetReplicaSnapshotFileRequest{
					IndexName: indexName, OpId: opID, FileName: fileName,
				}, &noopStreamServer[pb.FileChunk]{ctx: ctx})
			},
		},
		{
			name:  "starting change capture",
			index: func(refusal error) *fakeIndex { return &fakeIndex{startErr: refusal} },
			call: func(svc *FileReplicationService) error {
				_, err := svc.StartChangeCapture(ctx, &pb.StartChangeCaptureRequest{
					IndexName: indexName, ShardName: shardName, OpId: opID,
				})
				return err
			},
		},
		{
			name:  "opening the change-log tailer",
			index: func(refusal error) *fakeIndex { return &fakeIndex{getErr: refusal} },
			call: func(svc *FileReplicationService) error {
				return svc.GetChangeLog(&pb.GetChangeLogRequest{
					IndexName: indexName, ShardName: shardName, OpId: opID,
				}, &noopStreamServer[pb.ChangeLogStreamEntry]{ctx: ctx})
			},
		},
		{
			name:  "snapshotting the change-log LSN",
			index: func(refusal error) *fakeIndex { return &fakeIndex{snapshotErr: refusal} },
			call: func(svc *FileReplicationService) error {
				_, err := svc.SnapshotChangeLogLSN(ctx, &pb.SnapshotChangeLogLSNRequest{
					IndexName: indexName, ShardName: shardName, OpId: opID,
				})
				return err
			},
		},
		{
			name:  "finalizing the change log",
			index: func(refusal error) *fakeIndex { return &fakeIndex{finalizeErr: refusal} },
			call: func(svc *FileReplicationService) error {
				_, err := svc.FinalizeChangeLog(ctx, &pb.FinalizeChangeLogRequest{
					IndexName: indexName, ShardName: shardName, OpId: opID,
				})
				return err
			},
		},
		{
			name:  "stopping change capture",
			index: func(refusal error) *fakeIndex { return &fakeIndex{stopErr: refusal} },
			call: func(svc *FileReplicationService) error {
				_, err := svc.StopChangeCapture(ctx, &pb.StopChangeCaptureRequest{
					IndexName: indexName, ShardName: shardName, OpId: opID,
				})
				return err
			},
		},
	}

	refusals := []struct {
		name string
		// Wrapped, because the index returns the sentinel under its own context.
		refusal error
		want    codes.Code
	}{
		{
			name:    "a refusal that can be undone",
			refusal: fmt.Errorf("get shard %q: %w", shardName, namespaces.ErrNamespaceSuspended),
			want:    codes.FailedPrecondition,
		},
		{
			name:    "an unrelated failure",
			refusal: errors.New("boom"),
			want:    codes.Internal,
		},
	}

	assertCode := func(t *testing.T, ep func(*FileReplicationService) error, svc *FileReplicationService,
		refusal error, want codes.Code,
	) {
		t.Helper()

		err := ep(svc)
		require.Error(t, err)
		require.Equal(t, want, status.Code(err))
		// The consumer matches the sentinel on the message, so dropping it from
		// the wrapping cancels the movement just as a wrong code does.
		require.Contains(t, status.Convert(err).Message(), refusal.Error())
	}

	for _, ep := range entryPoints {
		for _, tc := range refusals {
			t.Run(ep.name+" refused by "+tc.name, func(t *testing.T) {
				svc := newService(t, map[string]*fakeIndex{indexName: ep.index(tc.refusal)})

				assertCode(t, ep.call, svc, tc.refusal, tc.want)
			})
		}
	}

	// The one refusal that carries a namespace sentinel and still must not defer:
	// a deleting namespace never becomes active again, so a movement waiting for
	// it would never end. Driven through one entry point, since the code the
	// helper returns does not vary by method.
	t.Run("a deleting namespace does not defer", func(t *testing.T) {
		refusal := fmt.Errorf("get shard %q: %w", shardName, namespaces.ErrNamespaceDeleting)
		ep := entryPoints[0]
		svc := newService(t, map[string]*fakeIndex{indexName: ep.index(refusal)})

		assertCode(t, ep.call, svc, refusal, codes.Internal)
	})

	// The refusal the file service had before namespaces, kept so a rewrite of
	// the shared list cannot drop it.
	t.Run("a structural vector op defers", func(t *testing.T) {
		refusal := fmt.Errorf("halt shard: %w", enterrors.ErrShardBusyStructuralOp)
		ep := entryPoints[0]
		svc := newService(t, map[string]*fakeIndex{indexName: ep.index(refusal)})

		assertCode(t, ep.call, svc, refusal, codes.FailedPrecondition)
	})
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
	}, &noopStreamServer[pb.ChangeLogStreamEntry]{ctx: context.Background()})
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
	}, &noopStreamServer[pb.ChangeLogStreamEntry]{ctx: context.Background()})
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

// The target reassembles the file from the offsets it is sent, and stops at the
// chunk flagged EOF.
func TestGetReplicaSnapshotFile_HappyPath(t *testing.T) {
	svc := newService(t, map[string]*fakeIndex{"MyClass": {fileContent: "hello"}})
	stream := &noopStreamServer[pb.FileChunk]{ctx: context.Background()}

	err := svc.GetReplicaSnapshotFile(&pb.GetReplicaSnapshotFileRequest{
		IndexName: "MyClass",
		OpId:      "op-1",
		FileName:  "segment-1.db",
	}, stream)
	require.NoError(t, err)
	require.Len(t, stream.sent, 2)
	require.Equal(t, []byte("hello"), stream.sent[0].Data)
	require.Equal(t, int64(0), stream.sent[0].Offset)
	require.False(t, stream.sent[0].Eof)
	require.Empty(t, stream.sent[1].Data)
	require.Equal(t, int64(5), stream.sent[1].Offset)
	require.True(t, stream.sent[1].Eof)
}

func TestGetReplicaSnapshotFile_CompressionUnsupported(t *testing.T) {
	svc := newService(t, map[string]*fakeIndex{"MyClass": {}})

	err := svc.GetReplicaSnapshotFile(&pb.GetReplicaSnapshotFileRequest{
		IndexName:   "MyClass",
		OpId:        "op-1",
		FileName:    "segment-1.db",
		Compression: pb.CompressionType_COMPRESSION_TYPE_GZIP,
	}, &noopStreamServer[pb.FileChunk]{ctx: context.Background()})
	require.Error(t, err)
	require.Equal(t, codes.Unimplemented, status.Code(err))
}
