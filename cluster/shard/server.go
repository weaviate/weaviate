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

package shard

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/schema"
	shardproto "github.com/weaviate/weaviate/cluster/shard/proto"
	"github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/entities/storagestate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NotLeaderRPCCode is the gRPC status code returned when this node is not the leader.
// Using ResourceExhausted to match the pattern used in cluster/rpc/server.go
const NotLeaderRPCCode = codes.ResourceExhausted

// defaultFileChunkSize is the default size of chunks sent when streaming
// snapshot files to followers (64KB).
const defaultFileChunkSize = 64 * 1024

// maxConcurrentSnapshots limits the number of concurrent transfer snapshot
// creations to prevent overwhelming the leader.
const maxConcurrentSnapshots = 3

// Server implements the ShardReplicationService gRPC server.
// It receives forwarded requests from followers and applies them to the local RAFT cluster.
type Server struct {
	shardproto.UnimplementedShardReplicationServiceServer
	registry    *Registry
	logger      logrus.FieldLogger
	snapshots   sync.Map      // snapshotID → *activeSnapshot
	snapshotSem chan struct{} // semaphore limiting concurrent snapshot creations
}

// activeSnapshot tracks a transfer snapshot that has been created but not yet released.
type activeSnapshot struct {
	dir   string // staging directory path
	class string
	shard string
}

// NewServer creates a new gRPC server for shard replication.
func NewServer(registry *Registry, logger logrus.FieldLogger) *Server {
	return &Server{
		registry:    registry,
		logger:      logger.WithField("component", "shard_rpc_server"),
		snapshotSem: make(chan struct{}, maxConcurrentSnapshots),
	}
}

// Apply handles incoming RAFT apply requests from followers.
//
// A gRPC unary error response carries no message body, so no ApplyResponse
// fields (the wire-level Leader hint is vestigial) accompany an error — the
// caller re-resolves the leader from its own Store on retry.
func (s *Server) Apply(ctx context.Context, req *shardproto.ApplyRequest) (*shardproto.ApplyResponse, error) {
	store := s.registry.GetStore(req.Class, req.Shard)
	if store == nil {
		return nil, status.Errorf(codes.NotFound, "store not found for %s/%s", req.Class, req.Shard)
	}
	v, err := store.Apply(ctx, req)
	if err != nil {
		return nil, toRPCError(err)
	}
	return &shardproto.ApplyResponse{Version: v}, nil
}

// CreateTransferSnapshot creates a hardlink snapshot of a shard's files
// for out-of-band state transfer.
func (s *Server) CreateTransferSnapshot(ctx context.Context, req *shardproto.CreateTransferSnapshotRequest) (*shardproto.CreateTransferSnapshotResponse, error) {
	// Rate-limit concurrent snapshot creations to prevent overwhelming the leader.
	select {
	case s.snapshotSem <- struct{}{}:
		defer func() { <-s.snapshotSem }()
	default:
		return nil, status.Errorf(codes.ResourceExhausted, "too many concurrent snapshot transfers")
	}

	store := s.registry.GetStore(req.Class, req.Shard)
	if store == nil {
		return nil, status.Errorf(codes.NotFound, "store not found for %s/%s", req.Class, req.Shard)
	}

	snap, err := store.CreateTransferSnapshot(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create transfer snapshot: %v", err)
	}

	// Track the snapshot so GetSnapshotFile and ReleaseTransferSnapshot can find it.
	s.snapshots.Store(snap.ID, &activeSnapshot{
		dir:   snap.Dir,
		class: req.Class,
		shard: req.Shard,
	})

	// Convert file list to proto.
	files := make([]*shardproto.SnapshotFileInfo, len(snap.Files))
	for i, f := range snap.Files {
		files[i] = &shardproto.SnapshotFileInfo{
			Name:  f.Name,
			Size:  f.Size,
			Crc32: f.CRC32,
		}
	}

	return &shardproto.CreateTransferSnapshotResponse{
		SnapshotId: snap.ID,
		Files:      files,
	}, nil
}

// GetSnapshotFile streams a file from a previously created transfer snapshot.
func (s *Server) GetSnapshotFile(req *shardproto.GetSnapshotFileRequest, stream grpc.ServerStreamingServer[shardproto.SnapshotFileChunk]) error {
	val, ok := s.snapshots.Load(req.SnapshotId)
	if !ok {
		return status.Errorf(codes.NotFound, "snapshot %s not found", req.SnapshotId)
	}
	snap := val.(*activeSnapshot)

	filePath := filepath.Join(snap.dir, req.FileName)

	// Validate the resolved path is within the staging directory to prevent
	// path traversal attacks.
	absDir, err := filepath.Abs(snap.dir)
	if err != nil {
		return status.Errorf(codes.Internal, "resolve staging dir: %v", err)
	}
	absFile, err := filepath.Abs(filePath)
	if err != nil {
		return status.Errorf(codes.Internal, "resolve file path: %v", err)
	}
	if !strings.HasPrefix(absFile, absDir+string(filepath.Separator)) {
		return status.Errorf(codes.InvalidArgument, "file name escapes snapshot directory")
	}

	f, err := os.Open(filePath)
	if err != nil {
		return status.Errorf(codes.NotFound, "open snapshot file %s: %v", req.FileName, err)
	}
	defer f.Close()

	buf := make([]byte, defaultFileChunkSize)
	offset := int64(0)

	for {
		n, err := f.Read(buf)
		eof := errors.Is(err, io.EOF)

		if err != nil && !eof {
			return status.Errorf(codes.Internal, "read file %s: %v", req.FileName, err)
		}

		if n == 0 && !eof {
			return status.Errorf(codes.Internal, "unexpected zero-byte read without EOF for file %s", req.FileName)
		}

		if err := stream.Send(&shardproto.SnapshotFileChunk{
			Offset: offset,
			Data:   buf[:n],
			Eof:    eof,
		}); err != nil {
			return err
		}
		offset += int64(n)

		if eof {
			return nil
		}
	}
}

// ReleaseTransferSnapshot cleans up the staging directory for a transfer snapshot.
func (s *Server) ReleaseTransferSnapshot(ctx context.Context, req *shardproto.ReleaseTransferSnapshotRequest) (*shardproto.ReleaseTransferSnapshotResponse, error) {
	val, ok := s.snapshots.Load(req.SnapshotId)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "snapshot %s not found", req.SnapshotId)
	}
	snap := val.(*activeSnapshot)

	store := s.registry.GetStore(snap.class, snap.shard)
	if store == nil {
		return nil, status.Errorf(codes.Internal, "store not found for %s/%s", snap.class, snap.shard)
	}

	if err := store.ReleaseTransferSnapshot(req.SnapshotId); err != nil {
		// Don't remove from map on failure — allows retry cleanup.
		return nil, status.Errorf(codes.Internal, "release transfer snapshot: %v", err)
	}

	// Only remove from tracking after successful cleanup.
	s.snapshots.Delete(req.SnapshotId)
	return &shardproto.ReleaseTransferSnapshotResponse{}, nil
}

// GetLastAppliedIndex returns the catch-up watermark for a shard: the index a
// follower must wait for locally before a read observes every acknowledged
// write. Apply acks at quorum commit, so the applied index alone can lag an
// acked write — the handler reports the committed-staged watermark when it is
// ahead (a follower waiting on it waits for entries it is guaranteed to
// receive). When VerifyLeader is true, the handler first drives the full
// linearizable read barrier (quorum-verified leadership plus the leader's own
// applied-wait), which is required for linearizable (STRONG) reads.
func (s *Server) GetLastAppliedIndex(ctx context.Context, req *shardproto.GetLastAppliedIndexRequest) (*shardproto.GetLastAppliedIndexResponse, error) {
	store := s.registry.GetStore(req.Class, req.Shard)
	if store == nil {
		return nil, status.Errorf(codes.NotFound, "store not found for %s/%s", req.Class, req.Shard)
	}
	if req.VerifyLeader {
		if err := store.VerifyLeader(ctx); err != nil {
			return nil, toRPCError(err)
		}
	}
	idx := store.LastAppliedIndex()
	if ci := store.CommittedIndex(); ci > idx {
		idx = ci
	}
	return &shardproto.GetLastAppliedIndexResponse{
		LastAppliedIndex: idx,
	}, nil
}

// toRPCError returns a gRPC error with the right error code based on the
// error. The codes are load-bearing for the write path's retry behavior:
// isRetryableApplyErr classifies a forwarded attempt purely by status code,
// so every transient leadership/availability condition the Store can raise
// must map to NotLeaderRPCCode or codes.Unavailable here — anything falling
// through to codes.Internal is permanent for the client.
func toRPCError(err error) error {
	if err == nil {
		return nil
	}

	var ec codes.Code
	switch {
	case errors.Is(err, ErrNotLeader), errors.Is(err, ErrLeadershipLost),
		errors.Is(err, types.ErrNotLeader), errors.Is(err, types.ErrLeaderNotFound):
		ec = NotLeaderRPCCode
	case errors.Is(err, ErrProposalBackpressure),
		errors.Is(err, ErrNotStarted), errors.Is(err, ErrAlreadyClosed),
		errors.Is(err, ErrGroupFailed),
		errors.Is(err, types.ErrNotOpen):
		// Transient node conditions: retryable, after re-resolving the leader.
		// A failed group (panic-stopped store) belongs here too — the group
		// lives on on its other voters.
		ec = codes.Unavailable
	case errors.Is(err, ErrCommandTooLarge):
		// Permanent: the command can never commit, so the client must not
		// retry it. Deliberately NOT the gRPC-conventional ResourceExhausted —
		// that is NotLeaderRPCCode here, which classifies as retryable.
		ec = codes.InvalidArgument
	case errors.Is(err, storagestate.ErrStatusReadOnly), errors.Is(err, ErrClassDropped):
		// Admission rejections (leader reject-fast): non-retryable, the
		// client must see the reason. Pressure lasts minutes and a class
		// drop is deliberate — retrying against the same leader only burns
		// the caller's budget. FailedPrecondition is never in the client
		// classifier's retryable code set (isRetryableApplyErr agrees on the
		// local route).
		ec = codes.FailedPrecondition
	case errors.Is(err, schema.ErrMTDisabled):
		ec = codes.FailedPrecondition
	case strings.Contains(err.Error(), types.ErrNotFound.Error()):
		ec = codes.NotFound
	default:
		ec = codes.Internal
	}
	return status.Error(ec, err.Error())
}
