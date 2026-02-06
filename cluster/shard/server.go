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
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/schema"
	shardproto "github.com/weaviate/weaviate/cluster/shard/proto"
	"github.com/weaviate/weaviate/cluster/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NotLeaderRPCCode is the gRPC status code returned when this node is not the leader.
// Using ResourceExhausted to match the pattern used in cluster/rpc/server.go
const NotLeaderRPCCode = codes.ResourceExhausted

// Server implements the ShardReplicationService gRPC server.
// It receives forwarded requests from followers and applies them to the local RAFT cluster.
type Server struct {
	shardproto.UnimplementedShardReplicationServiceServer
	registry *Registry
	logger   logrus.FieldLogger
}

// NewServer creates a new gRPC server for shard replication.
func NewServer(registry *Registry, logger logrus.FieldLogger) *Server {
	return &Server{
		registry: registry,
		logger:   logger.WithField("component", "shard_rpc_server"),
	}
}

// Apply handles incoming RAFT apply requests from followers.
func (s *Server) Apply(ctx context.Context, req *shardproto.ApplyRequest) (*shardproto.ApplyResponse, error) {
	store := s.registry.GetStore(req.Class, req.Shard)
	if store == nil {
		err := errors.New("store not found")
		return &shardproto.ApplyResponse{Leader: s.registry.Leader(req.Class, req.Shard)}, toRPCError(err)
	}
	v, err := store.Apply(ctx, req)
	if err != nil {
		return &shardproto.ApplyResponse{Leader: s.registry.Leader(req.Class, req.Shard)}, toRPCError(err)
	}
	return &shardproto.ApplyResponse{Version: v}, nil
}

// toRPCError returns a gRPC error with the right error code based on the error.
func toRPCError(err error) error {
	if err == nil {
		return nil
	}

	var ec codes.Code
	switch {
	case errors.Is(err, types.ErrNotLeader), errors.Is(err, types.ErrLeaderNotFound):
		ec = NotLeaderRPCCode
	case errors.Is(err, types.ErrNotOpen):
		ec = codes.Unavailable
	case errors.Is(err, schema.ErrMTDisabled):
		ec = codes.FailedPrecondition
	case strings.Contains(err.Error(), types.ErrNotFound.Error()):
		ec = codes.NotFound
	default:
		ec = codes.Internal
	}
	return status.Error(ec, err.Error())
}
