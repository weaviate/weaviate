//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package transport

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type members interface {
	Join(id string, addr string, voter bool) error
	Notify(id string, addr string) error
	Remove(id string) error
	Leader() string
}

type executor interface {
	Execute(cmd *cmd.ApplyRequest) (uint64, error)
	Query(ctx context.Context, req *cmd.QueryRequest) (*cmd.QueryResponse, error)
}

type Service struct {
	members    members
	executor   executor
	address    string
	ln         net.Listener
	grpcServer *grpc.Server
	log        *slog.Logger
}

func New(ms members, ex executor, address string, l *slog.Logger) *Service {
	return &Service{
		members:  ms,
		executor: ex,
		address:  address,
		log:      l,
	}
}

func (s *Service) JoinPeer(_ context.Context, req *cmd.JoinPeerRequest) (*cmd.JoinPeerResponse, error) {
	err := s.members.Join(req.Id, req.Address, req.Voter)
	if err == nil {
		return &cmd.JoinPeerResponse{}, nil
	}

	return &cmd.JoinPeerResponse{Leader: s.members.Leader()}, toRPCError(err)
}

func (s *Service) RemovePeer(_ context.Context, req *cmd.RemovePeerRequest) (*cmd.RemovePeerResponse, error) {
	err := s.members.Remove(req.Id)
	if err == nil {
		return &cmd.RemovePeerResponse{}, nil
	}
	return &cmd.RemovePeerResponse{Leader: s.members.Leader()}, toRPCError(err)
}

func (s *Service) NotifyPeer(_ context.Context, req *cmd.NotifyPeerRequest) (*cmd.NotifyPeerResponse, error) {
	return &cmd.NotifyPeerResponse{}, toRPCError(s.members.Notify(req.Id, req.Address))
}

func (s *Service) Apply(_ context.Context, req *cmd.ApplyRequest) (*cmd.ApplyResponse, error) {
	v, err := s.executor.Execute(req)
	if err == nil {
		return &cmd.ApplyResponse{Version: v}, nil
	}
	return &cmd.ApplyResponse{Leader: s.members.Leader()}, toRPCError(err)
}

func (s *Service) Query(ctx context.Context, req *cmd.QueryRequest) (*cmd.QueryResponse, error) {
	resp, err := s.executor.Query(ctx, req)
	if err != nil {
		return &cmd.QueryResponse{}, toRPCError(err)
	}

	return resp, nil
}

func (s *Service) Leader() string {
	return s.members.Leader()
}

func (s *Service) Open() error {
	s.log.Info("starting cloud rpc server ...", "address", s.address)
	if s.address == "" {
		return fmt.Errorf("address of rpc server cannot be empty")
	}
	ln, err := net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("server tcp net.listen: %w", err)
	}

	s.ln = ln
	s.grpcServer = grpc.NewServer()
	cmd.RegisterClusterServiceServer(s.grpcServer, s)
	go func() {
		if err := s.grpcServer.Serve(s.ln); err != nil {
			s.log.Error("serving incoming requests: " + err.Error())
			panic("error accepting incoming requests")
		}
	}()
	return nil
}

func (s *Service) Close() {
	if s.grpcServer != nil {
		s.grpcServer.Stop()
	}
}

func toRPCError(err error) error {
	if err == nil {
		return nil
	}
	ec := codes.Internal
	if errors.Is(err, store.ErrNotLeader) {
		ec = codes.NotFound
	} else if errors.Is(err, store.ErrNotOpen) {
		ec = codes.Unavailable
	}
	return status.Error(ec, err.Error())
}
