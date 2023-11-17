//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package store

import (
	"context"
	"errors"
	"log"
	"net"

	cmd "github.com/weaviate/weaviate/cloud/proto/cluster"
	command "github.com/weaviate/weaviate/cloud/proto/cluster"
	"github.com/weaviate/weaviate/cloud/store"
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
	Execute(cmd *command.ApplyRequest) error
}

type Cluster struct {
	members    members
	executor   executor
	address    string
	ln         net.Listener
	grpcServer *grpc.Server
}

func NewCluster(ms members, ex executor, address string) Cluster {
	return Cluster{
		members:  ms,
		executor: ex,
		address:  address,
	}
}

func (c *Cluster) JoinPeer(_ context.Context, req *cmd.JoinPeerRequest) (*cmd.JoinPeerResponse, error) {
	log.Printf("server: join peer %+v\n", req)
	err := c.members.Join(req.Id, req.Address, req.Voter)
	if err == nil {
		return &cmd.JoinPeerResponse{}, nil
	}

	return &cmd.JoinPeerResponse{Leader: c.members.Leader()}, toRPCError(err)
}

func (c *Cluster) RemovePeer(_ context.Context, req *cmd.RemovePeerRequest) (*cmd.RemovePeerResponse, error) {
	log.Printf("server: remove peer %+v\n", req)
	err := c.members.Remove(req.Id)
	if err == nil {
		return &cmd.RemovePeerResponse{}, nil
	}
	return &cmd.RemovePeerResponse{Leader: c.members.Leader()}, toRPCError(err)
}

func (c *Cluster) NotifyPeer(_ context.Context, req *cmd.NotifyPeerRequest) (*cmd.NotifyPeerResponse, error) {
	log.Printf("server: join peer %+v\n", req)
	return &cmd.NotifyPeerResponse{}, toRPCError(c.members.Notify(req.Id, req.Address))
}

func (c *Cluster) Apply(_ context.Context, req *cmd.ApplyRequest) (*cmd.ApplyResponse, error) {
	err := c.executor.Execute(req)
	if err == nil {
		return &cmd.ApplyResponse{}, nil
	}
	return &cmd.ApplyResponse{Leader: c.members.Leader()}, toRPCError(err)
}

func (c *Cluster) Leader() string {
	return c.members.Leader()
}

func (c *Cluster) Open() error {
	log.Printf("server listening at %v", c.address)
	ln, err := net.Listen("tcp", c.address)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	c.ln = ln
	go c.serve()
	return nil
}

func (c *Cluster) Shutdown() {
	log.Printf("server shutdown")
	c.grpcServer.Stop()
}

func (c *Cluster) serve() error {
	c.grpcServer = grpc.NewServer()
	cmd.RegisterClusterServiceServer(c.grpcServer, c)
	if err := c.grpcServer.Serve(c.ln); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
	return nil
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
