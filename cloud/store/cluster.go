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
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// ErrNotLeader is returned when an operation can't be completed on a
	// follower or candidate node.
	ErrNotLeader = errors.New("node is not the leader")
	ErrNotOpen   = errors.New("store not open")
)

type members interface {
	Join(id, addr string, voter bool) error
	Notify(id, addr string) error
	Remove(id string) error
	Leader() string
}

type Cluster struct {
	members members
	address string
	rpcPort string
	isLocal bool // localhost cluster used for testing only
	ln      net.Listener
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

func NewCluster(members members, address, rpcPort string) Cluster {
	return Cluster{
		members: members,
		address: address,
		rpcPort: rpcPort,
	}
}

func (c *Cluster) SetLocal() { // cluster is running on localhost for testing purposes
	c.isLocal = true
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

func (c *Cluster) serve() error {
	s := grpc.NewServer()
	cmd.RegisterClusterServiceServer(s, c)
	if err := s.Serve(c.ln); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
	return nil
}

func toRPCError(err error) error {
	if err == nil {
		return nil
	}
	ec := codes.Internal
	if errors.Is(err, ErrNotLeader) {
		ec = codes.NotFound
	} else if errors.Is(err, ErrNotOpen) {
		ec = codes.Unavailable
	}
	return status.Error(ec, err.Error())
}
