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
	"fmt"
	"log"
	"net"

	"github.com/hashicorp/raft"
	cmd "github.com/weaviate/weaviate/cloud/proto/cluster"
	"google.golang.org/grpc"
)

var (
	// ErrNotLeader is returned when an operation can't be completed on a
	// follower or candidate node.
	ErrNotLeader      = errors.New("node is not the leader")
	errLeaderNotFound = errors.New("leader not found")
)

type Cluster struct {
	*raft.Raft
	address string
	ln      net.Listener
}

func (h Cluster) RemovePeer(_ context.Context, req *cmd.RemovePeerRequest) (*cmd.RemovePeerResponse, error) {
	log.Printf("server: remove peer %+v\n", req)
	return &cmd.RemovePeerResponse{}, h.Remove(req.Id)
}

func (h Cluster) JoinPeer(_ context.Context, req *cmd.JoinPeerRequest) (*cmd.JoinPeerResponse, error) {
	log.Printf("server: join peer %+v\n", req)
	return &cmd.JoinPeerResponse{}, h.Join(req.Id, req.Address, req.Voter)
}

func NewCluster(raft *raft.Raft, address string) Cluster {
	return Cluster{Raft: raft, address: address}
}

func (h Cluster) Join(id, addr string, voter bool) error {
	if h.Raft.State() != raft.Leader {
		return ErrNotLeader
	}
	cfg := h.Raft.GetConfiguration()
	if err := cfg.Error(); err != nil {
		return fmt.Errorf("get raft config: %w", err)
	}
	var fut raft.IndexFuture
	if voter {
		fut = h.Raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 0)
		if err := fut.Error(); err != nil {
			return fmt.Errorf("add voter: %w", err)
		}
	} else {
		fut = h.Raft.AddNonvoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 0)
		if err := fut.Error(); err != nil {
			return fmt.Errorf("add non voter: %w", err)
		}
	}
	return nil
}

func (h Cluster) Remove(id string) error {
	if h.Raft.State() != raft.Leader {
		return fmt.Errorf("node %v is not the leader %v", id, raft.Leader)
	}
	cfg := h.Raft.GetConfiguration()
	if err := cfg.Error(); err != nil {
		return fmt.Errorf("get raft config: %w", err)
	}

	fut := h.Raft.RemoveServer(raft.ServerID(id), 0, 0)
	if err := fut.Error(); err != nil {
		return fmt.Errorf("add voter: %w", err)
	}
	return nil
}

func (h Cluster) Stats() map[string]string {
	return h.Raft.Stats()
}

func (h Cluster) Open() error {
	log.Printf("server listening at %v", h.address)
	ln, err := net.Listen("tcp", h.address)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	h.ln = ln
	go h.serve()
	return nil
}

func (h Cluster) serve() error {
	s := grpc.NewServer()
	cmd.RegisterClusterServiceServer(s, h)
	if err := s.Serve(h.ln); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
	return nil
}
