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
	"fmt"
	"log"
	"net"
	"strconv"

	cmd "github.com/weaviate/weaviate/cloud/proto/cluster"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type rpcAddressResolver interface {
	// Address returns the RPC address corresponding to the given Raft address.
	Address(raftAddress string) (string, error)
}

// Client is used for communication with remote nodes in a RAFT cluster.
type Client struct {
	rpc rpcAddressResolver
}

func NewClient(r rpcAddressResolver) *Client {
	return &Client{rpc: r}
}

// Join joins this node to an existing cluster identified by its leader's address.
// If a new leader has been elected, the request is redirected to the new leader.
func (cl *Client) Join(ctx context.Context, leaderAddr string, req *cmd.JoinPeerRequest) (*cmd.JoinPeerResponse, error) {
	log.Printf("client join: %s %+v\n", leaderAddr, req)
	addr, err := cl.rpc.Address(leaderAddr)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	c := cmd.NewClusterServiceClient(conn)
	return c.JoinPeer(ctx, req)
}

// Notify informs a remote node rAddr of this node's readiness to join.
func (cl *Client) Notify(ctx context.Context, rAddr string, req *cmd.NotifyPeerRequest) (*cmd.NotifyPeerResponse, error) {
	log.Printf("client notify: %s %+v\n", rAddr, req)
	addr, err := cl.rpc.Address(rAddr)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	c := cmd.NewClusterServiceClient(conn)
	return c.NotifyPeer(ctx, req)
}

// Remove removes this node from an existing cluster identified by its leader's address.
// If a new leader has been elected, the request is redirected to the new leader.
func (cl *Client) Remove(ctx context.Context, leaderAddress string, req *cmd.RemovePeerRequest) (*cmd.RemovePeerResponse, error) {
	log.Printf("client remove: %s %+v\n", leaderAddress, req)
	addr, err := cl.rpc.Address(leaderAddress)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	c := cmd.NewClusterServiceClient(conn)
	return c.RemovePeer(ctx, req)
}

func (cl *Client) Apply(leaderAddr string, req *cmd.ApplyRequest) (*cmd.ApplyResponse, error) {
	log.Printf("client apply on remote node: %s req-type: %v class: %v\n", leaderAddr, req.Type, req.Class)

	ctx := context.Background()
	addr, err := cl.rpc.Address(leaderAddr)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	c := cmd.NewClusterServiceClient(conn)
	return c.Apply(ctx, req)
}

func NewRPCResolver(isLocalHost bool, rpcPort string) rpcAddressResolver {
	return &rpcResolver{isLocalCluster: isLocalHost, rpcPort: rpcPort}
}

type rpcResolver struct {
	isLocalCluster bool
	rpcPort        string
}

// rpcAddressFromRAFT returns the RPC address based on the provided RAFT address.
// In a real cluster, the RPC port is the same for all nodes.
// In a local environment, the RAFT ports need to be different. Specifically,
// we calculate the RPC port as the RAFT port + 1.
func (cl *rpcResolver) Address(raftAddr string) (string, error) {
	host, port, err := net.SplitHostPort(raftAddr)
	if err != nil {
		return "", err
	}
	if !cl.isLocalCluster {
		return fmt.Sprintf("%s:%s", host, cl.rpcPort), nil
	}
	iPort, err := strconv.Atoi(port)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s:%d", host, iPort+1), nil
}
