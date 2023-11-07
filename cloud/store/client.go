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
	"time"

	cmd "github.com/weaviate/weaviate/cloud/proto/cluster"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Client is used for communication with remote nodes in a RAFT cluster.
type Client struct {
	localService Cluster
}

func NewClient(localService Cluster) *Client {
	return &Client{localService: localService}
}

// Join joins this node to an existing cluster identified by its leader's address.
// If a new leader has been elected, the request is redirected to the new leader.
func (cl *Client) Join(ctx context.Context, leaderAddr string, req *cmd.JoinPeerRequest) (*cmd.JoinPeerResponse, error) {
	resp, err := cl.join(ctx, leaderAddr, req)
	if err != nil {
		st := status.Convert(err)
		if leader := resp.GetLeader(); st.Code() == codes.NotFound && leader != "" {
			return cl.join(ctx, leader, req)
		}
	}

	return resp, err
}

// Remove removes this node from an existing cluster identified by its leader's address.
// If a new leader has been elected, the request is redirected to the new leader.
func (cl *Client) Remove(ctx context.Context, leaderAddress string, req *cmd.RemovePeerRequest) (*cmd.RemovePeerResponse, error) {
	resp, err := cl.remove(ctx, leaderAddress, req)
	if err != nil {
		st := status.Convert(err)
		if leader := resp.GetLeader(); st.Code() == codes.NotFound && leader != "" {
			return cl.remove(ctx, leader, req)
		}
	}

	return resp, nil
}

// Notify informs a remote node rAddr of this node's readiness to join.
func (cl *Client) Notify(ctx context.Context, rAddr string, req *cmd.NotifyPeerRequest) (*cmd.NotifyPeerResponse, error) {
	log.Printf("client join: %s %+v\n", rAddr, req)
	addr, err := cl.rpcAddressFromRAFT(rAddr)
	if err != nil {
		return nil, err
	}
	if addr == cl.localService.address {
		return cl.localService.NotifyPeer(ctx, req)
	}

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	c := cmd.NewClusterServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return c.NotifyPeer(ctx, req)
}

func (cl *Client) join(ctx context.Context, leaderAddr string, req *cmd.JoinPeerRequest) (*cmd.JoinPeerResponse, error) {
	log.Printf("client join: %s %+v\n", leaderAddr, req)
	addr, err := cl.rpcAddressFromRAFT(leaderAddr)
	if err != nil {
		return nil, err
	}
	if addr == cl.localService.address {
		return cl.localService.JoinPeer(ctx, req)
	}

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	c := cmd.NewClusterServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return c.JoinPeer(ctx, req)
}

func (cl *Client) remove(ctx context.Context, leaderAddress string, req *cmd.RemovePeerRequest) (*cmd.RemovePeerResponse, error) {
	log.Printf("client remove: %s %+v\n", leaderAddress, req)
	addr, err := cl.rpcAddressFromRAFT(leaderAddress)
	if err != nil {
		return nil, err
	}
	if cl.localService.address == addr {
		return cl.localService.RemovePeer(ctx, req)
	}

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	c := cmd.NewClusterServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return c.RemovePeer(ctx, req)
}

// rpcAddressFromRAFT returns the RPC address based on the provided RAFT address.
// In a real cluster, the RPC port is the same for all nodes.
// In a local environment, the RAFT ports need to be different. Specifically,
// we calculate the RPC port as the RAFT port + 1.
func (cl *Client) rpcAddressFromRAFT(raftAddr string) (string, error) {
	host, port, err := net.SplitHostPort(raftAddr)
	if err != nil {
		return "", err
	}
	if !cl.localService.isLocal {
		return fmt.Sprintf("%s:%s", host, cl.localService.rpcPort), nil
	}
	iPort, err := strconv.Atoi(port)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s:%d", host, iPort+1), nil
}
