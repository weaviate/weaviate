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

package rpc

import (
	"context"
	"fmt"
	"sync"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const serviceConfig = `
{
	"methodConfig": [
		{
			"name": [
				{
					"service": "weaviate.internal.cluster.ClusterService", "method": "Apply"
				},
				{
					"service": "weaviate.internal.cluster.ClusterService", "method": "Query"
				}
			],
			"waitForReady": true,
			"retryPolicy": {
				"MaxAttempts": 5,
				"BackoffMultiplier": 2,
				"InitialBackoff": "0.5s",
				"MaxBackoff": "5s",
				"RetryableStatusCodes": [
					"ABORTED",
					"RESOURCE_EXHAUSTED",
					"INTERNAL",
					"UNAVAILABLE",
					"NOT_FOUND"
				]
			}
		}
	]
}`

type rpcAddressResolver interface {
	// Address returns the RPC address corresponding to the given Raft address.
	Address(raftAddress string) (string, error)
}

// Client is used for communication with remote nodes in a RAFT cluster
// It wraps the gRPC client to our gRPC server that is running on the raft port on each node
type Client struct {
	addrResolver rpcAddressResolver
	// connLock is used to ensure that we are trying to establish/close the connection to the leader while no request
	// are in progress
	connLock sync.Mutex
	// leaderRaftAddr is the raft address of the current leader. It is updated at the same time as the leaderConn below
	// when leader is changing
	leaderRaftAddr string
	// leaderConn is the gRPC client to the leader node of the RAFT cluster. It is used for queries that must be sent to
	// the leader to have strong read consistency
	leaderRpcConn *grpc.ClientConn
	// rpcMessageMaxSize is the maximum size allows for gRPC call. As we re-instantiate the client when the leader
	// change we store that setting to re-use it. We set a custom limit to ensure that big queries that would exceed the
	// default maximum can still get through
	rpcMessageMaxSize int
}

// NewClient returns a Client using the rpcAddressResolver to resolve raft nodes and configured with rpcMessageMaxSize
func NewClient(r rpcAddressResolver, rpcMessageMaxSize int) *Client {
	return &Client{addrResolver: r, rpcMessageMaxSize: rpcMessageMaxSize}
}

// Join will contact the node at leaderRaftAddr and try to join this node to the cluster leaded by leaderRaftAddress using req
// Returns the server response to the join request
// Returns an error if an RPC connection to leaderRaftAddr can't be established
// Returns an error if joining the node fails
func (cl *Client) Join(ctx context.Context, leaderRaftAddr string, req *cmd.JoinPeerRequest) (*cmd.JoinPeerResponse, error) {
	conn, err := cl.getConn(leaderRaftAddr)
	if err != nil {
		return nil, err
	}
	c := cmd.NewClusterServiceClient(conn)
	return c.JoinPeer(ctx, req)
}

// Notify will contact the node at remoteAddr using the configured resolver and notify it of it's readiness to join a
// cluster using req
// Returns the server response to the notify request
// Returns an error if remoteAddr is not resolvable
// Returns an error if remoteAddr after resolve is not dial-able
// Returns an error if notifying the node fails. Note that Notify will not return an error if the node has notified
// itself already or if the remote node is already bootstrapped
// If the remote node is already bootstrapped/running a cluster, nodes should call Join instead
// Once a remote node has reached the sufficient amount of ready nodes (bootstrap_expect) it will initiate a cluster
// bootstrap process
func (cl *Client) Notify(ctx context.Context, remoteAddr string, req *cmd.NotifyPeerRequest) (*cmd.NotifyPeerResponse, error) {
	// Explicitly instantiate a connection here and avoid using cl.leaderRpcConn because notify will be called for each
	// remote node we have available to build a RAFT cluster. This connection is short lived to this function only
	addr, err := cl.addrResolver.Address(remoteAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve address: %w", err)
	}

	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()
	c := cmd.NewClusterServiceClient(conn)
	return c.NotifyPeer(ctx, req)
}

// Remove will contact the node at leaderRaftAddr and remove the client node from the RAFT cluster using req
// Returns the server response to the remove request
// Returns an error if an RPC connection to leaderRaftAddr can't be established
func (cl *Client) Remove(ctx context.Context, leaderRaftAddr string, req *cmd.RemovePeerRequest) (*cmd.RemovePeerResponse, error) {
	conn, err := cl.getConn(leaderRaftAddr)
	if err != nil {
		return nil, err
	}
	c := cmd.NewClusterServiceClient(conn)
	return c.RemovePeer(ctx, req)
}

// Apply will contact the node at leaderRaftAddr and send req to be applied in the RAFT store
// It does not take a context and will instead use a background context to ensure that request to apply a change isn't
// cancelled by a context exiting early/timeouting/failing.
// TODO: Check here if we want to keep the context in background or accept a callee context.
// Returns the server response to the apply request
// Returns an error if an RPC connection to leaderRaftAddr can't be established
// Returns an error if the apply command fails
func (cl *Client) Apply(leaderRaftAddr string, req *cmd.ApplyRequest) (*cmd.ApplyResponse, error) {
	ctx := context.Background()
	conn, err := cl.getConn(leaderRaftAddr)
	if err != nil {
		return nil, err
	}

	c := cmd.NewClusterServiceClient(conn)
	return c.Apply(ctx, req)
}

// Query will contact the node at leaderRaftAddr and send req to read data in the RAFT store
// Returns the server response to the query request
// Returns an error if an RPC connection to leaderRaftAddr can't be established
// Returns an error if the query command fails
func (cl *Client) Query(ctx context.Context, leaderRaftAddr string, req *cmd.QueryRequest) (*cmd.QueryResponse, error) {
	conn, err := cl.getConn(leaderRaftAddr)
	if err != nil {
		return nil, err
	}

	c := cmd.NewClusterServiceClient(conn)
	return c.Query(ctx, req)
}

// Close the client and allocated ressources
func (cl *Client) Close() error {
	if cl.leaderRpcConn != nil {
		return cl.leaderRpcConn.Close()
	}
	return nil
}

// getConn either returns the cached connection in the client to the leader or will instantiate a new one towards
// leaderRaftAddr and close the old one
// Returns the gRPC client connection to leaderRaftAddr
// Returns an error if an RPC connection to leaderRaftAddr can't be established
func (cl *Client) getConn(leaderRaftAddr string) (*grpc.ClientConn, error) {
	cl.connLock.Lock()
	defer cl.connLock.Unlock()

	if cl.leaderRpcConn != nil && leaderRaftAddr == cl.leaderRaftAddr {
		return cl.leaderRpcConn, nil
	}

	if cl.leaderRpcConn != nil {
		// close open conn if leader addr changed
		cl.leaderRpcConn.Close()
	}

	addr, err := cl.addrResolver.Address(leaderRaftAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve address: %w", err)
	}

	cl.leaderRpcConn, err = grpc.Dial(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(serviceConfig),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(cl.rpcMessageMaxSize)),
	)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}

	cl.leaderRaftAddr = leaderRaftAddr

	return cl.leaderRpcConn, nil
}
