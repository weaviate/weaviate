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

package rpc

import (
	"context"
	"errors"
	"fmt"
	"sync"

	grpc_sentry "github.com/johnbellone/grpc-middleware-sentry"
	"github.com/sirupsen/logrus"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// serviceConfig defines different retry policies for different RPC operation types:
//
// Apply/Query operations:
//   - Higher retry count (5) and longer backoff (15s max)
//   - Critical data operations that must succeed for cluster consistency
//
// Join/Remove/Notify operations:
//   - Lower retry count (3) and shorter backoff (3s max)
//   - Cluster management operations that should fail fast if nodes are unreachable
//   - Join uses RESOURCE_EXHAUSTED to tell nodes what the leader is, so we don't retry this code
//
// INTERNAL errors are never retried because they indicate programming errors or
// unexpected conditions that won't be resolved by retrying the same request
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
				"MaxBackoff": "15s",
				"RetryableStatusCodes": [
					"ABORTED",
					"RESOURCE_EXHAUSTED",					
					"UNAVAILABLE"
				]
			}
		},
		{
			"name": [
				{
					"service": "weaviate.internal.cluster.ClusterService", "method": "JoinPeer"
				},
				{
					"service": "weaviate.internal.cluster.ClusterService", "method": "NotifyPeer"
				},
				{
					"service": "weaviate.internal.cluster.ClusterService", "method": "RemovePeer"
				}
			],
			"waitForReady": true,
			"retryPolicy": {
				"MaxAttempts": 3,
				"BackoffMultiplier": 2,
				"InitialBackoff": "0.5s",
				"MaxBackoff": "3s",
				"RetryableStatusCodes": [
					"ABORTED",
					"UNAVAILABLE"
				]
			}
		}
	]
}`

type rpcAddressResolver interface {
	// Address returns the RPC address corresponding to the given Raft address.
	Address(raftAddress string) (string, error)
}

// refConn pairs the leader conn with a reference count so a leader change can
// retire the conn without closing it underneath an in-flight RPC.
type refConn struct {
	conn *grpc.ClientConn
	// addr is the raft address this conn was dialed for
	addr string
	// refs counts in-flight RPCs holding this conn; guarded by Client.connLock
	refs int
	// retired means a newer leader conn replaced this one; the last release closes it
	retired bool
}

// Client is used for communication with remote nodes in a RAFT cluster
// It wraps the gRPC client to our gRPC server that is running on the raft port on each node
type Client struct {
	addrResolver rpcAddressResolver
	// connLock guards leaderRpcConn and its refcount state
	connLock sync.Mutex
	// leaderRpcConn is the refcounted gRPC conn to the current RAFT leader. It is used for queries that must be sent
	// to the leader to have strong read consistency
	leaderRpcConn *refConn
	// rpcMessageMaxSize is the maximum size allows for gRPC call. As we re-instantiate the client when the leader
	// change we store that setting to re-use it. We set a custom limit to ensure that big queries that would exceed the
	// default maximum can still get through
	rpcMessageMaxSize int

	// sentryEnabled will configure the RPC client to set spans and captures traces using sentry SDK
	sentryEnabled bool

	// logger is the logger to log client warns etc.
	logger *logrus.Logger

	// testOnConnAcquired runs between conn acquisition and the RPC call so tests
	// can force a leader change into that window. Always nil in production.
	testOnConnAcquired func()
}

// NewClient returns a Client using the rpcAddressResolver to resolve raft nodes and configured with rpcMessageMaxSize
func NewClient(r rpcAddressResolver, rpcMessageMaxSize int, sentryEnabled bool, logger *logrus.Logger) *Client {
	return &Client{addrResolver: r, rpcMessageMaxSize: rpcMessageMaxSize, sentryEnabled: sentryEnabled, logger: logger}
}

// Join will contact the node at leaderRaftAddr and try to join this node to the cluster leaded by leaderRaftAddress using req
// Returns the server response to the join request
// Returns an error if an RPC connection to leaderRaftAddr can't be established
// Returns an error if joining the node fails
func (cl *Client) Join(ctx context.Context, leaderRaftAddr string, req *cmd.JoinPeerRequest) (*cmd.JoinPeerResponse, error) {
	conn, release, err := cl.getConn(ctx, leaderRaftAddr)
	if err != nil {
		return nil, err
	}
	defer release()

	return cmd.NewClusterServiceClient(conn).JoinPeer(ctx, req)
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

	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(serviceConfig),
	}
	if cl.sentryEnabled {
		options = append(options, grpc.WithUnaryInterceptor(grpc_sentry.UnaryClientInterceptor()))
	}

	conn, err := grpc.NewClient(addr, options...)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	return cmd.NewClusterServiceClient(conn).NotifyPeer(ctx, req)
}

// Remove will contact the node at leaderRaftAddr and remove the client node from the RAFT cluster using req
// Returns the server response to the remove request
// Returns an error if an RPC connection to leaderRaftAddr can't be established
func (cl *Client) Remove(ctx context.Context, leaderRaftAddr string, req *cmd.RemovePeerRequest) (*cmd.RemovePeerResponse, error) {
	conn, release, err := cl.getConn(ctx, leaderRaftAddr)
	if err != nil {
		return nil, err
	}
	defer release()

	return cmd.NewClusterServiceClient(conn).RemovePeer(ctx, req)
}

// Apply will contact the node at leaderRaftAddr and send req to be applied in the RAFT store
// Returns the server response to the apply request
// Returns an error if an RPC connection to leaderRaftAddr can't be established
// Returns an error if the apply command fails
func (cl *Client) Apply(ctx context.Context, leaderRaftAddr string, req *cmd.ApplyRequest) (*cmd.ApplyResponse, error) {
	conn, release, err := cl.getConn(ctx, leaderRaftAddr)
	if err != nil {
		return nil, err
	}
	defer release()

	return cmd.NewClusterServiceClient(conn).Apply(ctx, req)
}

// Query will contact the node at leaderRaftAddr and send req to read data in the RAFT store
// Returns the server response to the query request
// Returns an error if an RPC connection to leaderRaftAddr can't be established
// Returns an error if the query command fails
func (cl *Client) Query(ctx context.Context, leaderRaftAddr string, req *cmd.QueryRequest) (*cmd.QueryResponse, error) {
	conn, release, err := cl.getConn(ctx, leaderRaftAddr)
	if err != nil {
		return nil, err
	}
	defer release()

	if cl.testOnConnAcquired != nil {
		cl.testOnConnAcquired()
	}

	resp, err := cmd.NewClusterServiceClient(conn).Query(ctx, req)
	return resp, fromRPCError(err)
}

// Close the client and allocated resources
func (cl *Client) Close() {
	cl.connLock.Lock()
	defer cl.connLock.Unlock()

	if cl.leaderRpcConn == nil {
		return
	}
	cl.retireLocked()
}

// getConn returns the cached conn to the leader, or dials leaderRaftAddr and
// retires the old conn. Callers must invoke release once their RPC is done.
func (cl *Client) getConn(ctx context.Context, leaderRaftAddr string) (*grpc.ClientConn, func(), error) {
	cl.connLock.Lock()
	defer cl.connLock.Unlock()

	if cl.leaderRpcConn != nil && leaderRaftAddr == cl.leaderRpcConn.addr {
		conn, release := cl.acquireLocked()
		return conn, release, nil
	}

	if cl.leaderRpcConn != nil {
		cl.retireLocked()
	}

	addr, err := cl.addrResolver.Address(leaderRaftAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve address: %w", err)
	}

	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(serviceConfig),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(cl.rpcMessageMaxSize)),
	}

	if cl.sentryEnabled {
		options = append(options, grpc.WithUnaryInterceptor(grpc_sentry.UnaryClientInterceptor()))
	}

	conn, err := grpc.NewClient(addr, options...)
	if err != nil {
		return nil, nil, fmt.Errorf("dial: %w", err)
	}

	cl.leaderRpcConn = &refConn{conn: conn, addr: leaderRaftAddr}

	acquired, release := cl.acquireLocked()
	return acquired, release, nil
}

// acquireLocked hands out the current conn plus a release func; release closes
// the conn once it is both retired and idle. Callers must hold connLock.
func (cl *Client) acquireLocked() (*grpc.ClientConn, func()) {
	rc := cl.leaderRpcConn
	rc.refs++
	// a double release would drive refs negative and leak the conn
	var once sync.Once
	release := func() {
		once.Do(func() {
			cl.connLock.Lock()
			defer cl.connLock.Unlock()
			rc.refs--
			if rc.retired && rc.refs == 0 {
				cl.closeRefConn(rc)
			}
		})
	}
	return rc.conn, release
}

// retireLocked detaches the current leader conn: idle conns close immediately,
// busy ones close when the last in-flight RPC releases its reference.
// Callers must hold connLock.
func (cl *Client) retireLocked() {
	rc := cl.leaderRpcConn
	cl.leaderRpcConn = nil
	rc.retired = true
	if rc.refs == 0 {
		cl.closeRefConn(rc)
	}
}

func (cl *Client) closeRefConn(rc *refConn) {
	if err := rc.conn.Close(); err != nil {
		cl.logger.WithFields(
			logrus.Fields{
				"error":       err,
				"leader_addr": rc.addr,
			},
		).Warn("error closing the leader gRPC connection")
	}
}

// fromRPCError parses the error sent by rpc server
// to identify status and chain sentinal errors accordingly.
// This is helpful on the client side to make decision based on
// type-full errors rather than just string-based error
func fromRPCError(err error) error {
	st, ok := status.FromError(err)
	if ok && (st.Code() == codes.NotFound) {
		return errors.Join(err, schemaUC.ErrNotFound)
	}
	return err
}
