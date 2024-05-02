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

package store

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/sirupsen/logrus"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type joiner interface {
	Join(_ context.Context, leaderAddr string, _ *cmd.JoinPeerRequest) (*cmd.JoinPeerResponse, error)
	Notify(_ context.Context, leaderAddr string, _ *cmd.NotifyPeerRequest) (*cmd.NotifyPeerResponse, error)
}

// Bootstrapper is used to bootstrap this node by attempting to join it to a RAFT cluster.
type Bootstrapper struct {
	joiner       joiner
	addrResolver addressResolver

	localRaftAddr string
	localNodeID   string

	retryPeriod time.Duration
	jitter      time.Duration
}

// NewBootstrapper constructs a new bootsrapper
func NewBootstrapper(joiner joiner, raftID, raftAddr string, r addressResolver) *Bootstrapper {
	return &Bootstrapper{
		joiner:        joiner,
		addrResolver:  r,
		retryPeriod:   time.Second,
		jitter:        time.Second,
		localNodeID:   raftID,
		localRaftAddr: raftAddr,
	}
}

// Do iterates over a list of servers in an attempt to join this node to a cluster.
func (b *Bootstrapper) Do(ctx context.Context, serverPortMap map[string]int, lg *logrus.Logger, voter bool, close chan struct{}) error {
	ticker := time.NewTicker(jitter(b.retryPeriod, b.jitter))
	servers := make([]string, 0, len(serverPortMap))
	defer ticker.Stop()
	for {
		servers = b.servers(servers, serverPortMap)
		select {
		case <-close:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// try to join an existing cluster
			if leader, err := b.join(ctx, servers, voter); err == nil {
				lg.WithField("leader", leader).Info("successfully joined cluster")
				return nil
			}

			if voter {
				// notify other servers about readiness of this node to be joined
				if err := b.notify(ctx, servers); err != nil {
					lg.WithField("servers", servers).WithError(err).Error("notify all peers")
				}
			}

		}
	}
}

// join attempts to join an existing leader
func (b *Bootstrapper) join(ctx context.Context, servers []string, voter bool) (leader string, err error) {
	var resp *cmd.JoinPeerResponse
	req := &cmd.JoinPeerRequest{Id: b.localNodeID, Address: b.localRaftAddr, Voter: voter}
	for _, addr := range servers {
		resp, err = b.joiner.Join(ctx, addr, req)
		if err == nil {
			return addr, nil
		}
		st := status.Convert(err)
		if leader = resp.GetLeader(); st.Code() == codes.NotFound && leader != "" {
			_, err = b.joiner.Join(ctx, leader, req)
			if err == nil {
				return leader, nil
			}
		}
	}
	return "", fmt.Errorf("could not join a cluster from %v", servers)
}

// notify notifies another server of my presence
func (b *Bootstrapper) notify(ctx context.Context, servers []string) (err error) {
	for _, addr := range servers {
		req := &cmd.NotifyPeerRequest{Id: b.localNodeID, Address: b.localRaftAddr}
		_, err = b.joiner.Notify(ctx, addr, req)
		if err != nil {
			return err
		}
	}
	return
}

// servers retrieves a list of server addresses based on their names and ports.
func (b *Bootstrapper) servers(candidates []string, serverPortMap map[string]int) []string {
	candidates = candidates[:0]
	for name, raftPort := range serverPortMap {
		if addr := b.addrResolver.NodeAddress(name); addr != "" {
			candidates = append(candidates, fmt.Sprintf("%s:%d", addr, raftPort))
		}
	}
	return candidates
}

// jitter introduce some jitter to a given duration d + [0, 1) * jit -> [d, d+jit]
func jitter(d time.Duration, jit time.Duration) time.Duration {
	return d + time.Duration(float64(jit)*rand.Float64())
}
