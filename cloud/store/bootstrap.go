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
	"log/slog"
	"math/rand"
	"time"

	cmd "github.com/weaviate/weaviate/cloud/proto/cluster"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type joiner interface {
	Join(ctx context.Context, leaderAddr string, req *cmd.JoinPeerRequest) (*cmd.JoinPeerResponse, error)
	Notify(ctx context.Context, leaderAddr string, req *cmd.NotifyPeerRequest) (*cmd.NotifyPeerResponse, error)
}

// Bootstrapper is used to bootstrap this node by attempting to join it to a RAFT cluster.
type Bootstrapper struct {
	joiner joiner

	localRaftAddr string
	localNodeID   string

	retryPeriod time.Duration
	jitter      time.Duration
}

// NewBootstrapper constructs a new bootsrapper
func NewBootstrapper(joiner joiner, raftID, raftAddr string) *Bootstrapper {
	return &Bootstrapper{
		joiner:        joiner,
		retryPeriod:   time.Second,
		jitter:        time.Second,
		localNodeID:   raftID,
		localRaftAddr: raftAddr,
	}
}

// Do iterates over a list of servers in an attempt to join this node to a cluster.
func (b *Bootstrapper) Do(ctx context.Context, servers []string, lg *slog.Logger) error {
	// TODO handle empty server list
	ticker := time.NewTicker(jitter(b.retryPeriod, b.jitter))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// try to join an existing cluster
			if leader, err := b.join(ctx, servers); err == nil {
				lg.Info("successfully joined cluster", "leader", leader)
				return nil
			}
			// notify other servers about readiness of this node to be joined
			if err := b.notify(ctx, servers); err != nil {
				lg.Error("notify all peers", "servers", servers, "err", err)
			}
		}
	}
}

func (b *Bootstrapper) join(ctx context.Context, servers []string) (leader string, err error) {
	var resp *cmd.JoinPeerResponse
	req := &cmd.JoinPeerRequest{Id: b.localNodeID, Address: b.localRaftAddr, Voter: true}
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

// jitter introduce some jitter to a given duration d + [0, 1) * jit -> [d, d+jit]
func jitter(d time.Duration, jit time.Duration) time.Duration {
	return d + time.Duration(float64(jit)*rand.Float64())
}
