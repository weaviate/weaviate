package store

import (
	"context"
	"log"
	"math/rand"
	"time"

	cmd "github.com/weaviate/weaviate/cloud/proto/cluster"
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
func (b *Bootstrapper) Do(ctx context.Context, servers []string) error {
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
				log.Printf("successfully joined cluster with leader %v\n", leader)
				return nil
			}
			// notify other servers about readiness of this node to be joined
			if err := b.notify(ctx, servers); err != nil {
				log.Printf("notify all peers: %v: %v", servers, err)
			}
		}
	}
}

func (b *Bootstrapper) join(ctx context.Context, servers []string) (leader string, err error) {
	for _, addr := range servers {
		req := &cmd.JoinPeerRequest{Id: b.localNodeID, Address: b.localRaftAddr, Voter: true}
		_, err = b.joiner.Join(ctx, addr, req)
		if err == nil {
			return addr, nil
		}
	}
	return
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
