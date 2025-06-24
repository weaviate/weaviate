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

package bootstrap

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/sirupsen/logrus"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/resolver"
	entSentry "github.com/weaviate/weaviate/entities/sentry"
)

// PeerJoiner is the interface we expect to be able to talk to the other peers to either Join or Notify them
type PeerJoiner interface {
	Join(_ context.Context, leaderAddr string, _ *cmd.JoinPeerRequest) (*cmd.JoinPeerResponse, error)
	Notify(_ context.Context, leaderAddr string, _ *cmd.NotifyPeerRequest) (*cmd.NotifyPeerResponse, error)
}

// Bootstrapper is used to bootstrap this node by attempting to join it to a RAFT cluster.
type Bootstrapper struct {
	peerJoiner   PeerJoiner
	addrResolver resolver.ClusterStateReader
	isStoreReady func() bool

	localRaftAddr string
	localNodeID   string
	voter         bool

	retryPeriod time.Duration
	jitter      time.Duration
}

// NewBootstrapper constructs a new bootsrapper
func NewBootstrapper(peerJoiner PeerJoiner, raftID string, raftAddr string, voter bool, r resolver.ClusterStateReader, isStoreReady func() bool) *Bootstrapper {
	return &Bootstrapper{
		peerJoiner:    peerJoiner,
		addrResolver:  r,
		retryPeriod:   time.Second,
		jitter:        time.Second,
		localNodeID:   raftID,
		localRaftAddr: raftAddr,
		isStoreReady:  isStoreReady,
		voter:         voter,
	}
}

// Do iterates over a list of servers in an attempt to join this node to a cluster.
func (b *Bootstrapper) Do(ctx context.Context, serverPortMap map[string]int, lg *logrus.Logger, stop chan struct{}) error {
	if entSentry.Enabled() {
		transaction := sentry.StartTransaction(ctx, "raft.bootstrap",
			sentry.WithOpName("init"),
			sentry.WithDescription("Attempt to bootstrap a raft cluster"),
		)
		ctx = transaction.Context()
		defer transaction.Finish()
	}
	ticker := time.NewTicker(jitter(b.retryPeriod, b.jitter))
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if b.isStoreReady() {
				lg.WithField("action", "bootstrap").Info("node reporting ready, exiting bootstrap process")
				return nil
			}

			remoteNodes := ResolveRemoteNodes(b.addrResolver, serverPortMap)
			// We were not able to resolve any nodes to an address
			if len(remoteNodes) == 0 {
				lg.WithField("action", "bootstrap").WithField("join_list", serverPortMap).Warn("unable to resolve any node address to join")
				continue
			}

			// Always try to join an existing cluster first
			joiner := NewJoiner(b.peerJoiner, b.localNodeID, b.localRaftAddr, b.voter)
			if leader, err := joiner.Do(ctx, lg, remoteNodes); err != nil {
				lg.WithFields(logrus.Fields{
					"action":  "bootstrap",
					"servers": remoteNodes,
					"voter":   b.voter,
				}).WithError(err).Warning("failed to join cluster")
			} else {
				lg.WithFields(logrus.Fields{
					"action": "bootstrap",
					"leader": leader,
				}).Info("successfully joined cluster")
				return nil
			}

			// We are a voter, we resolve other peers but we're unable to join them. We're in the situation where we are
			// bootstrapping a new cluster and now we want to notify the other nodes.
			// Each node on notify will build a list of notified node. Once bootstrap expect is reached the nodes will
			// bootstrap together.
			if b.voter {
				// notify other servers about readiness of this node to be joined
				if err := b.notify(ctx, remoteNodes); err != nil {
					lg.WithFields(logrus.Fields{
						"action":  "bootstrap",
						"servers": remoteNodes,
					}).WithError(err).Error("failed to notify peers")
					continue
				}
				lg.WithFields(logrus.Fields{
					"action":  "bootstrap",
					"servers": remoteNodes,
				}).Info("notified peers this node is ready to join as voter")
			}
		}
	}
}

// notify attempts to notify all nodes in remoteNodes that this server is ready to bootstrap
func (b *Bootstrapper) notify(ctx context.Context, remoteNodes map[string]string) (err error) {
	if entSentry.Enabled() {
		span := sentry.StartSpan(ctx, "raft.bootstrap.notify",
			sentry.WithOpName("notify"),
			sentry.WithDescription("Attempt to notify existing node(s) to join a cluster"),
		)
		ctx = span.Context()
		span.SetData("servers", remoteNodes)
		defer span.Finish()
	}
	for _, addr := range remoteNodes {
		req := &cmd.NotifyPeerRequest{Id: b.localNodeID, Address: b.localRaftAddr}
		_, err = b.peerJoiner.Notify(ctx, addr, req)
		if err != nil {
			return err
		}
	}
	return
}

// ResolveRemoteNodes returns a list of remoteNodes addresses resolved using addrResolver. The nodes id used are
// taken from serverPortMap keys and ports from the values
func ResolveRemoteNodes(addrResolver resolver.ClusterStateReader, serverPortMap map[string]int) map[string]string {
	candidates := make(map[string]string, len(serverPortMap))
	for name, raftPort := range serverPortMap {
		if addr := addrResolver.NodeAddress(name); addr != "" {
			candidates[name] = fmt.Sprintf("%s:%d", addr, raftPort)
		}
	}
	return candidates
}

// jitter introduce some jitter to a given duration d + [0, 1) * jit -> [d, d+jit]
func jitter(d time.Duration, jit time.Duration) time.Duration {
	return d + time.Duration(float64(jit)*rand.Float64())
}
