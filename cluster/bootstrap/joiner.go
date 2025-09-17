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

	"github.com/getsentry/sentry-go"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/status"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/rpc"
	entSentry "github.com/weaviate/weaviate/entities/sentry"
)

type Joiner struct {
	localRaftAddr string
	localNodeID   string
	voter         bool
	peerJoiner    PeerJoiner
}

// NewJoiner returns a *Joiner configured with localNodeID, localRaftAddr and voter.
func NewJoiner(peerJoiner PeerJoiner, localNodeID string, localRaftAddr string, voter bool) *Joiner {
	return &Joiner{
		peerJoiner:    peerJoiner,
		localNodeID:   localNodeID,
		localRaftAddr: localRaftAddr,
		voter:         voter,
	}
}

// Do will attempt to send to any nodes in remoteNodes a JoinPeerRequest for j.localNodeID with the address j.localRaftAddr.
// Will join as voter if j.voter is true, non voter otherwise.
// Returns the leader address if a cluster was joined or an error otherwise.
func (j *Joiner) Do(ctx context.Context, lg *logrus.Logger, remoteNodes map[string]string) (string, error) {
	if entSentry.Enabled() {
		span := sentry.StartSpan(ctx, "raft.bootstrap.join",
			sentry.WithOpName("join"),
			sentry.WithDescription("Attempt to join an existing cluster"),
		)
		ctx = span.Context()
		span.SetData("servers", remoteNodes)
		defer span.Finish()
	}

	var resp *cmd.JoinPeerResponse
	var err error
	req := &cmd.JoinPeerRequest{Id: j.localNodeID, Address: j.localRaftAddr, Voter: j.voter}

	// For each server, try to join.
	// If we have no error then we have a leader
	// If we have an error check for err == NOT_FOUND and leader != "" -> we contacted a non-leader node part of the
	// cluster, let's join the leader.
	// If no server allows us to join a cluster, return an error
	for name, addr := range remoteNodes {
		lg.WithFields(logrus.Fields{
			"action":         "join",
			"node":           name,
			"local_address":  j.localRaftAddr,
			"remote_address": addr,
		}).Info("attempting to join")
		resp, err = j.peerJoiner.Join(ctx, addr, req)
		if err == nil {
			return addr, nil
		}

		rpcStatusCode := status.Convert(err).Code()
		leaderAddr := resp.GetLeader()
		// handle cases joining a cluster with no leader
		// or election is in progress
		if leaderAddr == "" {
			continue
		}

		// avoid self multiple join attempts
		if rpcStatusCode == rpc.NotLeaderRPCCode && leaderAddr == j.localRaftAddr {
			return leaderAddr, nil
		}

		lg.WithFields(logrus.Fields{
			"action":                  "join",
			"trying_remote_node_addr": addr,
			"leader":                  leaderAddr,
			"rpc_status_code":         rpcStatusCode,
		}).Info("attempted to join and failed")

		// Get the leader from response and if not empty try to join it
		if rpcStatusCode == rpc.NotLeaderRPCCode {
			// join the actual leader
			_, err = j.peerJoiner.Join(ctx, leaderAddr, req)
			if err == nil {
				return leaderAddr, nil
			}
			lg.WithField("leader", leaderAddr).WithError(err).Info("attempted to follow to leader and failed")
		}
	}
	return "", fmt.Errorf("could not join a cluster from %v", remoteNodes)
}
