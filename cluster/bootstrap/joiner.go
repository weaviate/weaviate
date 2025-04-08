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
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	entSentry "github.com/weaviate/weaviate/entities/sentry"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	lg.WithField("remoteNodes", remoteNodes).Info("attempting to join")

	// For each server, try to join.
	// If we have no error then we have a leader
	// If we have an error check for err == NOT_FOUND and leader != "" -> we contacted a non-leader node part of the
	// cluster, let's join the leader.
	// If no server allows us to join a cluster, return an error
	for _, addr := range remoteNodes {
		resp, err = j.peerJoiner.Join(ctx, addr, req)
		if err == nil {
			return addr, nil
		}
		st := status.Convert(err)
		lg.WithField("remoteNode", addr).WithField("status", st.Code()).Info("attempted to join and failed")
		// Get the leader from response and if not empty try to join it
		if leader := resp.GetLeader(); st.Code() == codes.NotFound && leader != "" {
			_, err = j.peerJoiner.Join(ctx, leader, req)
			if err == nil {
				return leader, nil
			}
			lg.WithField("leader", leader).WithError(err).Info("attempted to follow to leader and failed")
		}
	}
	return "", fmt.Errorf("could not join a cluster from %v", remoteNodes)
}
