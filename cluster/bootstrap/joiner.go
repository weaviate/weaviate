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
	"strings"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
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
	var errors []string
	req := &cmd.JoinPeerRequest{Id: j.localNodeID, Address: j.localRaftAddr, Voter: j.voter}

	// For each server, try to join.
	// If we have no error then we have a leader
	// If we have an error check for err == NOT_FOUND and leader != "" -> we contacted a non-leader node part of the
	// cluster, let's join the leader.
	// If no server allows us to join a cluster, return an error
	// For each server, try to join with retry logic and backoff.
	// The gRPC client has its own retry policy, but we add additional backoff
	// between different nodes to allow services to start up.
	for name, addr := range remoteNodes {
		if name == j.localNodeID {
			continue
		}

		lg.WithFields(logrus.Fields{
			"remoteNodes": remoteNodes,
			"node":        name,
			"address":     addr,
		}).Info("attempting to join")

		// Try to join this node - gRPC client will handle retries for this specific node
		resp, err = j.peerJoiner.Join(ctx, addr, req)
		if err == nil {
			return addr, nil
		}

		// Log the error but don't immediately give up
		st := status.Convert(err)
		lg.WithFields(logrus.Fields{
			"node":    name,
			"address": addr,
			"error":   err,
			"code":    st.Code(),
		}).Debug("failed to join node")

		// Get the leader from response and if not empty try to join it
		if leader := resp.GetLeader(); st.Code() == codes.ResourceExhausted && leader != "" {
			lg.WithField("leader", leader).Info("attempting to join leader")
			_, err = j.peerJoiner.Join(ctx, leader, req)
			if err == nil {
				return leader, nil
			}
			lg.WithField("leader", leader).WithError(err).Info("attempted to follow to leader and failed")
			errors = append(errors, fmt.Sprintf("leader(%s): %v", leader, err))
		} else {
			errors = append(errors, fmt.Sprintf("%s(%s): %v", name, addr, err))
		}

		// Add a small delay before trying the next node to allow services to start up
		// This gives the gRPC retry policy time to work and prevents overwhelming
		// nodes that might be starting up
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(100 * time.Millisecond):
			// Continue to next node
		}
	}

	// Return a joined error message with all failed attempts
	if len(errors) > 0 {
		return "", fmt.Errorf("could not join a cluster from %v: %s", remoteNodes, strings.Join(errors, "; "))
	}
	return "", fmt.Errorf("could not join a cluster from %v", remoteNodes)
}
