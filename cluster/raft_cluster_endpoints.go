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

package cluster

import (
	"context"
	"slices"

	"github.com/sirupsen/logrus"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

// LeaderWithID is used to return the current leader address and ID of the cluster.
// It may return empty strings if there is no current leader or the leader is unknown.
func (s *Raft) LeaderWithID() (string, string) {
	addr, id := s.store.LeaderWithID()
	return string(addr), string(id)
}

// Candidates return the nodes in the raft configuration or memberlist
// based on the current configuration of the cluster if it does have  MetadataVoterOnly nodes.
func (s *Raft) Candidates() []string {
	var candidates []string
	memberlistCandidates := s.nodeSelector.StorageCandidates()
	if s.store.raft == nil {
		// get candidates from memberlist
		return memberlistCandidates
	}

	servers := s.store.raft.GetConfiguration().Configuration().Servers
	for _, server := range servers {
		candidates = append(candidates, string(server.ID))
	}

	storageCandidates := []string{}
	nonStorage := s.nodeSelector.NonStorageNodes()

	for _, c := range candidates {
		if slices.Contains(nonStorage, c) {
			continue
		}
		storageCandidates = append(storageCandidates, c)
	}

	if len(memberlistCandidates) > len(storageCandidates) {
		// if memberlist has more nodes then use it instead
		// this case could happen if we have MetaVoterOnly Nodes
		// in the RAFT config
		return memberlistCandidates
	}

	return s.nodeSelector.SortCandidates(storageCandidates)
}

func (s *Raft) Join(ctx context.Context, id, addr string, voter bool) error {
	s.log.WithFields(logrus.Fields{
		"id":      id,
		"address": addr,
		"voter":   voter,
	}).Debug("membership.join")
	if s.store.IsLeader() {
		return s.store.Join(id, addr, voter)
	}
	leader := s.store.Leader()
	if leader == "" {
		return s.leaderErr()
	}
	req := &cmd.JoinPeerRequest{Id: id, Address: addr, Voter: voter}
	_, err := s.cl.Join(ctx, leader, req)
	return err
}

func (s *Raft) Remove(ctx context.Context, id string) error {
	s.log.WithField("id", id).Debug("membership.remove")
	if s.store.IsLeader() {
		return s.store.Remove(id)
	}
	leader := s.store.Leader()
	if leader == "" {
		return s.leaderErr()
	}
	req := &cmd.RemovePeerRequest{Id: id}
	_, err := s.cl.Remove(ctx, leader, req)
	return err
}

func (s *Raft) Stats() map[string]any {
	s.log.Debug("membership.stats")
	return s.store.Stats()
}
