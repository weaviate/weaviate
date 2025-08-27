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

// StorageCandidates return the nodes in the raft configuration or memberlist storage nodes
// based on the current configuration of the cluster if it does have  MetadataVoterOnly nodes.
func (s *Raft) StorageCandidates() []string {
	if s.store.raft == nil {
		// get candidates from memberlist
		return s.nodeSelector.StorageCandidates()
	}

	var (
		existedRaftCandidates []string
		raftStorageCandidates []string
		memStorageCandidates  = s.nodeSelector.StorageCandidates()
		nonStorageCandidates  = s.nodeSelector.NonStorageNodes()
	)

	for _, server := range s.store.raft.GetConfiguration().Configuration().Servers {
		existedRaftCandidates = append(existedRaftCandidates, string(server.ID))
	}

	// filter non storage candidates
	for _, c := range existedRaftCandidates {
		if slices.Contains(nonStorageCandidates, c) {
			continue
		}
		raftStorageCandidates = append(raftStorageCandidates, c)
	}

	if len(memStorageCandidates) > len(raftStorageCandidates) {
		// if memberlist has more nodes then use it instead
		// this case could happen if we have MetaVoterOnly Nodes
		// in the RAFT config
		return memStorageCandidates
	}

	return s.nodeSelector.SortCandidates(raftStorageCandidates)
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
