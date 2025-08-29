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
	"errors"
	"fmt"

	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/types"
)

// Join adds the given peer to the cluster.
// This operation must be executed on the leader, otherwise, it will fail with ErrNotLeader.
// If the cluster has not been opened yet, it will return ErrNotOpen.
func (st *Store) Join(id, addr string, voter bool) error {
	if !st.open.Load() {
		return types.ErrNotOpen
	}
	if st.raft.State() != raft.Leader {
		return types.ErrNotLeader
	}

	rID, rAddr := raft.ServerID(id), raft.ServerAddress(addr)

	// Get current cluster configuration
	configFuture := st.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		st.log.WithFields(logrus.Fields{
			"action":  "join_get_config_failed",
			"node_id": id,
			"error":   err,
		}).Error("failed to get raft configuration")
		// return err
	}

	// Check if this is a self-join scenario (node trying to join itself)
	// Only allow self-join if cluster has enough servers to avoid stepping down
	if id == st.cfg.NodeID {
		if l := len(configFuture.Configuration().Servers); l < st.cfg.BootstrapExpect {
			st.log.WithFields(logrus.Fields{
				"action":    "join_self_skip",
				"node_id":   id,
				"servers":   l,
				"raft_addr": addr,
			}).Debug("skipping self join check since cluster is too small")
			return nil
		}
	}

	// Check for existing servers with duplicate address or ID
	for _, server := range configFuture.Configuration().Servers {
		// If the address or ID matches an existing server, see if we need to remove the old one first
		if server.Address == rAddr || server.ID == rID {
			// Exit with no-op if this is being called on an existing server and both the ID and address match
			if server.Address == rAddr && server.ID == rID {
				st.log.WithFields(logrus.Fields{
					"action":    "join_noop",
					"node_id":   id,
					"raft_addr": addr,
				}).Debug("server already exists with same ID and address, no-op")
				return nil
			}

			// Check if removing this server would leave the cluster without voters
			// Count current voters excluding the server we're about to remove
			voterCount := 0
			for _, s := range configFuture.Configuration().Servers {
				if s.ID != server.ID && s.Suffrage == raft.Voter {
					voterCount++
				}
			}

			// If removing this server would leave no voters, we need to be careful
			if voterCount == 0 {
				// If we're adding a voter, allow the removal (for bootstrapping/recovery)
				if voter {
					st.log.WithFields(logrus.Fields{
						"action":    "join_removing_for_voter_bootstrap",
						"node_id":   id,
						"raft_addr": addr,
						"server_id": server.ID,
					}).Info("removing server to allow voter bootstrap")
				} else {
					// If we're not adding a voter, only allow removal if the server being removed is also not a voter
					// This allows removing non-voters even if it leaves only voters
					if server.Suffrage != raft.Voter {
						st.log.WithFields(logrus.Fields{
							"action":    "join_removing_nonvoter",
							"node_id":   id,
							"raft_addr": addr,
							"server_id": server.ID,
						}).Info("removing non-voter server, will leave only voters")
					} else {
						st.log.WithFields(logrus.Fields{
							"action":      "join_cannot_remove_last_voter",
							"node_id":     id,
							"raft_addr":   addr,
							"server_id":   server.ID,
							"voter_count": voterCount,
						}).Warn("cannot remove last voter when adding non-voter, skipping removal")
						continue
					}
				}
			}

			// Remove the conflicting server
			future := st.raft.RemoveServer(server.ID, 0, 0)
			if server.Address == rAddr {
				if err := future.Error(); err != nil {
					st.log.WithFields(logrus.Fields{
						"action":  "join_remove_duplicate_addr_failed",
						"address": server.Address,
						"error":   err,
					}).Error("failed to remove server with duplicate address")
					return fmt.Errorf("error removing server with duplicate address %q: %w", server.Address, err)
				}
				st.log.WithFields(logrus.Fields{
					"action":  "join_remove_duplicate_addr_success",
					"address": server.Address,
					"node_id": id,
				}).Info("removed server with duplicate address")
			} else {
				if err := future.Error(); err != nil {
					st.log.WithFields(logrus.Fields{
						"action": "join_remove_duplicate_id_failed",
						"id":     server.ID,
						"error":  err,
					}).Error("failed to remove server with duplicate ID")
					return fmt.Errorf("error removing server with duplicate ID %q: %w", server.ID, err)
				}
				st.log.WithFields(logrus.Fields{
					"action":  "join_remove_duplicate_id_success",
					"id":      server.ID,
					"node_id": id,
				}).Info("removed server with duplicate ID")
			}
		}
	}

	// Attempt to add the peer
	st.log.WithFields(logrus.Fields{
		"action":    "join_adding_peer",
		"node_id":   id,
		"raft_addr": addr,
		"voter":     voter,
	}).Info("adding new peer to cluster")

	if !voter {
		addFuture := st.raft.AddNonvoter(rID, rAddr, 0, 0)
		if err := addFuture.Error(); err != nil {
			st.log.WithFields(logrus.Fields{
				"action":  "join_add_nonvoter_failed",
				"node_id": id,
				"error":   err,
			}).Error("failed to add nonvoter peer")
			return err
		}
	} else {
		addFuture := st.raft.AddVoter(rID, rAddr, 0, 0)
		if err := addFuture.Error(); err != nil {
			st.log.WithFields(logrus.Fields{
				"action":  "join_add_voter_failed",
				"node_id": id,
				"error":   err,
			}).Error("failed to add voter peer")
			return err
		}
	}

	st.log.WithFields(logrus.Fields{
		"action":    "join_success",
		"node_id":   id,
		"raft_addr": addr,
		"voter":     voter,
	}).Info("successfully added peer to cluster")

	return nil
}

// Remove removes this peer from the cluster
func (st *Store) Remove(id string) error {
	if !st.open.Load() {
		return types.ErrNotOpen
	}
	if st.raft.State() != raft.Leader {
		return types.ErrNotLeader
	}
	return st.assertFuture(st.raft.RemoveServer(raft.ServerID(id), 0, 0))
}

// Notify signals this Store that a node is ready for bootstrapping at the specified address.
// Bootstrapping will be initiated once the number of known nodes reaches the expected level,
// which includes this node.
func (st *Store) Notify(id, addr string) (err error) {
	if !st.open.Load() {
		return types.ErrNotOpen
	}
	// peer is not voter or already bootstrapped or belong to an existing cluster
	if !st.cfg.Voter || st.cfg.BootstrapExpect == 0 || st.bootstrapped.Load() || st.Leader() != "" {
		return nil
	}

	st.bootstrapMutex.Lock()
	defer st.bootstrapMutex.Unlock()

	st.candidates[id] = addr
	if len(st.candidates) < st.cfg.BootstrapExpect {
		st.log.WithFields(logrus.Fields{
			"action": "bootstrap",
			"expect": st.cfg.BootstrapExpect,
			"got":    st.candidates,
		}).Debug("number of candidates lower than bootstrap expect param, stopping notify")
		return nil
	}
	candidates := make([]raft.Server, 0, len(st.candidates))
	for id, addr := range st.candidates {
		candidates = append(candidates, raft.Server{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(id),
			Address:  raft.ServerAddress(addr),
		})
		delete(st.candidates, id)
	}

	st.log.WithFields(logrus.Fields{
		"action":     "bootstrap",
		"candidates": candidates,
	}).Info("starting cluster bootstrapping")

	fut := st.raft.BootstrapCluster(raft.Configuration{Servers: candidates})
	if err := fut.Error(); err != nil {
		if !errors.Is(err, raft.ErrCantBootstrap) {
			st.log.WithField("action", "bootstrap").WithError(err).Error("could not bootstrapping cluster")
			return err
		}
		st.log.WithFields(logrus.Fields{
			"action": "bootstrap",
			"warn":   err,
		}).Warn("bootstrapping cluster")
	}
	st.bootstrapped.Store(true)
	return nil
}
