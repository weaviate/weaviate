//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package cluster

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"

	api "github.com/weaviate/weaviate/cluster/proto/api"
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

	if !voter {
		return st.assertFuture(st.raft.AddNonvoter(rID, rAddr, 0, 0))
	}
	return st.assertFuture(st.raft.AddVoter(rID, rAddr, 0, 0))
}

// Remove removes this peer from the cluster
func (st *Store) Remove(id string) error {
	if !st.open.Load() {
		return types.ErrNotOpen
	}
	if st.raft.State() != raft.Leader {
		return types.ErrNotLeader
	}
	// A namespace can only place shards on its home_node, so losing that node
	// leaves its shards with no eligible replacement. Checked here, not in
	// Raft.Remove: a forwarded request reaches the configuration change only here.
	if pinned := st.namespacesWithHomeNode(id); len(pinned) > 0 {
		st.log.WithFields(logrus.Fields{
			"id":         id,
			"namespaces": pinned,
		}).Warn("refusing node removal: node is a namespace home_node")
		return fmt.Errorf("cannot remove node %q: it is the home_node of namespace(s) %s",
			id, strings.Join(pinned, ", "))
	}
	return st.assertFuture(st.raft.RemoveServer(raft.ServerID(id), 0, 0))
}

// namespacesWithHomeNode returns the names of the namespaces whose home_node is
// node, sorted. Namespaces being deleted are left out: their shards are on their
// way out, so blocking on one would outlive anything worth protecting.
func (st *Store) namespacesWithHomeNode(node string) []string {
	var pinned []string
	for _, ns := range st.namespaceManager.List() {
		if ns.State == api.NamespaceStateDeleting {
			continue
		}
		if ns.Primary() == node {
			pinned = append(pinned, ns.Name)
		}
	}
	slices.Sort(pinned)
	return pinned
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

	// Concurrent NotifyPeer RPCs land here during bootstrap; the whole
	// candidates read-modify-drain below must be atomic.
	st.candidatesMu.Lock()
	defer st.candidatesMu.Unlock()

	// Re-evaluate under the lock: a competing notify may have bootstrapped while we waited.
	if st.bootstrapped.Load() || st.Leader() != "" {
		return nil
	}

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

func (st *Store) candidatesLen() int {
	st.candidatesMu.Lock()
	defer st.candidatesMu.Unlock()
	return len(st.candidates)
}

// candidatesSnapshot returns a copy safe to hand to loggers and stats.
func (st *Store) candidatesSnapshot() map[string]string {
	st.candidatesMu.Lock()
	defer st.candidatesMu.Unlock()
	out := make(map[string]string, len(st.candidates))
	for id, addr := range st.candidates {
		out[id] = addr
	}
	return out
}
