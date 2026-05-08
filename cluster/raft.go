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
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
	"github.com/weaviate/weaviate/cluster/schema"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/cluster"
)

// Raft abstracts away the Raft store, providing clients with an interface that encompasses all query & write operations.
// It ensures that these operations are executed on the current leader, regardless of the specific leader in the cluster.
// If current node is the leader, then changes will be applied on the local node and bypass any networking requests.
type Raft struct {
	nodeSelector cluster.NodeSelector
	store        *Store
	cl           client
	log          *logrus.Logger
}

// client to communicate with remote services
type client interface {
	Apply(ctx context.Context, leaderAddr string, req *cmd.ApplyRequest) (*cmd.ApplyResponse, error)
	Query(ctx context.Context, leaderAddr string, req *cmd.QueryRequest) (*cmd.QueryResponse, error)
	Remove(ctx context.Context, leaderAddress string, req *cmd.RemovePeerRequest) (*cmd.RemovePeerResponse, error)
	Join(ctx context.Context, leaderAddr string, req *cmd.JoinPeerRequest) (*cmd.JoinPeerResponse, error)
	WaitForAppliedIndex(ctx context.Context, peerRaftAddr string, req *cmd.WaitForAppliedIndexRequest) (*cmd.WaitForAppliedIndexResponse, error)
}

func NewRaft(selector cluster.NodeSelector, store *Store, client client) *Raft {
	return &Raft{nodeSelector: selector, store: store, cl: client, log: store.log}
}

// Open opens this store service and marked as such.
// It constructs a new Raft node using the provided configuration.
// If there is any old state, such as snapshots, logs, peers, etc., all of those will be restored
func (s *Raft) Open(ctx context.Context, db schema.Indexer) error {
	s.log.Info("starting raft sub-system ...")
	s.store.SetDB(db)
	return s.store.Open(ctx)
}

func (s *Raft) Close(ctx context.Context) (err error) {
	s.log.Info("shutting down raft sub-system ...")
	return s.store.Close(ctx)
}

func (s *Raft) Ready() bool {
	return s.store.Ready()
}

func (s *Raft) SchemaReader() schema.SchemaReader {
	return s.store.SchemaReader()
}

func (s *Raft) WaitUntilDBRestored(ctx context.Context, period time.Duration, close chan struct{}) error {
	return s.store.WaitToRestoreDB(ctx, period, close)
}

func (s *Raft) WaitForUpdate(ctx context.Context, schemaVersion uint64) error {
	return s.store.WaitForAppliedIndex(ctx, time.Millisecond*50, schemaVersion)
}

// WaitForAppliedIndex is the per-peer primitive for WaitForUpdateAllNodes.
func (s *Raft) WaitForAppliedIndex(ctx context.Context, version uint64) error {
	return s.store.WaitForAppliedIndex(ctx, time.Millisecond*50, version)
}

// WaitForUpdateAllNodes paired with operations whose correctness depends on
// every node's FSM having converged — e.g. before sealing the source change
// log during a MOVE, a stale-FSM peer would otherwise still route writes to
// source and bypass the sealed log.
func (s *Raft) WaitForUpdateAllNodes(ctx context.Context, schemaVersion uint64) error {
	servers, err := s.store.Servers()
	if err != nil {
		return fmt.Errorf("get raft servers: %w", err)
	}

	localAddr := s.store.raftConfig().LocalID
	var (
		mu   sync.Mutex
		errs []error
		wg   sync.WaitGroup
	)
	for _, server := range servers {
		server := server
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			if ctx.Err() != nil {
				return
			}
			var perErr error
			if string(server.ID) == string(localAddr) {
				perErr = s.WaitForAppliedIndex(ctx, schemaVersion)
			} else {
				_, perErr = s.cl.WaitForAppliedIndex(ctx, string(server.Address), &cmd.WaitForAppliedIndexRequest{Version: schemaVersion})
			}
			if perErr != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("peer %s: %w", server.ID, perErr))
				mu.Unlock()
			}
		}, s.log)
	}
	wg.Wait()
	return errors.Join(errs...)
}

func (s *Raft) NodeSelector() cluster.NodeSelector {
	return s.nodeSelector
}

func (s *Raft) ReplicationFsm() *replication.ShardReplicationFSM {
	return s.store.replicationManager.GetReplicationFSM()
}

func (s *Raft) IsLeader() bool {
	return s.store.IsLeader()
}
