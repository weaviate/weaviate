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
	return s.store.WaitForAppliedIndex(ctx, schemaVersion)
}

func (s *Raft) AppliedIndex() uint64 {
	return s.store.lastAppliedIndex.Load()
}

func (s *Raft) WaitForAppliedIndex(ctx context.Context, version uint64) (uint64, error) {
	if err := s.store.WaitForAppliedIndex(ctx, version); err != nil {
		return s.store.lastAppliedIndex.Load(), err
	}
	return s.store.lastAppliedIndex.Load(), nil
}

// WaitForUpdateAllNodes blocks until every RAFT peer (including local) has
// applied version. Local wait skips the self-dial. Including local is
// load-bearing: the caller (replication consumer post-leader-bump) has not
// necessarily waited for its own apply, and a stale local FSM lets parallel
// writes routed here build stale routing plans.
func (s *Raft) WaitForUpdateAllNodes(ctx context.Context, version uint64) error {
	servers, err := s.store.Servers()
	if err != nil {
		return fmt.Errorf("list servers for wait-all-nodes: %w", err)
	}

	localID := s.store.cfg.NodeID
	type result struct {
		peer string
		err  error
	}
	resultsCh := make(chan result, len(servers))

	var wg sync.WaitGroup
	for _, srv := range servers {
		srvID := string(srv.ID)
		peerAddr := string(srv.Address)
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			var callErr error
			if srvID == localID {
				_, callErr = s.WaitForAppliedIndex(ctx, version)
			} else {
				_, callErr = s.cl.WaitForAppliedIndex(ctx, peerAddr, &cmd.WaitForAppliedIndexRequest{Version: version})
			}
			resultsCh <- result{peer: srvID, err: callErr}
		}, s.log)
	}
	wg.Wait()
	close(resultsCh)

	var firstErr error
	for r := range resultsCh {
		if r.err != nil && firstErr == nil {
			firstErr = fmt.Errorf("peer %q wait for index %d: %w", r.peer, version, r.err)
		}
	}
	return firstErr
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
