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
	"time"

	"github.com/sirupsen/logrus"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/router"
	"github.com/weaviate/weaviate/cluster/schema"
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

	// non-voter can be safely removed, as they don't partake in RAFT elections
	if !s.store.IsVoter() {
		s.log.Info("removing this node from cluster prior to shutdown ...")
		if err := s.Remove(ctx, s.store.ID()); err != nil {
			s.log.WithError(err).Error("remove this node from cluster")
		} else {
			s.log.Info("successfully removed this node from the cluster.")
		}
	}
	return s.store.Close(ctx)
}

func (s *Raft) Ready() bool {
	return s.store.Ready()
}

func (s *Raft) SchemaReader() schema.SchemaReader {
	return s.store.SchemaReader()
}

func (s *Raft) NewRouter(logger *logrus.Logger) *router.Router {
	return router.New(logger, s.store.cfg.NodeSelector, s.store.SchemaReader(), s.store.replicationManager.GetReplicationFSM())
}

func (s *Raft) WaitUntilDBRestored(ctx context.Context, period time.Duration, close chan struct{}) error {
	return s.store.WaitToRestoreDB(ctx, period, close)
}

func (s *Raft) WaitForUpdate(ctx context.Context, schemaVersion uint64) error {
	return s.store.WaitForAppliedIndex(ctx, time.Millisecond*50, schemaVersion)
}
