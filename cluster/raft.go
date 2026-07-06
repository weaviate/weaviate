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

	"github.com/weaviate/weaviate/cluster/distributedtask"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
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

	// homeNodeIterator persists across AddNamespace calls so home-node
	// selection rotates through the cluster. Built lazily and rebuilt
	// whenever the candidate set changes (node join/leave) so newly added
	// nodes become eligible and removed ones drop out.
	homeNodeIteratorMu sync.Mutex
	homeNodeIterator   *cluster.NodeIterator
	homeNodeCandidates []string
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
	return s.store.Close(ctx)
}

// SetDistributedTaskSchedulerNotifier installs the wake-up notifier on
// the underlying distributed task FSM Manager. Called once at startup
// from MakeAppState, after both Raft and the Scheduler exist. See
// [distributedtask.SchedulerNotifier] for the contract.
func (s *Raft) SetDistributedTaskSchedulerNotifier(notifier distributedtask.SchedulerNotifier) {
	s.store.SetDistributedTaskSchedulerNotifier(notifier)
}

// SetDistributedTaskConflictDetectors installs the per-namespace
// conflict-detection hooks on the underlying distributed task FSM
// Manager. Called once at startup from MakeAppState, after the
// providers are registered. See [distributedtask.ConflictDetector] for
// the FSM-determinism contract.
func (s *Raft) SetDistributedTaskConflictDetectors(detectors map[string]distributedtask.ConflictDetector) {
	s.store.SetDistributedTaskConflictDetectors(detectors)
}

// SetDistributedTaskSchemaMutationDetectors installs the per-namespace
// schema-mutation detectors consulted from the schema FSM's
// UpdateProperty apply path. Called once at startup from MakeAppState,
// after the providers are registered. See
// [distributedtask.SchemaMutationDetector] for the contract and
// motivating failure mode.
func (s *Raft) SetDistributedTaskSchemaMutationDetectors(detectors map[string]distributedtask.SchemaMutationDetector) {
	s.store.SetDistributedTaskSchemaMutationDetectors(detectors)
}

// RegisterDistributedTaskCollectionExtractor opts a task namespace into
// the DELETE_CLASS cascade. See [distributedtask.CollectionExtractor]
// and weaviate/0-weaviate-issues#231.
func (s *Raft) RegisterDistributedTaskCollectionExtractor(namespace string, extractor distributedtask.CollectionExtractor) {
	s.store.RegisterDistributedTaskCollectionExtractor(namespace, extractor)
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

func (s *Raft) ReplicationAllPeersAtLeast(opID uint64, target cmd.ShardReplicationState) (bool, error) {
	if s.store.raft == nil {
		return false, nil
	}
	cfg := s.store.raft.GetConfiguration()
	if err := cfg.Error(); err != nil {
		return false, fmt.Errorf("get raft configuration: %w", err)
	}
	servers := cfg.Configuration().Servers
	if len(servers) == 0 {
		return false, nil
	}
	peers := make([]string, 0, len(servers))
	for _, server := range servers {
		peers = append(peers, string(server.ID))
	}
	return s.store.replicationManager.GetReplicationFSM().AllPeersAtLeast(opID, target, peers), nil
}

func (s *Raft) NodeSelector() cluster.NodeSelector {
	return s.nodeSelector
}

func (s *Raft) ReplicationFsm() *replication.ShardReplicationFSM {
	return s.store.replicationManager.GetReplicationFSM()
}

func (s *Raft) SetInflightDrainer(fn func(ctx context.Context, class, shard string) error) {
	s.store.replicationManager.SetInflightDrainer(fn)
}

func (s *Raft) IsLeader() bool {
	return s.store.IsLeader()
}

// ForceSnapshot triggers an immediate RAFT snapshot. See Store.ForceSnapshot.
func (s *Raft) ForceSnapshot() error {
	return s.store.ForceSnapshot()
}
