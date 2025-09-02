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
	"fmt"
	"time"

	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
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

// Close() is called when the node is shutting down.
//
// Shutdown Sequence
func (s *Raft) Close(ctx context.Context) (err error) {
	s.log.Info("shutting down raft sub-system ...")

	if s.store == nil {
		s.log.Warn("store is nil, skipping shutdown sequence")
		return nil
	}

	// Step 1: Mark store as closed (Kubernetes readiness probe)
	s.store.open.Store(false)
	s.log.Info("marked store as closed - node no longer ready")

	// Step 2: Transfer leadership if current leader
	if s.store.IsLeader() {
		s.store.log.Info("transferring leadership to another server")
		if err := s.store.raft.LeadershipTransfer().Error(); err != nil {
			s.store.log.WithError(err).Error("transferring leadership")
		} else {
			s.log.Info("verifying new leader election...")
			if err := s.waitForNewLeader(ctx); err != nil {
				s.log.WithError(err).Warn("failed to verify new leader, proceeding anyway")
			} else {
				s.log.Info("confirmed: new leader has been elected")
			}
		}
	}

	// // Step 5: Remove from Raft configuration
	// s.log.Info("requesting removal from leader via RemovePeer RPC...")
	// leader := s.store.Leader()
	// if leader != "" && s.cl != nil {
	// 	req := &cmd.RemovePeerRequest{Id: s.store.ID()}
	// 	_, err := s.cl.Remove(ctx, leader, req)
	// 	if err != nil {
	// 		s.log.WithError(err).Warn("failed to request removal from leader")
	// 	} else {
	// 		s.log.Info("successfully requested removal from leader")
	// 		if err := s.waitRemovedFromConfig(ctx); err != nil {
	// 			s.log.WithError(err).Warn("timeout waiting for config removal; proceeding with shutdown")
	// 		} else {
	// 			s.log.Info("confirmed removal from Raft configuration")
	// 		}
	// 	}
	// } else {
	// 	s.log.Warn("no leader available to request removal from")
	// }

	// Step 7: Shutdown Raft operations
	s.log.Info("stopping raft operations ...")
	if s.store.raft != nil {
		if err := s.store.raft.Shutdown().Error(); err != nil {
			s.log.WithError(err).Warn("shutdown raft")
		}
	}

	// Step X: Set graceful leave state in metadata (still counts for RF)
	// This prevents replication factor errors while allowing clean shutdown
	s.log.Info("setting graceful leave state in metadata (still counts for replication factor)...")
	if err := s.nodeSelector.Leave(); err != nil {
		s.log.WithError(err).Warn("failed to set graceful leave state")
	} else {
		s.log.Info("successfully set graceful leave state in metadata")
	}

	s.log.Info("shutting down memberlist completely...")
	if err := s.nodeSelector.Shutdown(30 * time.Second); err != nil {
		s.store.log.WithError(err).Warn("shutdown memberlist")
	}

	// Step 8: Close Raft transport
	s.log.Info("closing raft transport...")
	if s.store.raftTransport != nil {
		if err := s.store.raftTransport.Close(); err != nil {
			s.log.WithError(err).Warn("close raft transport")
		}
	}

	// Step 9: Close underlying store
	return s.store.Close(ctx)
}

// waitForNewLeader waits for a new leader to be elected after leadership transfer.
// This ensures that leadership transfer has completed successfully before
// proceeding with node removal from the cluster.
func (s *Raft) waitForNewLeader(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for new leader: %w", ctx.Err())
		case <-ticker.C:
			_, newLeader := s.store.LeaderWithID()
			if newLeader != "" && newLeader != raft.ServerID(s.store.ID()) {
				s.log.WithField("new_leader", newLeader).Info("new leader confirmed")
				return nil
			}
		}
	}
}

// waitRemovedFromConfig polls the Raft configuration until the given node ID is absent
// or the context times out. This ensures that the node removal has been committed
// cluster-wide before proceeding with memberlist shutdown.
// func (s *Raft) waitRemovedFromConfig(ctx context.Context) error {
// 	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
// 	defer cancel()

// 	ticker := time.NewTicker(100 * time.Millisecond)
// 	defer ticker.Stop()

// 	for {
// 		select {
// 		case <-ctx.Done():
// 			return fmt.Errorf("waiting for removal of %s from configuration: %w", s.store.ID(), ctx.Err())
// 		case <-ticker.C:
// 			fut := s.store.raft.GetConfiguration()
// 			if err := fut.Error(); err != nil {
// 				continue
// 			}
// 			removed := true
// 			for _, sv := range fut.Configuration().Servers {
// 				if string(sv.ID) == s.store.ID() {
// 					removed = false
// 					break
// 				}
// 			}
// 			if removed {
// 				return nil
// 			}
// 		}
// 	}
// }

func (s *Raft) Ready() bool {
	return s.store.Ready()
}

func (s *Raft) SchemaReader() schema.SchemaReader {
	return s.store.SchemaReader()
}

func (s *Raft) WaitUntilDBRestored(ctx context.Context, period time.Duration, close chan struct{}) error {
	return s.store.WaitToRestoreDB(ctx, period, close)
}
