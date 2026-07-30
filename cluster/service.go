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
	"math/rand"
	"net"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/hashicorp/raft"
	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/bootstrap"
	"github.com/weaviate/weaviate/cluster/fsm"
	"github.com/weaviate/weaviate/cluster/replication"
	"github.com/weaviate/weaviate/cluster/replication/metrics"
	"github.com/weaviate/weaviate/cluster/resolver"
	"github.com/weaviate/weaviate/cluster/rpc"
	"github.com/weaviate/weaviate/cluster/schema"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

const (
	// TODO: consider exposing these as settings
	shardReplicationEngineBufferSize = 16
	fsmOpProducerPollingInterval     = 5 * time.Second
	replicationEngineShutdownTimeout = 20 * time.Second
	replicationOperationTimeout      = 24 * time.Hour
	catchUpInterval                  = 5 * time.Second

	// Compiled-in fallbacks for the replica-movement cleanup knobs, used only when
	// the config seam left one unwired.
	defaultReplicaMovementCleanupMaxAge   = 168 * time.Hour
	defaultReplicaMovementCleanupInterval = time.Hour
)

// boolGetter / durationGetter turn a *runtime.DynamicValue into the closure the
// sweeper polls. A nil pointer means the rConfig line was missed: Get() on a nil
// receiver returns the zero value, and taking the method value of a nil pointer is
// legal Go, so the knob would silently read false / 0 and disable the sweep. Fall
// back to the compiled-in default and say so loudly instead.
//
// Both tolerate a nil logger defensively, for callers other than New: New itself
// already dereferences cfg.Logger well before it gets here (NewFSMOpProducer and
// friends), so a nil logger has panicked long since on that path.
func boolGetter(logger *logrus.Logger, dv *runtime.DynamicValue[bool], knob string, fallback bool) func() bool {
	if dv != nil {
		return dv.Get
	}
	if logger != nil {
		logger.Errorf("replication cleanup config not wired: %s; falling back to built-in default", knob)
	}
	return func() bool { return fallback }
}

func durationGetter(logger *logrus.Logger, dv *runtime.DynamicValue[time.Duration], knob string, fallback time.Duration) func() time.Duration {
	if dv != nil {
		return dv.Get
	}
	if logger != nil {
		logger.Errorf("replication cleanup config not wired: %s; falling back to built-in default", knob)
	}
	return func() time.Duration { return fallback }
}

// jitterUpTo spreads the sweepers' first tick across the interval so a cluster
// restarted together does not converge on one instant.
func jitterUpTo(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	return time.Duration(rand.Int63n(int64(d)))
}

// Service class serves as the primary entry point for the Raft layer, managing and coordinating
// the key functionalities of the distributed consensus protocol.
type Service struct {
	*Raft

	replicationEngine *replication.ShardReplicationEngine
	opCleaner         *replication.OpCleaner
	raftAddr          string
	config            *Config

	rpcClient *rpc.Client
	rpcServer *rpc.Server
	logger    *logrus.Logger

	// closing channels
	cancelReplicationEngine context.CancelFunc
	cancelOpCleaner         context.CancelFunc
	closeBootstrapper       chan struct{}
	closeOnFSMCaughtUp      chan struct{}
	closeWaitForDB          chan struct{}
}

// New returns a Service configured with cfg. The service will initialize internals gRPC api & clients to other cluster
// nodes.
// Raft store will be initialized and ready to be started. To start the service call Open().
func New(cfg Config, authZController authorization.Controller, snapshotter fsm.Snapshotter, svrMetrics *monitoring.GRPCServerMetrics) *Service {
	client := rpc.NewClient(resolver.NewRpc(cfg.IsLocalHost, cfg.RPCPort), cfg.RaftRPCMessageMaxSize, cfg.SentryEnabled, cfg.Logger)

	fsm := NewFSM(cfg, authZController, snapshotter, prometheus.DefaultRegisterer)
	raft := NewRaft(cfg.NodeSelector, &fsm, client)
	// Every state-transition apply on this node broadcasts the new state
	// into every peer's PerNodeState map; the consumer waits on it locally.
	fsm.replicationManager.SetLogger(cfg.Logger)
	fsm.replicationManager.SetNodeReachedStateSubmitter(cfg.NodeID, raft.SubmitNodeReachedState)
	fsmOpProducer := replication.NewFSMOpProducer(
		cfg.Logger,
		fsm.replicationManager.GetReplicationFSM(),
		fsmOpProducerPollingInterval,
		cfg.NodeSelector.LocalName(),
	)
	replicaCopyOpConsumer := replication.NewCopyOpConsumer(
		cfg.Logger,
		raft,
		cfg.ReplicaCopier,
		cfg.NodeSelector.LocalName(),
		&backoff.StopBackOff{},
		replication.NewOpsCache(),
		replicationOperationTimeout,
		cfg.ReplicationEngineMaxWorkers,
		metrics.NewReplicationEngineOpsCallbacks(prometheus.DefaultRegisterer),
		raft.SchemaReader(),
	)
	replicationEngine := replication.NewShardReplicationEngine(
		cfg.Logger,
		cfg.NodeSelector.LocalName(),
		fsmOpProducer,
		replicaCopyOpConsumer,
		shardReplicationEngineBufferSize,
		cfg.ReplicationEngineMaxWorkers,
		replicationEngineShutdownTimeout,
		metrics.NewReplicationEngineCallbacks(prometheus.DefaultRegisterer),
	)
	svr := rpc.NewServer(&fsm, raft, net.JoinHostPort(cfg.BindAddr, fmt.Sprintf("%d", cfg.RPCPort)), cfg.RaftRPCMessageMaxSize, cfg.SentryEnabled, svrMetrics, cfg.Logger)

	opCleaner, err := replication.NewOpCleaner(replication.OpCleanerParams{
		Logger:     cfg.Logger,
		NodeID:     cfg.NodeID,
		FSM:        fsm.replicationManager.GetReplicationFSM(),
		Remover:    raft,
		Clock:      clockwork.NewRealClock(),
		Registerer: prometheus.DefaultRegisterer,
		// The whole-tick gate, verbatim from shouldLogSlowApply: the loop runs on
		// every node and a tick on a follower ends right here.
		ReadyToSweep: func() bool {
			return raft.store.IsLeader() && raft.store.Ready() && raft.store.FSMHasCaughtUp()
		},
		IsLeader:         raft.store.IsLeader,
		Enabled:          boolGetter(cfg.Logger, cfg.ReplicaMovementCleanupEnabled, "REPLICA_MOVEMENT_CLEANUP_ENABLED", false),
		MaxAge:           durationGetter(cfg.Logger, cfg.ReplicaMovementCleanupMaxAge, "REPLICA_MOVEMENT_CLEANUP_MAX_AGE", defaultReplicaMovementCleanupMaxAge),
		Interval:         durationGetter(cfg.Logger, cfg.ReplicaMovementCleanupInterval, "REPLICA_MOVEMENT_CLEANUP_INTERVAL", defaultReplicaMovementCleanupInterval),
		IncludeCancelled: boolGetter(cfg.Logger, cfg.ReplicaMovementCleanupIncludeCancelled, "REPLICA_MOVEMENT_CLEANUP_INCLUDE_CANCELLED", false),
		Jitter:           jitterUpTo,
	})
	if err != nil && cfg.Logger != nil {
		// Reachable only with a nil dependency. The logger guard is defensive and
		// matches the getters above; New has already dereferenced cfg.Logger by this
		// point, so a nil one cannot actually get here. New cannot return an error,
		// so log it and leave the cleaner nil.
		cfg.Logger.Errorf("could not construct the replication cleanup sweeper: %v", err)
	}

	return &Service{
		Raft:               raft,
		replicationEngine:  replicationEngine,
		opCleaner:          opCleaner,
		raftAddr:           net.JoinHostPort(cfg.Host, fmt.Sprintf("%d", cfg.RaftPort)),
		config:             &cfg,
		rpcClient:          client,
		rpcServer:          svr,
		logger:             cfg.Logger,
		closeBootstrapper:  make(chan struct{}, 1),
		closeOnFSMCaughtUp: make(chan struct{}, 1),
		closeWaitForDB:     make(chan struct{}, 1),
	}
}

func (c *Service) onFSMCaughtUp(ctx context.Context) {
	if !c.config.ReplicaMovementEnabled {
		return
	}

	ticker := time.NewTicker(catchUpInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.closeOnFSMCaughtUp:
			return
		case <-ticker.C:
			if c.Raft.store.FSMHasCaughtUp() {
				c.logger.Infof("Metadata FSM reported caught up, starting replication engine")
				engineCtx, engineCancel := context.WithCancel(ctx)
				c.cancelReplicationEngine = engineCancel
				enterrors.GoWrapper(func() {
					// The context is cancelled by the engine itself when it is stopped
					if err := c.replicationEngine.Start(engineCtx); err != nil {
						if !errors.Is(err, context.Canceled) {
							c.logger.WithError(err).Error("replication engine failed to start after FSM caught up")
						}
					}
				}, c.logger)
				return
			}
		}
	}
}

// Open internal RPC service to handle node communication,
// bootstrap the Raft node, and restore the database state
func (c *Service) Open(ctx context.Context, db schema.Indexer) error {
	c.logger.WithField("servers", c.config.NodeNameToPortMap).Info("open cluster service")
	if err := c.rpcServer.Open(); err != nil {
		return fmt.Errorf("start rpc service: %w", err)
	}

	if err := c.Raft.Open(ctx, db); err != nil {
		return fmt.Errorf("open raft store: %w", err)
	}

	hasState, err := raft.HasExistingState(c.Raft.store.logCache, c.Raft.store.logStore, c.Raft.store.snapshotStore)
	if err != nil {
		return err
	}
	c.log.WithField("hasState", hasState).Info("raft init")

	// If we have a state in raft, we only want to re-join the nodes in raft_join list to ensure that we update the
	// configuration with our current ip.
	// If we have no state, we want to do the bootstrap procedure where we will try to join a cluster or notify other
	// peers that we are ready to form a new cluster.
	bootstrapCtx, bCancel := context.WithTimeout(ctx, c.config.BootstrapTimeout)
	defer bCancel()
	if hasState {
		joiner := bootstrap.NewJoiner(c.rpcClient, c.config.NodeID, c.raftAddr, c.config.Voter)
		err = backoff.Retry(func() error {
			joinNodes := bootstrap.ResolveRemoteNodes(c.config.NodeSelector, c.config.NodeNameToPortMap)
			_, err := joiner.Do(bootstrapCtx, c.logger, joinNodes)
			return err
		}, backoff.WithContext(backoff.NewConstantBackOff(1*time.Second), bootstrapCtx))
		if err != nil {
			return fmt.Errorf("could not join raft join list: %w. Weaviate detected this node to have state stored. If the DB is still loading up we will hit this timeout. You can try increasing/setting RAFT_BOOTSTRAP_TIMEOUT env variable to a higher value", err)
		}
	} else {
		bs := bootstrap.NewBootstrapper(
			c.rpcClient,
			c.config.NodeID,
			c.raftAddr,
			c.config.Voter,
			c.config.NodeSelector,
			c.Raft.Ready,
		)
		if err := bs.Do(
			bootstrapCtx,
			c.config.NodeNameToPortMap,
			c.logger,
			c.closeBootstrapper); err != nil {
			return fmt.Errorf("bootstrap: %w", err)
		}
	}

	if err := c.WaitUntilDBRestored(ctx, 1*time.Second, c.closeWaitForDB); err != nil {
		return fmt.Errorf("restore database: %w", err)
	}

	enterrors.GoWrapper(func() {
		c.onFSMCaughtUp(ctx)
	}, c.logger)

	// Deliberately outside onFSMCaughtUp: that function returns early when
	// ReplicaMovementEnabled is false while the FSM keeps accumulating ops, so the
	// sweeper must not inherit that gate. The loop runs on every node; leadership
	// is handled inside Tick.
	if c.opCleaner != nil {
		cleanerCtx, cleanerCancel := context.WithCancel(ctx)
		c.cancelOpCleaner = cleanerCancel
		enterrors.GoWrapper(func() {
			if err := c.opCleaner.Run(cleanerCtx); err != nil && !errors.Is(err, context.Canceled) {
				c.logger.Errorf("replication cleanup loop stopped: %v", err)
			}
		}, c.logger)
	}
	return nil
}

// Close closes the raft service and frees all allocated ressources. Internal RAFT store will be closed and if
// leadership is assumed it will be transferred to another node. gRPC server and clients will also be closed.
func (c *Service) Close(ctx context.Context) error {
	enterrors.GoWrapper(func() {
		c.closeBootstrapper <- struct{}{}
		c.closeWaitForDB <- struct{}{}
		c.closeOnFSMCaughtUp <- struct{}{}
	}, c.logger)

	if c.config.ReplicaMovementEnabled {
		c.logger.Info("closing replication engine ...")
		if c.cancelReplicationEngine != nil {
			c.cancelReplicationEngine()
		}
		c.replicationEngine.Stop()
	}

	// Outside the conditional: the cleanup loop runs regardless of
	// ReplicaMovementEnabled, and the manager's context is created unconditionally.
	// Nil-guarded because Open has early-return paths that leave the cancel unset.
	if c.cancelOpCleaner != nil {
		c.cancelOpCleaner()
	}
	// Cancel any in-flight node-reached-state broadcast/drain retry loops.
	if c.Raft != nil && c.Raft.store != nil && c.Raft.store.replicationManager != nil {
		c.Raft.store.replicationManager.Close()
	}

	c.logger.Info("closing raft FSM store ...")
	if err := c.Raft.Close(ctx); err != nil {
		return err
	}

	c.logger.Info("closing raft-rpc client ...")
	c.rpcClient.Close()

	c.logger.Info("closing raft-rpc server ...")
	c.rpcServer.Close()
	return nil
}

// Ready returns or not whether the node is ready to accept requests.
func (c *Service) Ready() bool {
	return c.Raft.Ready()
}

// LeaderWithID is used to return the current leader address and ID of the cluster.
// It may return empty strings if there is no current leader or the leader is unknown.
func (c *Service) LeaderWithID() (string, string) {
	return c.Raft.LeaderWithID()
}

func (c *Service) StorageCandidates() []string {
	return c.Raft.StorageCandidates()
}
