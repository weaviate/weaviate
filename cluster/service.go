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

	"github.com/cenkalti/backoff/v4"
	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/bootstrap"
	"github.com/weaviate/weaviate/cluster/resolver"
	"github.com/weaviate/weaviate/cluster/rpc"
	"github.com/weaviate/weaviate/cluster/schema"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/cluster"
)

// Service class serves as the primary entry point for the Raft layer, managing and coordinating
// the key functionalities of the distributed consensus protocol.
type Service struct {
	*Raft

	raftAddr string
	config   *Config

	rpcClient *rpc.Client
	rpcServer *rpc.Server
	logger    *logrus.Logger

	// closing channels
	closeBootstrapper chan struct{}
	closeWaitForDB    chan struct{}
}

// New returns a Service configured with cfg. The service will initialize internals gRPC api & clients to other cluster
// nodes.
// Raft store will be initialized and ready to be started. To start the service call Open().
func New(selector cluster.NodeSelector, cfg Config) *Service {
	rpcListenAddress := fmt.Sprintf("%s:%d", cfg.Host, cfg.RPCPort)
	// When using FQDN lookup we might want to advertise a different IP than the one we'll be listening on.
	// This address is then sent to raft peers as the address this node listen on.
	// This address needs to proxy/forward to that node (think static ip for a service)
	// This is necessary to ensure that the FQDN ip will be stored in the raft logs.
	raftAdvertisedAddress := fmt.Sprintf("%s:%d", cfg.Host, cfg.RaftPort)
	if cfg.EnableFQDNResolver {
		addr := resolver.NewFQDN(resolver.FQDNConfig{
			// We don't need to specify more config as we are only using that resolver to resolve a node id to an IP
			// overriding the default resolver using memberlist.
			TLD: cfg.FQDNResolverTLD,
		}).NodeAddress(cfg.NodeID)
		if addr != "" {
			raftAdvertisedAddress = addr
		} else {
			cfg.Logger.Warnf("raft fqdn lookup configured but unable to resolve node %s to an IP, fallbacking to %s", cfg.NodeID, raftAdvertisedAddress)
		}
	}
	cl := rpc.NewClient(resolver.NewRpc(cfg.IsLocalHost, cfg.RPCPort), cfg.RaftRPCMessageMaxSize, cfg.SentryEnabled, cfg.Logger)
	fsm := NewFSM(cfg)
	raft := NewRaft(selector, &fsm, cl)
	return &Service{
		Raft:              raft,
		raftAddr:          raftAdvertisedAddress,
		config:            &cfg,
		rpcClient:         cl,
		rpcServer:         rpc.NewServer(&fsm, raft, rpcListenAddress, cfg.Logger, cfg.RaftRPCMessageMaxSize, cfg.SentryEnabled),
		logger:            cfg.Logger,
		closeBootstrapper: make(chan struct{}),
		closeWaitForDB:    make(chan struct{}),
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

	// If FQDN resolver is enabled make sure we're also using it for the bootstrapping process
	nodeToAddressResolver := c.config.NodeToAddressResolver
	if c.config.EnableFQDNResolver {
		nodeToAddressResolver = resolver.NewFQDN(resolver.FQDNConfig{
			// We don't need to specify more config as we are only using that resolver to resolve a node id to an IP
			// overriding the default resolver using memberlist.
			TLD: c.config.FQDNResolverTLD,
		})
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
			joinNodes := bootstrap.ResolveRemoteNodes(nodeToAddressResolver, c.config.NodeNameToPortMap)
			_, err := joiner.Do(bootstrapCtx, c.logger, joinNodes)
			return err
		}, backoff.WithContext(backoff.NewConstantBackOff(1*time.Second), bootstrapCtx))
		if err != nil {
			return fmt.Errorf("could not join raft join list: %w. Weaviate detected this node to have state stored. If the DB is still loading up we will hit this timeout. You can try increasing/setting RAFT_BOOTSTRAP_TIMEOUT env variable to a higher value.", err)
		}
	} else {
		bs := bootstrap.NewBootstrapper(
			c.rpcClient,
			c.config.NodeID,
			c.raftAddr,
			c.config.Voter,
			nodeToAddressResolver,
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

	if err := c.WaitUntilDBRestored(ctx, 10*time.Second, c.closeWaitForDB); err != nil {
		return fmt.Errorf("restore database: %w", err)
	}

	return nil
}

// Close closes the raft service and frees all allocated ressources. Internal RAFT store will be closed and if
// leadership is assumed it will be transferred to another node. gRPC server and clients will also be closed.
func (c *Service) Close(ctx context.Context) error {
	enterrors.GoWrapper(func() {
		c.closeBootstrapper <- struct{}{}
		c.closeWaitForDB <- struct{}{}
	}, c.logger)

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
