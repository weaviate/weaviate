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

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/store"
	"github.com/weaviate/weaviate/cluster/transport"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// Service class serves as the primary entry point for the Raft layer, managing and coordinating
// the key functionalities of the distributed consensus protocol.
type Service struct {
	*store.Service
	raftAddr string
	config   *store.Config

	client     *transport.Client
	rpcService *transport.Service
	logger     *logrus.Logger

	// closing channels
	closeBootstrapper chan struct{}
	closeWaitForDB    chan struct{}
}

func New(cfg store.Config) *Service {
	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.RPCPort)
	cl := transport.NewClient(transport.NewRPCResolver(cfg.IsLocalHost, cfg.RPCPort), cfg.RaftRPCMessageMaxSize)
	fsm := store.New(cfg)
	server := store.NewService(&fsm, cl)
	return &Service{
		Service:  server,
		raftAddr: fmt.Sprintf("%s:%d", cfg.Host, cfg.RaftPort),

		config:            &cfg,
		client:            cl,
		rpcService:        transport.New(&fsm, server, addr, cfg.Logger, cfg.RaftRPCMessageMaxSize),
		logger:            cfg.Logger,
		closeBootstrapper: make(chan struct{}),
		closeWaitForDB:    make(chan struct{}),
	}
}

// Open internal RPC service to handle node communication,
// bootstrap the Raft node, and restore the database state
func (c *Service) Open(ctx context.Context, db store.Indexer) error {
	c.logger.WithField("servers", c.config.ServerName2PortMap).Info("open cluster service")
	if err := c.rpcService.Open(); err != nil {
		return fmt.Errorf("start rpc service: %w", err)
	}

	if err := c.Service.Open(ctx, db); err != nil {
		return fmt.Errorf("open raft store: %w", err)
	}

	bs := store.NewBootstrapper(
		c.client,
		c.config.NodeID,
		c.raftAddr,
		c.config.AddrResolver)

	bCtx, bCancel := context.WithTimeout(ctx, c.config.BootstrapTimeout)
	defer bCancel()
	if err := bs.Do(
		bCtx,
		c.config.ServerName2PortMap,
		c.logger,
		c.config.Voter, c.closeBootstrapper); err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}

	if err := c.WaitUntilDBRestored(ctx, 10*time.Second, c.closeWaitForDB); err != nil {
		return fmt.Errorf("restore database: %w", err)
	}

	return nil
}

func (c *Service) Close(ctx context.Context) error {
	enterrors.GoWrapper(func() {
		c.closeBootstrapper <- struct{}{}
		c.closeWaitForDB <- struct{}{}
	}, c.logger)

	if err := c.Service.Close(ctx); err != nil {
		return err
	}

	c.logger.Info("closing raft-rpc client ...")

	if err := c.client.Close(); err != nil {
		return err
	}

	c.logger.Info("closing raft-rpc server ...")
	c.rpcService.Close()
	return nil
}

func (c *Service) Ready() bool {
	return c.Service.Ready()
}

// LeaderWithID is used to return the current leader address and ID of the cluster.
// It may return empty strings if there is no current leader or the leader is unknown.
func (c *Service) LeaderWithID() (string, string) {
	return c.Service.LeaderWithID()
}
