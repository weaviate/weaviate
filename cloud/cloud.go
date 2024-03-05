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

package cloud

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	schemaTypes "github.com/weaviate/weaviate/adapters/repos/schema/types"
	"github.com/weaviate/weaviate/cloud/store"
	"github.com/weaviate/weaviate/cloud/transport"
)

// Service class serves as the primary entry point for the Raft layer, managing and coordinating
// the key functionalities of the distributed consensus protocol.
type Service struct {
	*store.Service
	raftAddr string
	config   *store.Config

	client     *transport.Client
	rpcService *transport.Service
	logger     *slog.Logger
}

// New returns a new Service instance configured with cfg.
func New(cfg store.Config) *Service {
	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.RPCPort)
	cl := transport.NewClient(transport.NewRPCResolver(cfg.IsLocalHost, cfg.RPCPort))
	fsm := store.New(cfg)
	server := store.NewService(&fsm, cl)
	return &Service{
		Service:  server,
		raftAddr: fmt.Sprintf("%s:%d", cfg.Host, cfg.RaftPort),

		config:     &cfg,
		client:     cl,
		rpcService: transport.New(&fsm, server, addr, cfg.Logger),
		logger:     cfg.Logger,
	}
}

// Open internal RPC service to handle node communication,
// bootstrap the Raft node, and restore the database state
func (c *Service) Open(ctx context.Context, db store.Indexer) error {
	c.logger.Info("open cluster service", "servers", c.config.ServerName2PortMap)
	if err := c.rpcService.Open(); err != nil {
		return fmt.Errorf("start rpc service: %w", err)
	}

	if err := c.Service.Open(ctx, db); err != nil {
		return fmt.Errorf("open raft store: %w", err)
	}

	bs := store.NewBootstrapper(c.client, c.config.NodeID, c.raftAddr, c.config.AddrResolver)
	bTimeout := time.Second * 60 // TODO make timeout configurable
	bCtx, bCancel := context.WithTimeout(ctx, bTimeout)
	defer bCancel()
	if err := bs.Do(bCtx, c.config.ServerName2PortMap, c.logger, c.config.Voter); err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}
	if err := c.WaitUntilDBRestored(ctx, 10*time.Second); err != nil {
		return fmt.Errorf("restore database: %w", err)
	}

	return nil
}

// Close closes all underlying ressources (RPC service, DB and RAFT store).
// It returns an error if any of these fails to close.
func (c *Service) Close(ctx context.Context) error {
	err := c.Service.Close(ctx)
	c.rpcService.Close()
	return err
}

// Ready returns true if the underlying DB and store are open and ready for use
func (c *Service) Ready() bool {
	return c.Service.Ready()
}

// MigrateToRaft will start migrating schemaRepo to RAFT based schema representation (on *this* node if it is the leader
// node).
// This call will be blocking until the RAFT migration is either skipped or completed.
// It returns an error if the migration process started and failed.
func (c *Service) MigrateToRaft(schemaRepo schemaTypes.SchemaRepo) error {
	return c.Service.MigrateToRaft(schemaRepo)
}
