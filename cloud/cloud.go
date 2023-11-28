package cloud

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/weaviate/weaviate/cloud/store"
	"github.com/weaviate/weaviate/cloud/transport"
)

// Service class serves as the primary entry point for the Raft layer, managing and coordinating
// the key functionalities of the distributed consensus protocol.

type Service struct {
	*store.Service
	nodeName   string
	raftAddr   string
	client     *transport.Client
	rpcService *transport.Cluster
	logger     *slog.Logger
}

func New(cfg store.Config) Service {
	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.RPCPort)
	cl := transport.NewClient(transport.NewRPCResolver(cfg.IsLocalHost, cfg.RPCPort))
	fsm := store.New(cfg)
	server := store.NewService(&fsm, cl)
	return Service{
		Service:    server,
		nodeName:   cfg.NodeID,
		raftAddr:   fmt.Sprintf("%s:%d", cfg.Host, cfg.RaftPort),
		client:     cl,
		rpcService: transport.NewCluster(&fsm, server, addr, cfg.Logger),
		logger:     cfg.Logger,
	}
}

// Open internal RPC service to handle node communication,
// bootstrap the Raft node, and restore the database state
func (c *Service) Open(ctx context.Context, servers []string, db store.DB) error {
	if err := c.rpcService.Open(); err != nil {
		return fmt.Errorf("start rpc service: %w", err)
	}

	if err := c.Service.Open(ctx, db); err != nil {
		return fmt.Errorf("open raft store: %w", err)
	}

	bs := store.NewBootstrapper(c.client, c.nodeName, c.raftAddr)
	bTimeout := time.Second * 60 // TODO make timeout configurable
	bCtx, bCancel := context.WithTimeout(ctx, bTimeout)
	defer bCancel()
	if err := bs.Do(bCtx, servers, c.logger); err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}
	if err := c.WaitUntilDBRestored(ctx, 10*time.Second); err != nil {
		return fmt.Errorf("restore database: %w", err)
	}

	return nil
}

func (c *Service) Close(ctx context.Context) (err error) {
	err = c.Service.Close(ctx)
	c.rpcService.Close()
	return
}

func (c *Service) Ready() bool {
	return c.Service.Ready()
}
