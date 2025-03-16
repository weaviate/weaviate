package copier

import (
	"context"
)

// TODO
type Copier struct {
	t targetNode
}

// using targetNode interface here to avoid circular dependency
type targetNode interface {
	PauseAndListFiles(ctx context.Context,
		hostName, indexName, shardName string) error
}

// TODO
func New() *Copier {
	return &Copier{}
}

// TODO
func (c *Copier) Run(srcNodeId, destNodeId, collectionName, shardName string) error {
	// TODO context
	// TODO srcNodeId to host
	c.t.PauseAndListFiles(context.Background(), srcNodeId, collectionName, shardName)
	return nil
}
