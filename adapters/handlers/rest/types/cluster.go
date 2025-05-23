package types

import "context"

// ClusterServer defines the interface for cluster API server operations
type ClusterServer interface {
	Serve() error
	Close(ctx context.Context) error
}
