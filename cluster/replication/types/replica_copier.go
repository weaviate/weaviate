package types

import "context"

// ReplicaCopier see cluster/replication/copier.Copier
type ReplicaCopier interface {
	// CopyReplica see cluster/replication/copier.Copier.CopyReplica
	CopyReplica(ctx context.Context, sourceNode string, sourceCollection string, sourceShard string) error
}
