package types

// ReplicaCopier see cluster/replication/copier.Copier
type ReplicaCopier interface {
	// CopyReplica see cluster/replication/copier.Copier.CopyReplica
	CopyReplica(sourceNode string, sourceCollection string, sourceShard string) error
}
