package db

import "github.com/weaviate/weaviate/cluster/distributedtask"

var (
	_ distributedtask.Provider               = (*DropVectorIndexProvider)(nil)
	_ distributedtask.UnitAwareProvider      = (*DropVectorIndexProvider)(nil)
	_ distributedtask.ConflictDetector       = (*DropVectorIndexProvider)(nil)
	_ distributedtask.SchemaMutationDetector = (*DropVectorIndexProvider)(nil)
	_ distributedtask.RecoveryAwareProvider  = (*DropVectorIndexProvider)(nil)
)
