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

package db

import "github.com/weaviate/weaviate/cluster/distributedtask"

var (
	_ distributedtask.Provider               = (*DropVectorIndexProvider)(nil)
	_ distributedtask.UnitAwareProvider      = (*DropVectorIndexProvider)(nil)
	_ distributedtask.ConflictDetector       = (*DropVectorIndexProvider)(nil)
	_ distributedtask.SchemaMutationDetector = (*DropVectorIndexProvider)(nil)
	_ distributedtask.RecoveryAwareProvider  = (*DropVectorIndexProvider)(nil)
)

var (
	_ dropVectorShards          = (*DB)(nil)
	_ dropVectorSchemaFinalizer = (*schemaVectorConfigFinalizer)(nil)
)
