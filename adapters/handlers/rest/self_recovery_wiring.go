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

package rest

import (
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/schema"
)

// selfRecoverySchemaReader adapts schema.SchemaReader to selfrecovery.SchemaReader, avoiding an import cycle.
type selfRecoverySchemaReader struct {
	r schema.SchemaReader
}

func (a selfRecoverySchemaReader) ShardReplicas(class, shard string) ([]string, error) {
	return a.r.ShardReplicas(class, shard)
}

// selfRecoveryDBPathResolver adapts *db.DB.ShardPath to selfrecovery.PathResolver.
type selfRecoveryDBPathResolver struct {
	db   *db.DB
	root string // unused; reserved for future multi-root layouts
}

func (a selfRecoveryDBPathResolver) ShardPath(collection, shard string) string {
	return a.db.ShardPath(collection, shard)
}
