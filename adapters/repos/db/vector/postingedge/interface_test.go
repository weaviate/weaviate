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

package postingedge_test

import (
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/postingedge"
)

// The interface the shard drives every vector index through. The index a shard
// opens is Persistent. Nothing wires the one in memory into a shard, so nothing
// else checks that it satisfies it.
var (
	_ db.VectorIndex = (*postingedge.Persistent)(nil)
	_ db.VectorIndex = (*postingedge.Index)(nil)
)
