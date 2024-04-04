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

package replication

import (
	"testing"
)

func TestReplication(t *testing.T) {
	t.Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")
	t.Run("SyncReplication", immediateReplicaCRUD)
	t.Run("eventual replica CRUD", eventualReplicaCRUD)
	t.Run("multishard scale out", multiShardScaleOut)
	t.Run("read repair", readRepair)
	t.Run("async repair simple scenario", asyncRepairSimpleScenario)
	t.Run("async repair object insertion scenario", asyncRepairObjectInsertionScenario)
	t.Run("async repair object update scenario", asyncRepairObjectUpdateScenario)
	// t.Run("async repair object delete scenario", asyncRepairObjectDeleteScenario)
	t.Run("graphql search", graphqlSearch)
	t.Run("multi-tenancy enabled", multiTenancyEnabled)
}
