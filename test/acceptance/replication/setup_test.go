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
	t.Run("EventualConsistency", eventualReplicaCRUD)
	t.Run("ScaleOut", multiShardScaleOut)
	t.Run("ReadRepair", readRepair)
	t.Run("AsyncRepair", asyncRepairSimpleScenario)
	t.Run("AsyncRepairInsertion", asyncRepairObjectInsertionScenario)
	t.Run("AsyncRepairUpdate", asyncRepairObjectUpdateScenario)
	t.Run("AsyncRepairDelete", asyncRepairObjectDeleteScenario)
	t.Run("GraphqlSearch", graphqlSearch)
	t.Run("MultiTenancy", multiTenancyEnabled)
}
