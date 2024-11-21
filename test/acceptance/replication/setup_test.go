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
	{
		// Sync replication
		t.Run("replication enabled at class creation", immediateReplicaCRUD)
		t.Run("replication enabled after class creation CRUD", eventualReplicaCRUD)
		t.Run("replication factor increase", replicationFactorIncrease)
		t.Run("read repair", readRepair)
		t.Run("read repair - delete on conflict", readRepairDeleteOnConflict)
		t.Run("read repair - timebased resolution", readRepairTimebasedResolution)
		t.Run("graphql search", graphqlSearch)
		t.Run("multi-tenancy enabled", multiTenancyEnabled)
	}
	{
		// Async replication
		t.Run("async repair simple scenario", asyncRepairSimpleScenario)
		t.Run("async repair object insertion scenario", asyncRepairObjectInsertionScenario)
		t.Run("async repair object update scenario", asyncRepairObjectUpdateScenario)
		t.Run("async repair object delete scenario", asyncRepairObjectDeleteScenario)
		t.Run("async repair with tenant operations", asyncRepairMultiTenancyScenario)
	}
}
