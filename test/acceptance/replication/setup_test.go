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

func TestReplication_ImmediateReplicaCRUD(t *testing.T) {
	t.Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")
	t.Run("immediate replica CRUD", immediateReplicaCRUD)
}

func TestReplication_EventualReplicaCRUD(t *testing.T) {
	t.Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")
	t.Run("eventual replica CRUD", eventualReplicaCRUD)
}

func TestReplication_MultiShardScaleOut(t *testing.T) {
	t.Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")
	t.Run("multishard scale out", multiShardScaleOut)
}

func TestReplication_ReadRepair(t *testing.T) {
	t.Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")
	t.Run("read repair", readRepair)
}

func TestReplication_GraphqlSearch(t *testing.T) {
	t.Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")
	t.Run("graphql search", graphqlSearch)
}

func TestReplication_MultiTenancyEnabled(t *testing.T) {
	t.Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")
	t.Run("multi-tenancy enabled", multiTenancyEnabled)
}
