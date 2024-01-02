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

import "testing"

func TestReplication(t *testing.T) {
	t.Run("immediate replica CRUD", immediateReplicaCRUD)
	t.Run("eventual replica CRUD", eventualReplicaCRUD)
	t.Run("multishard scale out", multiShardScaleOut)
	t.Run("read repair", readRepair)
	t.Run("graphql search", graphqlSearch)
	t.Run("multi-tenancy enabled", multiTenancyEnabled)
}
