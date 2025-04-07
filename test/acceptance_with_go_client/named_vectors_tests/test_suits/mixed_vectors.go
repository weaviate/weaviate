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

package test_suits

import (
	"testing"
)

func AllMixedVectorsTests(endpoint string) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("schema", testMixedVectorsCreateSchema(endpoint))
		t.Run("add vectors", testMixedVectorsAddNewVectors(endpoint))
		t.Run("object", testMixedVectorsObject(endpoint))
		t.Run("batch byov", testMixedVectorsBatchBYOV(endpoint))
		t.Run("hybrid", testMixedVectorsHybrid(endpoint))
		t.Run("aggregate", testMixedVectorsAggregate(endpoint))
		t.Run("name forwarding", testMixedVectorsNamedForwarding(endpoint))
	}
}
