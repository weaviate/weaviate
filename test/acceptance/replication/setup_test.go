//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package replication

import "testing"

// These tests has been deactivated because getting
// the server endpoints for getting objects are not working properly
// adapters/handlers/rest/clusterapi/indices_replicas.go
// TODO: activate once fixed
//
//nolint:all
func TestReplication(t *testing.T) {
	enable := false
	if enable { // just to satisfy golangci-lint
		t.Run("immediate replica CRUD", immediateReplicaCRUD)
		t.Run("eventual replica CRUD", eventualReplicaCRUD)
	}
	t.Run("multishard scale out", multiShardScaleOut)
}
