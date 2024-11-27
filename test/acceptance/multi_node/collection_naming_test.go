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

package multi_node

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

func TestCollectionNamingGQL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.New().
		With3NodeCluster().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	testCollection := "testcollectioncase"

	helper.SetupClient(compose.GetWeaviate().URI()) // node1
	helper.CreateClass(t, &models.Class{
		Class:      testCollection,
		Vectorizer: "none",
	})
	helper.GetClass(t, testCollection) // try to get from same node

	timeout := 2 * time.Second
	compose.Stop(ctx, docker.Weaviate1, &timeout) // stop node1

	helper.SetupClient(compose.GetWeaviateNode2().URI()) // node2
	helper.GetClass(t, testCollection)                   // try getting it from different node
	helper.DeleteClass(t, testCollection)                // try deleting it from different node

	// collection should not exist
	class, err := helper.GetClassWithoutAssert(t, testCollection)
	require.Nil(t, class)
	require.Error(t, err)
}
