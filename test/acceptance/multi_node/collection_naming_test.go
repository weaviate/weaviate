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

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

func TestCollectionNamingGQL(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		With3NodeCluster().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	cases := []struct {
		name       string
		createWith string
		deleteWith string
		getWith    string
		shouldFail bool
	}{
		{
			name:       "all-lower",
			createWith: "testcollectioncase",
			deleteWith: "testcollectioncase",
			getWith:    "testcollectioncase",
		},
		{
			name:       "create-lower-get-delete-GQL",
			createWith: "testcollectioncase",
			deleteWith: "Testcollectioncase",
			getWith:    "Testcollectioncase",
		},
		{
			name:       "create-mixed-get-delete-lower",
			createWith: "testCollectionCase",
			deleteWith: "testCollectionCase",
			getWith:    "testCollectionCase",
		},
		{
			name:       "create-mixed-get-delete-GQL",
			createWith: "testCollectionCase",
			deleteWith: "TestCollectionCase",
			getWith:    "TestCollectionCase",
		},
		{
			name:       "create-lower-get-delete-nonGQL-should-fail",
			createWith: "testCollectionCase",
			deleteWith: "TestCoLLectionCase", // not GQL
			getWith:    "TestCoLLectionCase", // not GQL
			shouldFail: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			helper.SetupClient(compose.GetWeaviate().URI()) // node1
			helper.CreateClass(t, &models.Class{
				Class:      c.createWith,
				Vectorizer: "none",
			})

			if c.shouldFail {
				// get should fail
				class, err := helper.GetClassWithoutAssert(t, c.getWith)
				require.Nil(t, class)
				require.Error(t, err)

				helper.SetupClient(compose.GetWeaviateNode2().URI())    // node2
				class, err = helper.GetClassWithoutAssert(t, c.getWith) // try getting it from different node
				require.Nil(t, class)
				require.Error(t, err)

				// delete should fail
				helper.DeleteClass(t, c.deleteWith) // try deleting it from different node
				// make sure after delete, collection still exist with original created name
				helper.GetClass(t, c.createWith)

				return
			}

			helper.GetClass(t, c.getWith) // try to get from same node

			helper.SetupClient(compose.GetWeaviateNode2().URI()) // node2
			helper.GetClass(t, c.getWith)                        // try getting it from different node
			helper.DeleteClass(t, c.deleteWith)                  // try deleting it from different node

			// make sure after delete, collection should not exist
			class, err := helper.GetClassWithoutAssert(t, c.deleteWith)
			require.Nil(t, class)
			require.Error(t, err)
		})
	}
}
