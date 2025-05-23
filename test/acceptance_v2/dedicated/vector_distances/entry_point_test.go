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

package vector_distances

import (
	"testing"

	"github.com/weaviate/weaviate/client"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/acceptance_v2/dedicated"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

func TestDedicatedVectorDistances(t *testing.T) {
	t.Parallel()

	compose := dedicated.SetupDedicated(t, func(t *testing.T) *docker.Compose {
		return docker.New().With1NodeCluster().WithWeaviateBasicAuth("user", "pass").WithText2VecContextionary()
	})
	client := helper.ClientFromURI(t, compose.GetWeaviate().URI())

	t.Run("setup test schema (cosine only)", func(t *testing.T) { addTestSchemaCosine(t, client) })
	// at this point only cosine is present, so we can evaluate both Get and
	// Explore
	t.Run("import cosine test data ", func(t *testing.T) { addTestDataCosine(t, client) })
	t.Run("test cosine distance", func(t *testing.T) { testCosine(t, client) })

	// import rest of the schema meaning, we can only test Get, Explore is now
	// impossible
	t.Run("setup test schema (other)", func(t *testing.T) { addTestSchemaOther(t, client) })

	t.Run("import dot test data ", func(t *testing.T) { addTestDataDot(t, client) })
	t.Run("import l2 test data ", func(t *testing.T) { addTestDataL2(t, client) })
	t.Run("import manhattan test data", func(t *testing.T) { addTestDataManhattan(t, client) })
	t.Run("import hamming test data", func(t *testing.T) { addTestDataHamming(t, client) })
	t.Run("test dot distance", func(t *testing.T) { testDot(t, client) })
	t.Run("test l2 distance", func(t *testing.T) { testL2(t, client) })
	t.Run("test hamming distance", func(t *testing.T) { testHamming(t, client) })
	t.Run("test manhattan distance", func(t *testing.T) { testManhattan(t, client) })

	// tear down what we no longer need
	deleteObjectClass(t, client, "Cosine_Class")
	deleteObjectClass(t, client, "Dot_Class")
	deleteObjectClass(t, client, "Manhattan_Class")
	deleteObjectClass(t, client, "Hamming_Class")
	deleteObjectClass(t, client, "C1")

	// now only l2 is left so we can test explore with L2
	t.Run("explore across multiple non-cosine classes", func(t *testing.T) { testExplore(t, client) })
	// tear down remaining classes
	deleteObjectClass(t, client, "L2Squared_Class")
}

func createObjectClass(t *testing.T, client *client.Weaviate, class *models.Class) {
	params := schema.NewSchemaObjectsCreateParams().WithObjectClass(class)
	resp, err := client.Schema.SchemaObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)
}

func createObject(t *testing.T, client *client.Weaviate, object *models.Object) {
	params := objects.NewObjectsCreateParams().WithBody(object)
	resp, err := client.Objects.ObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)
}

func deleteObjectClass(t *testing.T, client *client.Weaviate, class string) {
	delParams := schema.NewSchemaObjectsDeleteParams().WithClassName(class)
	delRes, err := client.Schema.SchemaObjectsDelete(delParams, nil)
	helper.AssertRequestOk(t, delRes, err, nil)
}
