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

package test

import (
	"testing"

	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

func Test_GraphQL(t *testing.T) {
	t.Run("setup test schema (cosine only)", addTestSchemaCosine)
	// at this point only cosine is present, so we can evaluate both Get and
	// Explore
	t.Run("import cosine test data ", addTestDataCosine)
	t.Run("test cosine distance", testCosine)

	// import rest of the schema meaning, we can only test Get, Explore is now
	// impossible
	t.Run("setup test schema (all)", addTestSchemaOther)
	t.Run("import dot test data ", addTestDataDot)
	t.Run("test dot distance", testDot)
	t.Run("import l2 test data ", addTestDataL2)
	t.Run("test l2 distance", testL2)
	t.Run("import manhattan test data", addTestDataManhattan)
	t.Run("test manhattan distance", testManhattan)
	t.Run("import hamming test data", addTestDataHamming)
	t.Run("test hamming distance", testHamming)

	// tear down what we no longer need
	deleteObjectClass(t, "Cosine_Class")
	deleteObjectClass(t, "Dot_Class")
	deleteObjectClass(t, "Manhattan_Class")
	deleteObjectClass(t, "Hamming_Class")

	// now only l2 is left so we can test explore with L2
	t.Run("explore across multiple non-cosine classes", testExplore)

	// tear down remaining classes
	deleteObjectClass(t, "L2Squared_Class")
}

func createObjectClass(t *testing.T, class *models.Class) {
	params := schema.NewSchemaObjectsCreateParams().WithObjectClass(class)
	resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)
}

func createObject(t *testing.T, object *models.Object) {
	params := objects.NewObjectsCreateParams().WithBody(object)
	resp, err := helper.Client(t).Objects.ObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)
}

func deleteObjectClass(t *testing.T, class string) {
	delParams := schema.NewSchemaObjectsDeleteParams().WithClassName(class)
	delRes, err := helper.Client(t).Schema.SchemaObjectsDelete(delParams, nil)
	helper.AssertRequestOk(t, delRes, err, nil)
}
