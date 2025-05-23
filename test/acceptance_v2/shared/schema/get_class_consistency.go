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

package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

func TestGetClassWithConsistency(t *testing.T, compose *docker.DockerCompose) {
	t.Parallel()

	client := helper.ClientFromURI(t, compose.GetWeaviateNode2().URI())
	className := "TestGetClassWithConsistency"
	assert.NotContains(t, GetObjectClassNamesWithClient(t, client), className)
	c := &models.Class{
		Class: className,
	}

	params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(c)
	_, err := client.Schema.SchemaObjectsCreate(params, nil)
	assert.Nil(t, err)

	truePtr := true
	paramsGet := clschema.NewSchemaObjectsGetParams().WithClassName(className).WithConsistency(&truePtr)
	res, err := client.Schema.SchemaObjectsGet(paramsGet, nil)
	require.Nil(t, err)
	// Check only the `Class` as the returned class from the server has all the defaults config initialized
	require.Equal(t, c.Class, res.Payload.Class)
}
