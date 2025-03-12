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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

func TestGetClassWithConsistency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// 3 Node cluster so that we can verify that the proxy to leader feature work
	compose, err := docker.New().WithWeaviateCluster(3).
		WithText2VecContextionary().
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
	}()

	helper.SetupClient(compose.GetWeaviateNode2().URI())
	defer helper.ResetClient()

	className := t.Name()

	t.Run("asserting that this class does not exist yet", func(t *testing.T) {
		assert.NotContains(t, GetObjectClassNames(t), className)
	})

	c := &models.Class{
		Class: className,
	}

	params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(c)
	_, err = helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
	assert.Nil(t, err)

	truePtr := true
	paramsGet := clschema.NewSchemaObjectsGetParams().WithClassName(className).WithConsistency(&truePtr)
	res, err := helper.Client(t).Schema.SchemaObjectsGet(paramsGet, nil)
	require.Nil(t, err)
	// Check only the `Class` as the returned class from the server has all the defaults config initialized
	require.Equal(t, c.Class, res.Payload.Class)
}
