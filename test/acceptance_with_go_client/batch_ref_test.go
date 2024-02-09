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

package acceptance_with_go_client

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate/entities/models"
)

func TestBatchReferenceCreateNoObjects(t *testing.T) {
	client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)

	classNameFrom := "GreenTeddyFlowerFrom"
	classNameTo := "GreenTeddyFlowerTo"

	// delete class if exists and cleanup after test
	client.Schema().ClassDeleter().WithClassName(classNameFrom).Do(ctx)
	defer client.Schema().ClassDeleter().WithClassName(classNameFrom).Do(ctx)
	client.Schema().ClassDeleter().WithClassName(classNameTo).Do(ctx)
	defer client.Schema().ClassDeleter().WithClassName(classNameTo).Do(ctx)

	classTo := &models.Class{Class: classNameTo, Vectorizer: "none"}
	require.Nil(t, client.Schema().ClassCreator().WithClass(classTo).Do(ctx))

	classFrom := &models.Class{
		Class: classNameFrom,
		Properties: []*models.Property{
			{Name: "ref", DataType: []string{classNameTo}},
		},
		Vectorizer: "none",
	}
	require.Nil(t, client.Schema().ClassCreator().WithClass(classFrom).Do(ctx))

	// no objects exist, ref must fail - note that we tolerate if the target does not exist, however the source must exist
	rpb := client.Batch().ReferencePayloadBuilder().
		WithFromClassName(classNameFrom).
		WithFromRefProp("ref").
		WithFromID("5b6a08ba-1d46-43aa-89cc-8b070790c6f2"). // uuids dont matter as we havent added any objects
		WithToClassName(classNameTo).
		WithToID("10523cdd-15a2-42f4-81fa-267fe92f7cd6")
	references := []*models.BatchReference{rpb.Payload()}

	resp, err := client.Batch().ReferencesBatcher().
		WithReferences(references...).
		Do(context.Background())
	require.Nil(t, err)
	require.NotNil(t, resp)
	assert.Len(t, resp, len(references))
	for i := range resp {
		require.NotNil(t, resp[i].Result)
		require.NotNil(t, resp[i].Result.Status)
		assert.Equal(t, "FAILED", *resp[i].Result.Status)
		assert.Nil(t, resp[i].Result.Errors)
	}
}
