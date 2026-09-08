//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package acceptance_with_go_client

import (
	"acceptance_tests_with_client/internal/wvhost"
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v6/collections"
	"github.com/weaviate/weaviate-go-client/v6/data"
	"github.com/weaviate/weaviate/entities/models"
)

const (
	UUID1 = "10523cdd-15a2-42f4-81fa-267fe92f7cd6"
	UUID2 = ("5b6a08ba-1d46-43aa-89cc-8b070790c6f2")
)

func TestBatchReferenceCreateNoObjects(t *testing.T) {
	ctx := t.Context()
	c := wvhost.NewClient(t)

	collectionFrom := "GreenTeddyFlowerFrom"
	collectionTo := "GreenTeddyFlowerTo"
	uuid1, uuid2 := uuid.MustParse(UUID1), uuid.MustParse(UUID2)

	// delete class if exists and cleanup after test
	c.Collections.Delete(ctx, collectionFrom)
	t.Cleanup(func() { c.Collections.Delete(context.Background(), collectionFrom) })
	c.Collections.Delete(ctx, collectionTo)
	t.Cleanup(func() { c.Collections.Delete(context.Background(), collectionTo) })

	to, err := c.Collections.Create(ctx, collections.Collection{Name: collectionTo})
	require.NoError(t, err)
	require.NotNilf(t, to, "%q collection handle", collectionTo)

	from, err := c.Collections.Create(ctx, collections.Collection{
		Name: collectionFrom,
		References: []collections.Reference{
			{Name: "ref", Collections: []string{collectionTo}},
		},
	})
	require.NoError(t, err)
	require.NotNilf(t, from, "%q collection handle", collectionTo)

	// no objects exist, ref must fail - note that we tolerate if the target does not exist, however the source must exist
	ref := data.Reference{
		Origin: data.ObjectPath{
			Property: "ref",
			UUID:     uuid1,
		},
		UUID: uuid2,
	}
	_, err = from.Data.AddReferences(ctx, ref)

	var partial data.AddReferencesError
	if assert.ErrorAs(t, err, &partial) {
		require.Len(t, partial.Errors, 1, "number of failed inserts")
		require.Contains(t, partial.Errors, ref, "add-reference from %+v must fail", ref)
	}
}

func TestBatchReferenceTargetIsMT(t *testing.T) {
	client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: wvhost.REST()})
	require.Nil(t, err)

	classNameFrom := "RedTeddyFlowerFrom"
	classNameTo := "RedTeddyFlowerTo"

	// delete class if exists and cleanup after test
	client.Schema().ClassDeleter().WithClassName(classNameFrom).Do(ctx)
	defer client.Schema().ClassDeleter().WithClassName(classNameFrom).Do(ctx)
	client.Schema().ClassDeleter().WithClassName(classNameTo).Do(ctx)
	defer client.Schema().ClassDeleter().WithClassName(classNameTo).Do(ctx)

	classTo := &models.Class{Class: classNameTo, Vectorizer: "none", MultiTenancyConfig: &models.MultiTenancyConfig{
		Enabled: true,
	}}
	require.Nil(t, client.Schema().ClassCreator().WithClass(classTo).Do(ctx))
	require.Nil(t, client.Schema().TenantsCreator().
		WithClassName(classNameTo).
		WithTenants(models.Tenant{Name: "Tenant"}).
		Do(context.Background()))

	require.Nil(t, err)
	classFrom := &models.Class{
		Class: classNameFrom,
		Properties: []*models.Property{
			{Name: "ref", DataType: []string{classNameTo}},
		},
		Vectorizer: "none",
	}
	require.Nil(t, client.Schema().ClassCreator().WithClass(classFrom).Do(ctx))

	// add object to target and source class
	_, err = client.Data().Creator().WithClassName(classNameTo).WithID(UUID1).WithTenant("Tenant").WithProperties(map[string]interface{}{}).Do(ctx)
	require.Nil(t, err)
	_, err = client.Data().Creator().WithClassName(classNameFrom).WithID(UUID2).WithProperties(map[string]interface{}{}).Do(ctx)
	require.Nil(t, err)

	rpb := client.Batch().ReferencePayloadBuilder().
		WithFromClassName(classNameFrom).
		WithFromRefProp("ref").
		WithFromID(UUID2).
		WithToID(UUID1) // no to class supplied, will be auto-detected
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
		assert.NotNil(t, resp[i].Result.Errors)
	}
}
