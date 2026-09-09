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
	"github.com/weaviate/weaviate-go-client/v6/collections"
	"github.com/weaviate/weaviate-go-client/v6/data"
	"github.com/weaviate/weaviate-go-client/v6/tenant"
)

const (
	UUID1 = "10523cdd-15a2-42f4-81fa-267fe92f7cd6"
	UUID2 = "5b6a08ba-1d46-43aa-89cc-8b070790c6f2"
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

	_, err := c.Collections.Create(ctx, collections.Collection{Name: collectionTo})
	require.NoError(t, err)

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
	ctx := t.Context()
	c := wvhost.NewClient(t)

	collectionFrom := "RedTeddyFlowerFrom"
	collectionTo := "RedTeddyFlowerTo"
	uuid1, uuid2 := uuid.MustParse(UUID1), uuid.MustParse(UUID2)

	// delete class if exists and cleanup after test
	c.Collections.Delete(ctx, collectionFrom)
	t.Cleanup(func() { c.Collections.Delete(context.Background(), collectionFrom) })
	c.Collections.Delete(ctx, collectionTo)
	t.Cleanup(func() { c.Collections.Delete(context.Background(), collectionTo) })

	to, err := c.Collections.Create(ctx, collections.Collection{
		Name:         collectionTo,
		MultiTenancy: &collections.MultiTenancyConfig{Enabled: true},
	})
	require.NoError(t, err)
	require.NotNilf(t, to, "%q collection handle", collectionTo)
	require.NoError(t, to.Tenants.Create(ctx, tenant.Tenant{Name: "john_doe"}))

	from, err := c.Collections.Create(ctx, collections.Collection{
		Name: collectionFrom,
		References: []collections.Reference{
			{Name: "ref", Collections: []string{collectionTo}},
		},
	})
	require.NoError(t, err)
	require.NotNilf(t, from, "%q collection handle", collectionFrom)

	// add object to target and source class
	to = to.WithOptions(collections.WithTenant("john_doe"))
	_, err = to.Data.Insert(ctx, &data.Object{UUID: &uuid1})
	require.NoError(t, err)

	_, err = from.Data.Insert(ctx, &data.Object{UUID: &uuid2})
	require.NoError(t, err)

	ref := data.Reference{
		Origin: data.ObjectPath{
			Property: "ref",
			UUID:     uuid2,
		},
		UUID: uuid1,
	}
	_, err = from.Data.AddReferences(ctx, ref)

	var partial data.AddReferencesError
	if assert.ErrorAs(t, err, &partial) {
		require.Len(t, partial.Errors, 1, "number of failed inserts")
		require.Contains(t, partial.Errors, ref, "add-reference from %+v must fail", ref)
	}
}
