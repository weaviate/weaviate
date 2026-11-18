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

package db

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	resolver "github.com/weaviate/weaviate/adapters/repos/db/sharding"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/multi"

	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/namespaces"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

func TestMultiGetCrossRef(t *testing.T) {
	const (
		tenant     = "foo-tenant"
		sourceMTID = strfmt.UUID("11111111-1111-4111-8111-111111111111")
		targetSTID = strfmt.UUID("22222222-2222-4222-8222-222222222222")
		leafSTID   = strfmt.UUID("33333333-3333-4333-8333-333333333333")
		targetMTID = strfmt.UUID("44444444-4444-4444-8444-444444444444")
	)

	logger, _ := test.NewNullLogger()
	db := &DB{logger: logger, indices: map[string]*Index{}}

	leafSTIdx := crossRefTestIndex(t, "LeafST", false, &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:    leafSTID,
			Class: "LeafST",
			Properties: map[string]interface{}{
				"label": "leaf-single-tenant",
			},
		},
	})
	targetSTIdx := crossRefTestIndex(t, "TargetST", false, &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:    targetSTID,
			Class: "TargetST",
			Properties: map[string]interface{}{
				"title": "shared-single-tenant",
				"toLeaf": models.MultipleRef{
					&models.SingleRef{
						Beacon: strfmt.URI("weaviate://localhost/LeafST/" + string(leafSTID)),
					},
				},
			},
		},
	})
	targetMTIdx := crossRefTestIndex(t, "TargetMT", true, &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:     targetMTID,
			Class:  "TargetMT",
			Tenant: tenant,
			Properties: map[string]interface{}{
				"title": "scoped-multi-tenant",
				"toST": models.MultipleRef{
					&models.SingleRef{
						Beacon: strfmt.URI("weaviate://localhost/TargetST/" + string(targetSTID)),
					},
				},
			},
		},
	})
	sourceMTIdx := crossRefTestIndex(t, "SourceMT", true, &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:     sourceMTID,
			Class:  "SourceMT",
			Tenant: tenant,
			Properties: map[string]interface{}{
				"name": "origin-mt",
				"ref": models.MultipleRef{
					&models.SingleRef{
						Beacon: strfmt.URI("weaviate://localhost/TargetST/" + string(targetSTID)),
					},
				},
			},
		},
	})

	for _, idx := range []*Index{leafSTIdx, targetSTIdx, targetMTIdx, sourceMTIdx} {
		db.indices[idx.ID()] = idx
	}

	t.Run("ResolveReferences resolves MT to single-tenant cross-reference", func(t *testing.T) {
		input := search.Results{
			{
				ID:        sourceMTID,
				ClassName: "SourceMT",
				Tenant:    tenant,
				Schema: map[string]interface{}{
					"name": "origin-mt",
					"ref": models.MultipleRef{
						&models.SingleRef{
							Beacon: strfmt.URI("weaviate://localhost/TargetST/" + string(targetSTID)),
						},
					},
				},
			},
		}
		selectProps := search.SelectProperties{
			{Name: "name", IsPrimitive: true},
			{
				Name: "ref",
				Refs: []search.SelectClass{
					{
						ClassName: "TargetST",
						RefProperties: search.SelectProperties{
							{Name: "title", IsPrimitive: true},
						},
					},
				},
			},
		}

		res, err := db.ResolveReferences(t.Context(), input, selectProps, nil, additional.Properties{}, tenant)
		require.NoError(t, err)
		require.Len(t, res, 1)

		schemaMap, ok := res[0].Schema.(map[string]interface{})
		require.True(t, ok)
		refs, ok := schemaMap["ref"].([]interface{})
		require.True(t, ok)
		require.Len(t, refs, 1)

		localRef, ok := refs[0].(search.LocalRef)
		require.True(t, ok)
		assert.Equal(t, "TargetST", localRef.Class)
		assert.Equal(t, "shared-single-tenant", localRef.Fields["title"])
	})

	t.Run("MultiGet mixes multi-tenant and single-tenant targets without leaking tenant", func(t *testing.T) {
		res, err := db.MultiGet(t.Context(), []multi.Identifier{
			{ID: string(targetSTID), ClassName: "TargetST"},
			{ID: string(targetMTID), ClassName: "TargetMT"},
		}, additional.Properties{}, tenant)
		require.NoError(t, err)
		require.Len(t, res, 2)

		assert.Equal(t, targetSTID, res[0].ID)
		assert.Equal(t, "", res[0].Tenant, "single-tenant result must not inherit caller tenant")
		assert.Equal(t, targetMTID, res[1].ID)
		assert.Equal(t, tenant, res[1].Tenant)
	})

	t.Run("ResolveReferences resolves multi-hop MT->ST->ST and MT->MT->ST chains", func(t *testing.T) {
		input := search.Results{
			{
				ID:        sourceMTID,
				ClassName: "SourceMT",
				Tenant:    tenant,
				Schema: map[string]interface{}{
					"toST": models.MultipleRef{
						&models.SingleRef{
							Beacon: strfmt.URI("weaviate://localhost/TargetST/" + string(targetSTID)),
						},
					},
					"toMT": models.MultipleRef{
						&models.SingleRef{
							Beacon: strfmt.URI("weaviate://localhost/TargetMT/" + string(targetMTID)),
						},
					},
				},
			},
		}
		selectProps := search.SelectProperties{
			{
				Name: "toST",
				Refs: []search.SelectClass{{
					ClassName: "TargetST",
					RefProperties: search.SelectProperties{
						{Name: "title", IsPrimitive: true},
						{
							Name: "toLeaf",
							Refs: []search.SelectClass{{
								ClassName:     "LeafST",
								RefProperties: search.SelectProperties{{Name: "label", IsPrimitive: true}},
							}},
						},
					},
				}},
			},
			{
				Name: "toMT",
				Refs: []search.SelectClass{{
					ClassName: "TargetMT",
					RefProperties: search.SelectProperties{
						{Name: "title", IsPrimitive: true},
						{
							Name: "toST",
							Refs: []search.SelectClass{{
								ClassName:     "TargetST",
								RefProperties: search.SelectProperties{{Name: "title", IsPrimitive: true}},
							}},
						},
					},
				}},
			},
		}

		res, err := db.ResolveReferences(t.Context(), input, selectProps, nil, additional.Properties{}, tenant)
		require.NoError(t, err)
		require.Len(t, res, 1)

		rootSchema := res[0].Schema.(map[string]interface{})

		stRefs := rootSchema["toST"].([]interface{})
		require.Len(t, stRefs, 1)
		stLocal := stRefs[0].(search.LocalRef)
		leafRefs := stLocal.Fields["toLeaf"].([]interface{})
		require.Len(t, leafRefs, 1)
		assert.Equal(t, "leaf-single-tenant", leafRefs[0].(search.LocalRef).Fields["label"])

		mtRefs := rootSchema["toMT"].([]interface{})
		require.Len(t, mtRefs, 1)
		mtLocal := mtRefs[0].(search.LocalRef)
		nestedSTRefs := mtLocal.Fields["toST"].([]interface{})
		require.Len(t, nestedSTRefs, 1)
		assert.Equal(t, "shared-single-tenant", nestedSTRefs[0].(search.LocalRef).Fields["title"])
	})

	t.Run("ResolveReferences resolves groupBy hit references from MT to ST", func(t *testing.T) {
		groupProps := search.SelectProperties{
			{
				Name: "ref",
				Refs: []search.SelectClass{{
					ClassName:     "TargetST",
					RefProperties: search.SelectProperties{{Name: "title", IsPrimitive: true}},
				}},
			},
		}
		input := search.Results{
			{
				ID:        sourceMTID,
				ClassName: "SourceMT",
				Tenant:    tenant,
				Schema:    map[string]interface{}{"name": "origin-mt"},
				AdditionalProperties: models.AdditionalProperties{
					"group": &additional.Group{
						Hits: []map[string]interface{}{
							{
								"ref": models.MultipleRef{
									&models.SingleRef{
										Beacon: strfmt.URI("weaviate://localhost/TargetST/" + string(targetSTID)),
									},
								},
							},
						},
					},
				},
			},
		}

		res, err := db.ResolveReferences(t.Context(), input, nil, &searchparams.GroupBy{
			Property:   "name",
			Properties: groupProps,
		}, additional.Properties{}, tenant)
		require.NoError(t, err)
		require.Len(t, res, 1)

		group := res[0].AdditionalProperties["group"].(*additional.Group)
		refs := group.Hits[0]["ref"].([]interface{})
		require.Len(t, refs, 1)
		assert.Equal(t, "shared-single-tenant", refs[0].(search.LocalRef).Fields["title"])
	})

	t.Run("DB.Object resolves single-object MT to ST cross-reference", func(t *testing.T) {
		selectProps := search.SelectProperties{
			{
				Name: "ref",
				Refs: []search.SelectClass{{
					ClassName:     "TargetST",
					RefProperties: search.SelectProperties{{Name: "title", IsPrimitive: true}},
				}},
			},
		}

		got, err := db.Object(t.Context(), "SourceMT", sourceMTID, selectProps, additional.Properties{}, nil, tenant)
		require.NoError(t, err)
		require.NotNil(t, got)

		schemaMap := got.Schema.(map[string]interface{})
		refs := schemaMap["ref"].([]interface{})
		require.Len(t, refs, 1)
		assert.Equal(t, "shared-single-tenant", refs[0].(search.LocalRef).Fields["title"])
	})

	t.Run("MultiGet preserves multi-tenancy errors for MT target and direct Index.multiObjectByID", func(t *testing.T) {
		_, err := db.MultiGet(t.Context(), []multi.Identifier{
			{ID: string(targetMTID), ClassName: "TargetMT"},
		}, additional.Properties{}, "")
		var mtErr objects.ErrMultiTenancy
		require.ErrorAs(t, err, &mtErr)

		_, err = targetSTIdx.multiObjectByID(t.Context(), []multi.Identifier{
			{ID: string(targetSTID), ClassName: "TargetST"},
		}, tenant)
		require.ErrorAs(t, err, &mtErr)
		assert.ErrorContains(t, err, "has multi-tenancy disabled, but request was with tenant")
	})
}

func crossRefTestIndex(t *testing.T, className string, multiTenant bool, obj *storobj.Object) *Index {
	t.Helper()

	exister := namespaces.NewMockExister(t)
	exister.EXPECT().GetNamespace(mock.Anything).
		Return(api.Namespace{State: api.NamespaceStateActive}, true).Maybe()
	withLookups := func(i *Index) {
		i.closeRequestedCtx, i.signalCloseRequested = context.WithCancelCause(context.Background())
		i.namespace = namespacing.NamespaceFromQualified(className)
		i.namespacesExister = exister
		i.shardResolver = resolver.NewShardResolver(className, multiTenant, i.getSchema)
	}

	var shard ShardLike
	var idx *Index
	if multiTenant {
		shard, idx = testShardMultiTenant(t, t.Context(), className, withLookups)
	} else {
		shard, idx = testShard(t, t.Context(), className, withLookups)
	}
	shutdownIndexOnCleanup(t, idx)
	require.NoError(t, shard.PutObject(t.Context(), obj))
	return idx
}
