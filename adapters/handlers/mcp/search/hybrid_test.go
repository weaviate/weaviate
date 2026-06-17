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

package search

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/mcp/auth"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// stubSchemaManager satisfies namespacing.SchemaManager. ResolveAlias returns
// whatever was placed in aliases, "" otherwise.
type stubSchemaManager struct {
	aliases map[string]string
}

func (s stubSchemaManager) ResolveAlias(alias string) string {
	return s.aliases[alias]
}

type stubSchemaReader struct {
	classes map[string]*models.Class
}

func (s stubSchemaReader) ReadOnlyClass(name string) *models.Class {
	return s.classes[name]
}

// recordingTraverser captures the GetParams it last received so tests can
// assert that the resolved class name flowed all the way through.
type recordingTraverser struct {
	gotParams dto.GetParams
}

func (r *recordingTraverser) GetClass(ctx context.Context, principal *models.Principal, params dto.GetParams) ([]any, error) {
	r.gotParams = params
	return []any{}, nil
}

func newSearcher(t *testing.T, principal *models.Principal, namespacesEnabled bool, aliases map[string]string) (*WeaviateSearcher, *recordingTraverser) {
	t.Helper()
	composer := func(token string, _ []string) (*models.Principal, error) {
		return principal, nil
	}
	authHandler := auth.NewAuth(false, composer, &authorization.DummyAuthorizer{}, nil)
	trav := &recordingTraverser{}
	logger, _ := test.NewNullLogger()
	return NewWeaviateSearcher(
		authHandler,
		trav,
		stubSchemaReader{},
		stubSchemaManager{aliases: aliases},
		namespacesEnabled,
		logger,
	), trav
}

func bearerReq() mcp.CallToolRequest {
	return mcp.CallToolRequest{Header: http.Header{"Authorization": []string{"Bearer dummy"}}}
}

// TestHybrid_NamespaceResolution covers the MCP search handler under
// namespacing: the resolved (qualified, alias-resolved) class flows into the
// traverser params, invalid prefixes from a namespaced principal are rejected,
// and filterext.Parse qualifies reference-path inner classes on NS clusters.
func TestHybrid_NamespaceResolution(t *testing.T) {
	aliases := map[string]string{
		"customer1:Films": "customer1:Movies",
	}

	refPathFilter := map[string]any{
		"path":      []any{"hasAuthor", "Author", "name"},
		"operator":  "Equal",
		"valueText": "Anyone",
	}
	directFilter := map[string]any{
		"path":      []any{"title"},
		"operator":  "Equal",
		"valueText": "Inception",
	}

	cases := []struct {
		name              string
		principal         *models.Principal
		namespacesEnabled bool
		args              QueryHybridArgs
		wantErrSubstr     string
		wantClassName     string
	}{
		{
			name:              "namespaced principal, short name resolves into traverser params",
			principal:         &models.Principal{Namespace: "customer1"},
			namespacesEnabled: true,
			args:              QueryHybridArgs{CollectionName: "Movies", Query: "x"},
			wantClassName:     "customer1:Movies",
		},
		{
			name:              "namespaced principal, alias resolves to qualified target",
			principal:         &models.Principal{Namespace: "customer1"},
			namespacesEnabled: true,
			args:              QueryHybridArgs{CollectionName: "Films", Query: "x"},
			wantClassName:     "customer1:Movies",
		},
		{
			name:              "namespaced principal, own-namespace qualified is rejected",
			principal:         &models.Principal{Namespace: "customer1"},
			namespacesEnabled: true,
			args:              QueryHybridArgs{CollectionName: "customer1:Movies", Query: "x"},
			wantErrSubstr:     "is not a valid class name",
		},
		{
			name:              "global principal, qualified name passes through",
			principal:         &models.Principal{},
			namespacesEnabled: true,
			args:              QueryHybridArgs{CollectionName: "customer1:Movies", Query: "x"},
			wantClassName:     "customer1:Movies",
		},
		{
			name:              "namespaces disabled, name flows through untouched",
			principal:         nil,
			namespacesEnabled: false,
			args:              QueryHybridArgs{CollectionName: "Global", Query: "x"},
			wantClassName:     "Global",
		},
		{
			name:              "namespacesEnabled accepts reference-path filter (inner class qualified)",
			principal:         &models.Principal{Namespace: "customer1"},
			namespacesEnabled: true,
			args:              QueryHybridArgs{CollectionName: "Movies", Query: "x", Filters: refPathFilter},
			wantClassName:     "customer1:Movies",
		},
		{
			name:              "namespacesEnabled accepts direct-property filter",
			principal:         &models.Principal{Namespace: "customer1"},
			namespacesEnabled: true,
			args:              QueryHybridArgs{CollectionName: "Movies", Query: "x", Filters: directFilter},
			wantClassName:     "customer1:Movies",
		},
		{
			name:              "namespacesEnabled=false still accepts reference-path filter",
			principal:         nil,
			namespacesEnabled: false,
			args:              QueryHybridArgs{CollectionName: "Movies", Query: "x", Filters: refPathFilter},
			wantClassName:     "Movies",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, trav := newSearcher(t, tc.principal, tc.namespacesEnabled, aliases)
			_, err := s.Hybrid(context.Background(), bearerReq(), tc.args)

			if tc.wantErrSubstr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrSubstr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantClassName, trav.gotParams.ClassName)
		})
	}
}

// stubTraverser returns a fixed result set, used to drive the response-strip
// path that recordingTraverser (empty results) doesn't exercise.
type stubTraverser struct{ results []any }

func (s *stubTraverser) GetClass(ctx context.Context, principal *models.Principal, params dto.GetParams) ([]any, error) {
	return s.results, nil
}

func newSearcherWithResults(t *testing.T, principal *models.Principal, results []any) *WeaviateSearcher {
	t.Helper()
	composer := func(token string, _ []string) (*models.Principal, error) { return principal, nil }
	authHandler := auth.NewAuth(false, composer, &authorization.DummyAuthorizer{}, nil)
	logger, _ := test.NewNullLogger()
	return NewWeaviateSearcher(authHandler, &stubTraverser{results: results},
		stubSchemaReader{}, stubSchemaManager{}, true, logger)
}

// TestHybrid_NestedRefClassStripped pins the NS strip on nested
// LocalRef.Class values in MCP hybrid responses.
func TestHybrid_NestedRefClassStripped(t *testing.T) {
	mkResults := func() []any {
		return []any{
			map[string]any{
				"title": "Z",
				"hasAnimals": []any{
					search.LocalRef{Class: "customer1:Animal", Fields: map[string]any{"name": "tigger"}},
					search.LocalRef{Class: "customer2:Animal", Fields: map[string]any{"name": "foreign"}},
					// Deeply nested: a ref inside the Fields of another ref.
					search.LocalRef{
						Class: "customer1:Animal",
						Fields: map[string]any{
							"hasHabitat": []any{
								search.LocalRef{Class: "customer1:Habitat", Fields: map[string]any{"name": "savanna"}},
							},
						},
					},
				},
			},
		}
	}

	type wantClass struct {
		top  string
		mid  string
		deep string
	}
	cases := []struct {
		name      string
		principal *models.Principal
		want      wantClass
	}{
		{
			name:      "namespaced caller: own NS stripped, foreign preserved, recursive",
			principal: &models.Principal{Namespace: "customer1"},
			want:      wantClass{top: "Animal", mid: "customer2:Animal", deep: "Habitat"},
		},
		{
			name:      "global principal: qualified class preserved",
			principal: &models.Principal{},
			want:      wantClass{top: "customer1:Animal", mid: "customer2:Animal", deep: "customer1:Habitat"},
		},
		{
			name:      "IsGlobalOperator with own-NS set still skips strip",
			principal: &models.Principal{IsGlobalOperator: true, Namespace: "customer1"},
			want:      wantClass{top: "customer1:Animal", mid: "customer2:Animal", deep: "customer1:Habitat"},
		},
		{
			name:      "nil principal: passthrough (NS-disabled)",
			principal: nil,
			want:      wantClass{top: "customer1:Animal", mid: "customer2:Animal", deep: "customer1:Habitat"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newSearcherWithResults(t, tc.principal, mkResults())
			args := QueryHybridArgs{CollectionName: "customer1:Zoo", Query: "x"}
			if tc.principal != nil && tc.principal.Namespace != "" {
				args.CollectionName = "Zoo"
			}
			resp, err := s.Hybrid(context.Background(), bearerReq(), args)
			require.NoError(t, err)
			require.Len(t, resp.Results, 1)

			top, ok := resp.Results[0].(map[string]any)
			require.True(t, ok)
			refs, ok := top["hasAnimals"].([]any)
			require.True(t, ok)
			require.Len(t, refs, 3)

			assert.Equal(t, tc.want.top, refs[0].(search.LocalRef).Class)
			assert.Equal(t, tc.want.mid, refs[1].(search.LocalRef).Class)

			deepRef := refs[2].(search.LocalRef)
			deepInner, ok := deepRef.Fields["hasHabitat"].([]any)
			require.True(t, ok)
			require.Len(t, deepInner, 1)
			assert.Equal(t, tc.want.deep, deepInner[0].(search.LocalRef).Class)
		})
	}
}

// TestHybrid_DefaultSelectProperties pins the default property selection: with
// no return_properties, the handler resolves all non-ref, non-blob properties
// (mirroring gRPC). An empty selection made the vector leg return bare IDs.
func TestHybrid_DefaultSelectProperties(t *testing.T) {
	class := &models.Class{
		Class: "Things",
		Properties: []*models.Property{
			{Name: "title", DataType: []string{"text"}},
			{Name: "image", DataType: []string{"blob"}},
			{Name: "hasOwner", DataType: []string{"Owner"}},
			{Name: "body", DataType: []string{"text"}},
		},
	}
	reader := stubSchemaReader{classes: map[string]*models.Class{"Things": class}}

	newSearcherWithSchema := func(t *testing.T) (*WeaviateSearcher, *recordingTraverser) {
		t.Helper()
		composer := func(token string, _ []string) (*models.Principal, error) { return &models.Principal{}, nil }
		authHandler := auth.NewAuth(false, composer, &authorization.DummyAuthorizer{}, nil)
		trav := &recordingTraverser{}
		logger, _ := test.NewNullLogger()
		return NewWeaviateSearcher(authHandler, trav, reader,
			stubSchemaManager{}, false, logger), trav
	}

	t.Run("no return_properties resolves all non-ref non-blob properties", func(t *testing.T) {
		s, trav := newSearcherWithSchema(t)
		_, err := s.Hybrid(context.Background(), bearerReq(), QueryHybridArgs{
			CollectionName: "Things", Query: "x",
		})
		require.NoError(t, err)
		require.Equal(t, search.SelectProperties{
			{Name: "title", IsPrimitive: true},
			{Name: "body", IsPrimitive: true},
		}, trav.gotParams.Properties)
	})

	t.Run("explicit return_properties pass through unchanged", func(t *testing.T) {
		s, trav := newSearcherWithSchema(t)
		_, err := s.Hybrid(context.Background(), bearerReq(), QueryHybridArgs{
			CollectionName: "Things", Query: "x", ReturnProperties: []string{"title"},
		})
		require.NoError(t, err)
		require.Equal(t, search.SelectProperties{
			{Name: "title", IsPrimitive: true},
		}, trav.gotParams.Properties)
	})

	t.Run("unknown class falls back to nil selection", func(t *testing.T) {
		s, trav := newSearcherWithSchema(t)
		_, err := s.Hybrid(context.Background(), bearerReq(), QueryHybridArgs{
			CollectionName: "Missing", Query: "x",
		})
		require.NoError(t, err)
		require.Nil(t, trav.gotParams.Properties)
	})
}

// Defensive canary: the JSON response a customer1 caller sees must not
// contain "customer1:" anywhere — catches any future carrier beyond LocalRef.
func TestHybrid_ResponseHasNoOwnNamespaceLeak(t *testing.T) {
	const uuid = "11111111-2222-3333-4444-555555555555"
	results := []any{
		map[string]any{
			"title": "Zoo",
			"hasAnimals": []any{
				search.LocalRef{
					Class:  "customer1:Animal",
					Fields: map[string]any{"name": "tigger", "id": uuid},
				},
				search.LocalRef{
					Class: "customer1:Animal",
					Fields: map[string]any{
						"hasHabitat": []any{
							search.LocalRef{Class: "customer1:Habitat", Fields: map[string]any{"name": "savanna"}},
						},
					},
				},
			},
			"_additional": map[string]any{"id": uuid, "distance": 0.42},
		},
	}

	principal := &models.Principal{Namespace: "customer1"}
	s := newSearcherWithResults(t, principal, results)
	resp, err := s.Hybrid(context.Background(), bearerReq(), QueryHybridArgs{
		CollectionName: "Zoo", Query: "x",
	})
	require.NoError(t, err)

	blob, err := json.Marshal(resp)
	require.NoError(t, err)
	assert.NotContains(t, string(blob), "customer1:",
		"namespaced response must not echo the caller's own \"<ns>:\" anywhere: %s", string(blob))
}
