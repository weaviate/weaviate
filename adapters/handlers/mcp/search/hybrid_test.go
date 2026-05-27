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
	"net/http"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/mcp/auth"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"
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
		stubSchemaManager{aliases: aliases},
		namespacesEnabled,
		logger,
	), trav
}

func bearerReq() mcp.CallToolRequest {
	return mcp.CallToolRequest{Header: http.Header{"Authorization": []string{"Bearer dummy"}}}
}

// TestHybrid_NamespaceResolution covers what the MCP search handler must do
// once namespacing is wired in:
//
//   - the resolved class name (qualified, alias-resolved) flows into the
//     traverser params
//   - invalid namespace prefixes from a namespaced principal are rejected
//   - filterext.Parse picks up the namespacesEnabled flag (reference-path
//     filters have their inner class qualified on NS-enabled clusters,
//     pass-through otherwise)
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
