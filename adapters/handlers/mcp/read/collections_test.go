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

package read

import (
	"context"
	"net/http"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/mcp/auth"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/objects"
)

// stubSchemaManager satisfies namespacing.SchemaManager. ResolveAlias returns
// whatever was placed in aliases, "" otherwise.
type stubSchemaManager struct {
	aliases map[string]string
}

func (s stubSchemaManager) ResolveAlias(alias string) string {
	return s.aliases[alias]
}

// stubSchemaReader returns a fixed schema and is the only schemaReader hit by
// GetCollectionConfig. GetConsistentTenants is unused by these tests.
type stubSchemaReader struct {
	classes []*models.Class
}

func (s stubSchemaReader) GetConsistentSchema(ctx context.Context, principal *models.Principal, consistency bool) (schema.Schema, error) {
	return schema.Schema{Objects: &models.Schema{Classes: s.classes}}, nil
}

func (s stubSchemaReader) GetConsistentTenants(ctx context.Context, principal *models.Principal, class string, consistency bool, tenants []string) ([]*models.Tenant, error) {
	return nil, nil
}

// stubObjectsManager is a no-op stand-in; GetCollectionConfig never reaches it.
type stubObjectsManager struct{}

func (stubObjectsManager) GetObject(context.Context, *models.Principal, string, strfmt.UUID, additional.Properties, *additional.ReplicationProperties, string) (*models.Object, error) {
	return nil, nil
}

func (stubObjectsManager) GetObjects(context.Context, *models.Principal, *int64, *int64, *string, *string, *string, additional.Properties, string) ([]*models.Object, error) {
	return nil, nil
}

func (stubObjectsManager) Query(context.Context, *models.Principal, *objects.QueryParams) ([]*models.Object, *objects.Error) {
	return nil, nil
}

func newReader(t *testing.T, principal *models.Principal, namespacesEnabled bool, classes []*models.Class, aliases map[string]string) *WeaviateReader {
	t.Helper()
	composer := func(token string, _ []string) (*models.Principal, error) {
		return principal, nil
	}
	authHandler := auth.NewAuth(false, composer, &authorization.DummyAuthorizer{})
	logger, _ := test.NewNullLogger()
	return NewWeaviateReader(
		authHandler,
		stubSchemaReader{classes: classes},
		stubSchemaManager{aliases: aliases},
		namespacesEnabled,
		stubObjectsManager{},
		logger,
	)
}

func bearerReq() mcp.CallToolRequest {
	return mcp.CallToolRequest{Header: http.Header{"Authorization": []string{"Bearer dummy"}}}
}

// TestGetCollectionConfig_NamespaceResolution covers the four resolution
// branches that GetCollectionConfig now exercises:
//
//   - namespaced principal + short name resolves to the qualified class
//   - namespaced principal + qualified name fails namespace-prefix validation
//   - global principal + qualified name passes through unchanged
//   - global principal + short name does not resolve to a namespaced class
//
// Alias resolution and the list-all branch are covered too.
func TestGetCollectionConfig_NamespaceResolution(t *testing.T) {
	classes := []*models.Class{
		{Class: "customer1:Movies"},
		{Class: "customer2:Movies"},
		{Class: "Global"},
	}
	aliases := map[string]string{
		"customer1:Films": "customer1:Movies",
	}

	type wantResp struct {
		errSubstr string
		classes   []string
	}

	cases := []struct {
		name              string
		principal         *models.Principal
		namespacesEnabled bool
		args              GetCollectionConfigArgs
		want              wantResp
	}{
		{
			name:              "namespaced principal, short name resolves",
			principal:         &models.Principal{Namespace: "customer1"},
			namespacesEnabled: true,
			args:              GetCollectionConfigArgs{CollectionName: "Movies"},
			want:              wantResp{classes: []string{"customer1:Movies"}},
		},
		{
			name:              "namespaced principal, own-namespace qualified is rejected",
			principal:         &models.Principal{Namespace: "customer1"},
			namespacesEnabled: true,
			args:              GetCollectionConfigArgs{CollectionName: "customer1:Movies"},
			want:              wantResp{errSubstr: "is not a valid class name"},
		},
		{
			name:              "namespaced principal, foreign-namespace qualified is rejected",
			principal:         &models.Principal{Namespace: "customer1"},
			namespacesEnabled: true,
			args:              GetCollectionConfigArgs{CollectionName: "customer2:Movies"},
			want:              wantResp{errSubstr: "is not a valid class name"},
		},
		{
			name:              "global principal, qualified name passes through",
			principal:         &models.Principal{},
			namespacesEnabled: true,
			args:              GetCollectionConfigArgs{CollectionName: "customer1:Movies"},
			want:              wantResp{classes: []string{"customer1:Movies"}},
		},
		{
			name:              "global principal, short name misses namespaced class",
			principal:         &models.Principal{},
			namespacesEnabled: true,
			args:              GetCollectionConfigArgs{CollectionName: "Movies"},
			want:              wantResp{errSubstr: "not found"},
		},
		{
			name:              "namespaced principal, alias resolves to qualified target",
			principal:         &models.Principal{Namespace: "customer1"},
			namespacesEnabled: true,
			args:              GetCollectionConfigArgs{CollectionName: "Films"},
			want:              wantResp{classes: []string{"customer1:Movies"}},
		},
		{
			name:              "list-all branch skips resolution",
			principal:         &models.Principal{Namespace: "customer1"},
			namespacesEnabled: true,
			args:              GetCollectionConfigArgs{},
			want:              wantResp{classes: []string{"customer1:Movies", "customer2:Movies", "Global"}},
		},
		{
			name:              "namespaces disabled, short name flows through",
			principal:         nil,
			namespacesEnabled: false,
			args:              GetCollectionConfigArgs{CollectionName: "Global"},
			want:              wantResp{classes: []string{"Global"}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newReader(t, tc.principal, tc.namespacesEnabled, classes, aliases)
			resp, err := r.GetCollectionConfig(context.Background(), bearerReq(), tc.args)

			if tc.want.errSubstr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.want.errSubstr)
				return
			}
			require.NoError(t, err)
			got := make([]string, len(resp.Collections))
			for i, c := range resp.Collections {
				got[i] = c.Class
			}
			assert.ElementsMatch(t, tc.want.classes, got)
		})
	}
}
