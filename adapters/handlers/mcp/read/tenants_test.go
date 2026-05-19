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
	"strings"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/mcp/auth"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// recordingAuthorizer captures the resources and verbs passed to Authorize.
// Always allows access. Used to prove that GetTenants does not gate on
// CollectionsData (which would be the wrong permission) — only the
// tool-level mcp domain check.
type recordingAuthorizer struct {
	gotResources []string
}

func (r *recordingAuthorizer) Authorize(ctx context.Context, principal *models.Principal, verb string, resources ...string) error {
	r.gotResources = append(r.gotResources, resources...)
	return nil
}

func (r *recordingAuthorizer) AuthorizeSilent(ctx context.Context, principal *models.Principal, verb string, resources ...string) error {
	return r.Authorize(ctx, principal, verb, resources...)
}

func (r *recordingAuthorizer) FilterAuthorizedResources(ctx context.Context, principal *models.Principal, verb string, resources ...string) ([]string, error) {
	return resources, nil
}

// recordingTenantsReader captures the class name passed to GetConsistentTenants
// so the test can assert the user-supplied name flows through unmodified.
type recordingTenantsReader struct {
	gotClass string
}

func (s *recordingTenantsReader) GetConsistentSchema(ctx context.Context, principal *models.Principal, consistency bool) (schema.Schema, error) {
	return schema.Schema{}, nil
}

func (s *recordingTenantsReader) GetConsistentTenants(ctx context.Context, principal *models.Principal, class string, consistency bool, tenants []string) ([]*models.Tenant, error) {
	s.gotClass = class
	return nil, nil
}

// TestGetTenants_PassThrough proves the MCP handler delegates to
// GetConsistentTenants without rewriting the class name and without an
// extra CollectionsData (read_data) authz gate. Namespace qualification,
// alias resolution, and per-tenant ShardsMetadata RBAC happen inside
// GetConsistentTenants; the acceptance test covers the end-to-end behaviour.
func TestGetTenants_PassThrough(t *testing.T) {
	cases := []struct {
		name      string
		principal *models.Principal
		args      GetTenantsArgs
	}{
		{
			name:      "namespaced principal, short input",
			principal: &models.Principal{Namespace: "customer1"},
			args:      GetTenantsArgs{CollectionName: "Movies"},
		},
		{
			name:      "namespaced principal, alias input",
			principal: &models.Principal{Namespace: "customer1"},
			args:      GetTenantsArgs{CollectionName: "Films"},
		},
		{
			name:      "global principal, qualified input",
			principal: &models.Principal{},
			args:      GetTenantsArgs{CollectionName: "customer1:Movies"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			composer := func(token string, _ []string) (*models.Principal, error) {
				return tc.principal, nil
			}
			authz := &recordingAuthorizer{}
			authHandler := auth.NewAuth(false, composer, authz)
			reader := &recordingTenantsReader{}
			logger, _ := test.NewNullLogger()
			r := NewWeaviateReader(
				authHandler,
				reader,
				stubSchemaManager{},
				true,
				stubObjectsManager{},
				logger,
			)

			_, err := r.GetTenants(context.Background(), bearerReq(), tc.args)
			require.NoError(t, err)

			assert.Equal(t, tc.args.CollectionName, reader.gotClass,
				"the raw user-supplied class name must flow through to GetConsistentTenants")

			// Only the mcp-domain check should run; no CollectionsData
			// (data/collections/.../shards/.../objects/...) resource.
			for _, res := range authz.gotResources {
				assert.False(t, strings.Contains(res, "data/collections/"),
					"GetTenants must not gate on CollectionsData; got %q", res)
			}
		})
	}
}
