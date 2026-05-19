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

// recordingAuthorizer captures the resources passed to Authorize so the test
// can prove that authz runs against the post-resolution (qualified) class
// name. Always allows access.
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
// so the test can prove resolution flowed through.
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

// TestGetTenants_NamespaceResolution proves that authz and the downstream
// schemaReader call both see the resolved (qualified) class name, not the
// raw user input. The pre-fix behaviour authorized against the unqualified
// name and was denied for narrowed namespaced principals on NS-enabled
// clusters; this test locks the fix in.
func TestGetTenants_NamespaceResolution(t *testing.T) {
	aliases := map[string]string{
		"customer1:Films": "customer1:Movies",
	}

	cases := []struct {
		name              string
		principal         *models.Principal
		namespacesEnabled bool
		args              GetTenantsArgs
		wantErrSubstr     string
		wantClass         string
	}{
		{
			name:              "namespaced principal, short name resolves",
			principal:         &models.Principal{Namespace: "customer1"},
			namespacesEnabled: true,
			args:              GetTenantsArgs{CollectionName: "Movies"},
			wantClass:         "customer1:Movies",
		},
		{
			name:              "namespaced principal, alias resolves to qualified target",
			principal:         &models.Principal{Namespace: "customer1"},
			namespacesEnabled: true,
			args:              GetTenantsArgs{CollectionName: "Films"},
			wantClass:         "customer1:Movies",
		},
		{
			name:              "namespaced principal, own-namespace qualified is rejected",
			principal:         &models.Principal{Namespace: "customer1"},
			namespacesEnabled: true,
			args:              GetTenantsArgs{CollectionName: "customer1:Movies"},
			wantErrSubstr:     "is not a valid class name",
		},
		{
			name:              "namespaced principal, foreign-namespace qualified is rejected",
			principal:         &models.Principal{Namespace: "customer1"},
			namespacesEnabled: true,
			args:              GetTenantsArgs{CollectionName: "customer2:Movies"},
			wantErrSubstr:     "is not a valid class name",
		},
		{
			name:              "global principal, qualified name passes through",
			principal:         &models.Principal{},
			namespacesEnabled: true,
			args:              GetTenantsArgs{CollectionName: "customer1:Movies"},
			wantClass:         "customer1:Movies",
		},
		{
			name:              "namespaces disabled, name flows through untouched",
			principal:         nil,
			namespacesEnabled: false,
			args:              GetTenantsArgs{CollectionName: "Global"},
			wantClass:         "Global",
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
				stubSchemaManager{aliases: aliases},
				tc.namespacesEnabled,
				stubObjectsManager{},
				logger,
			)

			_, err := r.GetTenants(context.Background(), bearerReq(), tc.args)

			if tc.wantErrSubstr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrSubstr)
				return
			}
			require.NoError(t, err)

			assert.Equal(t, tc.wantClass, reader.gotClass,
				"GetConsistentTenants must receive the resolved class name")

			// The collection-data Authorize call must see the resolved class
			// in some resource path. Search rather than indexing into a
			// specific call so the test doesn't bind to the exact verb/order.
			require.NotEmpty(t, authz.gotResources)
			found := false
			for _, res := range authz.gotResources {
				if strings.Contains(res, tc.wantClass) {
					found = true
					break
				}
			}
			assert.True(t, found,
				"authorizer must see the resolved class %q in some resource path; got %v",
				tc.wantClass, authz.gotResources)
		})
	}
}
