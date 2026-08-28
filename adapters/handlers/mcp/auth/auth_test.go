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

package auth

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	usecasesNamespaces "github.com/weaviate/weaviate/usecases/namespaces"
)

const qualifiedCollection = "alpha:Movies"

// TestAuthorizeCollectionDataRequiresActiveNamespace pins that the collection-data
// check authorizes through AuthorizeAndRequireActiveNamespace and hands it the
// collection the resources were built from, for both resource shapes.
func TestAuthorizeCollectionDataRequiresActiveNamespace(t *testing.T) {
	tests := []struct {
		name          string
		tenant        string
		wantResources []string
	}{
		{
			name:          "without a tenant the collection's data is authorized",
			wantResources: authorization.CollectionsData(qualifiedCollection),
		},
		{
			name:          "with a tenant the shard's data is authorized",
			tenant:        "tenantA",
			wantResources: authorization.ShardsData(qualifiedCollection, "tenantA"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			principal := &models.Principal{Username: "someone"}
			authorizer := mocks.NewMockAuthorizer()
			a := NewAuth(false, nil, authorizer, nil)

			require.NoError(t, a.AuthorizeCollectionData(context.Background(), principal,
				authorization.READ, qualifiedCollection, tt.tenant))

			require.Equal(t, []mocks.AuthZReq{{
				Principal: principal,
				Verb:      authorization.READ,
				Resources: tt.wantResources,
				Method:    mocks.MethodAuthorizeAndRequireActiveNamespace,
				Class:     qualifiedCollection,
			}}, authorizer.Calls())
		})
	}
}

// TestAuthorizeCollectionDataReturnsTheNamespaceRefusal pins that the refusal
// comes back unwrapped rather than as a Forbidden. observeAuthzFailure matches
// only Forbidden and Unauthenticated, so a namespace refusal records no metric.
// One sentinel stands for every state: this check returns what it is handed.
func TestAuthorizeCollectionDataReturnsTheNamespaceRefusal(t *testing.T) {
	sentinel := usecasesNamespaces.ErrNamespaceSuspended
	authorizer := mocks.NewMockAuthorizer()
	authorizer.SetErr(sentinel)
	a := NewAuth(false, nil, authorizer, nil)

	err := a.AuthorizeCollectionData(context.Background(), &models.Principal{Username: "someone"},
		authorization.READ, qualifiedCollection, "")

	require.ErrorIs(t, err, sentinel)
	require.Len(t, authorizer.Calls(), 1)
	require.Equal(t, mocks.MethodAuthorizeAndRequireActiveNamespace, authorizer.Calls()[0].Method)
}

// TestAuthorizeCollectionDataDeniesBeforeReadingTheNamespace pins the deny side of
// the gate. The denial comes back as the Forbidden the caller was already given,
// carrying no namespace wording, so the check cannot become a state probe.
func TestAuthorizeCollectionDataDeniesBeforeReadingTheNamespace(t *testing.T) {
	principal := &models.Principal{Username: "someone"}
	authorizer := mocks.NewMockAuthorizer()
	authorizer.SetErr(authzerrors.NewForbidden(principal, authorization.READ, qualifiedCollection))
	a := NewAuth(false, nil, authorizer, nil)

	err := a.AuthorizeCollectionData(context.Background(), principal,
		authorization.READ, qualifiedCollection, "")

	var forbidden authzerrors.Forbidden
	require.ErrorAs(t, err, &forbidden)
	require.NotContains(t, err.Error(), "namespace")
}
