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
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
)

func TestAuthorizeCollectionDataRequiresActiveNamespace(t *testing.T) {
	const qualifiedCollection = "alpha:Movies"
	principal := &models.Principal{Username: "someone"}
	authorizer := mocks.NewMockAuthorizer()
	a := NewAuth(false, nil, authorizer, nil)

	require.NoError(t, a.AuthorizeCollectionData(context.Background(), principal,
		authorization.READ, qualifiedCollection, ""))

	require.Equal(t, []mocks.AuthZReq{{
		Principal: principal,
		Verb:      authorization.READ,
		Resources: authorization.CollectionsData(qualifiedCollection),
		Method:    mocks.MethodAuthorizeAndRequireActiveNamespace,
		Class:     qualifiedCollection,
	}}, authorizer.Calls())
}
