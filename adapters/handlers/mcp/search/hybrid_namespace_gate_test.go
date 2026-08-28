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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	usecasesNamespaces "github.com/weaviate/weaviate/usecases/namespaces"
)

// refuseCollectionData admits the tool-level Authorize and refuses the
// collection-data gate, which is the shape a non-active namespace produces.
type refuseCollectionData struct {
	authorization.DummyAuthorizer
	err error
}

func (a *refuseCollectionData) AuthorizeAndRequireActiveNamespace(ctx context.Context,
	principal *models.Principal, verb string, class string, resources ...string,
) error {
	return a.err
}

// TestHybridReturnsTheNamespaceRefusal pins that the refusal survives the
// handler's StripErrForPrincipal defer unwrapped. That defer rewrites the message
// and keeps Unwrap, so errors.Is still reaches the sentinel. One sentinel stands
// for every state: the defer strips the message whatever the state was.
func TestHybridReturnsTheNamespaceRefusal(t *testing.T) {
	sentinel := usecasesNamespaces.ErrNamespaceSuspended
	s, trav := newSearcherWithAuthorizer(t, &models.Principal{Username: "someone"}, true, nil,
		&refuseCollectionData{err: sentinel})

	_, err := s.Hybrid(context.Background(), bearerReq(),
		QueryHybridArgs{CollectionName: "alpha:Movies", Query: "x"})

	require.ErrorIs(t, err, sentinel)
	require.Empty(t, trav.gotParams.ClassName, "the refusal must land before the search runs")
}
