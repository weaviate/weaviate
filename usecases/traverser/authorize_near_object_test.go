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

package traverser

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/config"
)

// aliasSchemaGetter lets tests exercise beacons that name a collection alias.
type aliasSchemaGetter struct {
	*fakeSchemaGetter
	aliases map[string]string
}

func (a *aliasSchemaGetter) ResolveAlias(alias string) string {
	return a.aliases[alias]
}

const anchorID = strfmt.UUID("45000000-0000-0000-0000-000000000001")

func TestAuthorizeNearObjectBeacon(t *testing.T) {
	principal := &models.Principal{Username: "some-user"}

	tests := []struct {
		name       string
		nearObject *searchparams.NearObject
		className  string
		tenant     string
		aliases    map[string]string
		// expectResources is what the authorizer must be asked for;
		// empty means the check must not call the authorizer at all
		expectResources []string
		expectParseErr  bool
	}{
		{
			name:      "nil nearObject",
			className: "Books",
		},
		{
			name:       "id only stays in the searched collection",
			nearObject: &searchparams.NearObject{ID: anchorID.String()},
			className:  "Books",
		},
		{
			name:       "class-less beacon stays in the searched collection",
			nearObject: &searchparams.NearObject{Beacon: "weaviate://localhost/" + anchorID.String()},
			className:  "Books",
		},
		{
			name:       "same-collection beacon is covered by the query authorization",
			nearObject: &searchparams.NearObject{Beacon: "weaviate://localhost/Books/" + anchorID.String()},
			className:  "Books",
		},
		{
			name:       "same-collection beacon in different casing resolves to the same index",
			nearObject: &searchparams.NearObject{Beacon: "weaviate://localhost/bOoKs/" + anchorID.String()},
			className:  "Books",
		},
		{
			name:            "cross-collection beacon requires read on the target object",
			nearObject:      &searchparams.NearObject{Beacon: "weaviate://localhost/Papers/" + anchorID.String()},
			className:       "Books",
			expectResources: []string{authorization.Objects("Papers", "", anchorID)},
		},
		{
			name:            "cross-collection beacon in lowercase is authorized against the class name",
			nearObject:      &searchparams.NearObject{Beacon: "weaviate://localhost/papers/" + anchorID.String()},
			className:       "Books",
			expectResources: []string{authorization.Objects("Papers", "", anchorID)},
		},
		{
			name:            "cross-collection beacon carries the request tenant",
			nearObject:      &searchparams.NearObject{Beacon: "weaviate://localhost/Papers/" + anchorID.String()},
			className:       "Books",
			tenant:          "tenantA",
			expectResources: []string{authorization.Objects("Papers", "tenantA", anchorID)},
		},
		{
			name:            "cross-class beacon from Explore (no searched collection)",
			nearObject:      &searchparams.NearObject{Beacon: "weaviate://localhost/Papers/" + anchorID.String()},
			className:       "",
			expectResources: []string{authorization.Objects("Papers", "", anchorID)},
		},
		{
			name:       "beacon naming an alias of the searched collection",
			nearObject: &searchparams.NearObject{Beacon: "weaviate://localhost/BooksAlias/" + anchorID.String()},
			className:  "Books",
			aliases:    map[string]string{"BooksAlias": "Books"},
		},
		{
			name:       "search by alias with a beacon naming the class behind it",
			nearObject: &searchparams.NearObject{Beacon: "weaviate://localhost/Books/" + anchorID.String()},
			className:  "BooksAlias",
			aliases:    map[string]string{"BooksAlias": "Books"},
		},
		{
			name:            "beacon naming an alias of another collection",
			nearObject:      &searchparams.NearObject{Beacon: "weaviate://localhost/PapersAlias/" + anchorID.String()},
			className:       "Books",
			aliases:         map[string]string{"PapersAlias": "Papers"},
			expectResources: []string{authorization.Objects("Papers", "", anchorID)},
		},
		{
			name:           "malformed beacon errors",
			nearObject:     &searchparams.NearObject{Beacon: "not-a-beacon"},
			className:      "Books",
			expectParseErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			authorizer := mocks.NewMockAuthorizer()
			logger, _ := test.NewNullLogger()
			schemaGetter := &aliasSchemaGetter{
				fakeSchemaGetter: &fakeSchemaGetter{},
				aliases:          tc.aliases,
			}
			traverser := NewTraverser(&config.WeaviateConfig{}, logger, authorizer,
				&fakeVectorRepo{}, &fakeExplorer{}, schemaGetter, nil, nil, -1)

			err := traverser.authorizeNearObjectBeacon(context.Background(),
				principal, tc.nearObject, tc.className, tc.tenant)

			if tc.expectParseErr {
				require.Error(t, err)
				assert.Empty(t, authorizer.Calls())
				return
			}

			require.NoError(t, err)
			if len(tc.expectResources) == 0 {
				assert.Empty(t, authorizer.Calls())
				return
			}
			require.Len(t, authorizer.Calls(), 1)
			call := authorizer.Calls()[0]
			assert.Equal(t, principal, call.Principal)
			assert.Equal(t, authorization.READ, call.Verb)
			assert.Equal(t, tc.expectResources, call.Resources)
		})
	}

	t.Run("authorizer denial propagates", func(t *testing.T) {
		authorizer := mocks.NewMockAuthorizer()
		authorizer.SetErr(errors.New("forbidden"))
		logger, _ := test.NewNullLogger()
		traverser := NewTraverser(&config.WeaviateConfig{}, logger, authorizer,
			&fakeVectorRepo{}, &fakeExplorer{}, &fakeSchemaGetter{}, nil, nil, -1)

		err := traverser.authorizeNearObjectBeacon(context.Background(), principal,
			&searchparams.NearObject{Beacon: "weaviate://localhost/Papers/" + anchorID.String()},
			"Books", "")
		require.EqualError(t, err, "forbidden")
	})
}

func TestNearObjectBeaconAuthzWiring(t *testing.T) {
	principal := &models.Principal{Username: "some-user"}
	crossBeacon := "weaviate://localhost/Papers/" + anchorID.String()

	newDeniedTraverser := func() *Traverser {
		authorizer := mocks.NewMockAuthorizer()
		authorizer.SetErr(errors.New("forbidden"))
		logger, _ := test.NewNullLogger()
		return NewTraverser(&config.WeaviateConfig{}, logger, authorizer,
			&fakeVectorRepo{}, &fakeExplorer{}, &fakeSchemaGetter{}, nil, nil, -1)
	}

	t.Run("GetClass", func(t *testing.T) {
		_, err := newDeniedTraverser().GetClass(context.Background(), principal, dto.GetParams{
			ClassName:  "Books",
			NearObject: &searchparams.NearObject{Beacon: crossBeacon},
		})
		require.EqualError(t, err, "forbidden")
	})

	t.Run("Aggregate", func(t *testing.T) {
		_, err := newDeniedTraverser().Aggregate(context.Background(), principal, &aggregation.Params{
			ClassName:  "Books",
			NearObject: &searchparams.NearObject{Beacon: crossBeacon},
		})
		require.EqualError(t, err, "forbidden")
	})

	t.Run("Explore", func(t *testing.T) {
		_, err := newDeniedTraverser().Explore(context.Background(), principal, ExploreParams{
			NearObject: &searchparams.NearObject{Beacon: crossBeacon},
		})
		require.EqualError(t, err, "forbidden")
	})

	t.Run("GetClass with a same-collection beacon does not consult the authorizer", func(t *testing.T) {
		authorizer := mocks.NewMockAuthorizer()
		authorizer.SetErr(errors.New("forbidden"))
		logger, _ := test.NewNullLogger()
		traverser := NewTraverser(&config.WeaviateConfig{}, logger, authorizer,
			&fakeVectorRepo{}, &fakeExplorer{}, &fakeSchemaGetter{}, nil, nil, -1)

		_, err := traverser.GetClass(context.Background(), principal, dto.GetParams{
			ClassName:  "Books",
			NearObject: &searchparams.NearObject{Beacon: "weaviate://localhost/Books/" + anchorID.String()},
		})
		require.NoError(t, err)
		assert.Empty(t, authorizer.Calls())
	})
}
