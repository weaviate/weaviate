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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	autherrs "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	usecasesNamespaces "github.com/weaviate/weaviate/usecases/namespaces"
)

// namespacedTestHandler is newTestHandler with namespaces on and two qualified
// collections in the schema, so a row can assert the class the gate received.
// Movie's reference property points at the collection in the other namespace,
// which is what a request touching two namespaces at once needs.
func namespacedTestHandler(t *testing.T) *testDeps {
	t.Helper()
	return namespacedTestHandlerWithAliases(t, nil)
}

// namespacedTestHandlerWithAliases is namespacedTestHandler with aliases, which
// the schema takes at construction like every other seeded fact.
func namespacedTestHandlerWithAliases(t *testing.T, aliases map[string]string) *testDeps {
	t.Helper()

	movie := movieClass()
	movie.Class = qualifiedMovie
	for _, prop := range movie.Properties {
		if prop.Name == "hasAuthor" {
			prop.DataType = []string{qualifiedAuthor}
		}
	}
	// A second reference inside Movie's own namespace, because a where-filter path
	// whose inner segment names another namespace dies at QualifyRefTarget before
	// the gated getter ever sees it.
	movie.Properties = append(movie.Properties,
		&models.Property{Name: "hasEditor", DataType: []string{qualifiedEditor}})

	author := authorClass()
	author.Class = qualifiedAuthor
	// Author's own reference points at a third namespace, so a row can reach a
	// collection the request names nowhere and only the second hop selects.
	for _, prop := range author.Properties {
		if prop.Name == "worksFor" {
			prop.DataType = []string{qualifiedStudio}
		}
	}

	editor := authorClass()
	editor.Class = qualifiedEditor
	studio := authorClass()
	studio.Class = qualifiedStudio

	deps := newTestHandlerSeeded(t, []*models.Class{movie, author, editor, studio}, aliases)
	deps.handler.namespacesEnabled = true
	return deps
}

const (
	qualifiedMovie  = "alpha:Movie"
	qualifiedAuthor = "beta:Author"
	qualifiedEditor = "alpha:Editor"
	qualifiedStudio = "gamma:Studio"
)

// searchEntryPoints runs each endpoint family against one collection, so a row
// covers search and aggregate without restating the request bodies.
var searchEntryPoints = map[string]func(*testing.T, *testDeps, string) *APIError{
	"search": func(t *testing.T, deps *testDeps, collection string) *APIError {
		_, apiErr := doNearText(t, deps, nil, collection, `{"query":["space"]}`)
		return apiErr
	},
	"aggregate": func(t *testing.T, deps *testDeps, collection string) *APIError {
		_, apiErr := doAggregate(t, deps, nil, collection, `{}`)
		return apiErr
	},
}

// TestSearchAuthorizesThroughTheNamespaceGate pins that both endpoint families
// authorize through AuthorizeAndRequireActiveNamespace and hand it the resolved
// class name, which an assertion on the returned error alone cannot tell from
// plain Authorize.
func TestSearchAuthorizesThroughTheNamespaceGate(t *testing.T) {
	for entryPoint, run := range searchEntryPoints {
		t.Run(entryPoint, func(t *testing.T) {
			deps := namespacedTestHandler(t)
			deps.searcher.aggregateRes = ungroupedCount(1)

			require.Nil(t, run(t, deps, qualifiedMovie))

			require.Equal(t, []mocks.AuthZReq{{
				Principal: nil,
				Verb:      authorization.READ,
				Resources: authorization.CollectionsData(qualifiedMovie),
				Method:    mocks.MethodAuthorizeAndRequireActiveNamespace,
				Class:     qualifiedMovie,
			}}, deps.authorizer.Calls())
		})
	}
}

// TestSearchNamespaceRefusalReachesTheCaller pins that a refusal survives the
// handler stack unwrapped. It renders as a 500 rather than a 403, because the
// sentinel is no Forbidden and statusFromError has no arm for it. One sentinel
// stands for every state: the stack carries the error without reading it.
func TestSearchNamespaceRefusalReachesTheCaller(t *testing.T) {
	sentinel := usecasesNamespaces.ErrNamespaceSuspended

	for entryPoint, run := range searchEntryPoints {
		t.Run(entryPoint, func(t *testing.T) {
			deps := namespacedTestHandler(t)
			deps.authorizer.SetErr(sentinel)

			apiErr := run(t, deps, qualifiedMovie)

			require.NotNil(t, apiErr)
			require.ErrorIs(t, apiErr.Err, sentinel)
			assert.Equal(t, http.StatusInternalServerError, apiErr.Status)
			require.Len(t, deps.authorizer.Calls(), 1)
			assert.Equal(t, mocks.MethodAuthorizeAndRequireActiveNamespace,
				deps.authorizer.Calls()[0].Method)
		})
	}
}

// TestSearchRefusesWhenAReferencedCollectionIsSuspended pins the partial case. The
// collection the caller named is served, a collection the request also reaches is
// not, and the request as a whole is refused. The closure gates every class a
// selection or filter names, not only the one the request is addressed to.
func TestSearchRefusesWhenAReferencedCollectionIsSuspended(t *testing.T) {
	tests := []struct {
		name       string
		refused    string
		body       string
		wantStatus int
		wantGated  []string
	}{
		{
			name:       "a reference selection",
			refused:    qualifiedAuthor,
			body:       `{"query":["space"],"returnReferences":[{"linkOn":"hasAuthor"}]}`,
			wantStatus: http.StatusInternalServerError,
			wantGated:  []string{qualifiedMovie, qualifiedAuthor},
		},
		{
			// The gate follows nesting. gamma:Studio is named nowhere in the
			// request and only the second hop reaches it.
			name:       "a nested reference selection",
			refused:    qualifiedStudio,
			body:       `{"query":["space"],"returnReferences":[{"linkOn":"hasAuthor","returnReferences":[{"linkOn":"worksFor"}]}]}`,
			wantStatus: http.StatusInternalServerError,
			wantGated:  []string{qualifiedMovie, qualifiedAuthor, qualifiedStudio},
		},
		{
			// ValidateFilters answers through parseWhere, which rewrites anything it
			// returns into "invalid 'where' filter", so this shape tells the caller
			// its filter is malformed. Pinned as it is until the refusal gets a
			// status of its own.
			name:       "a where filter across a reference",
			refused:    qualifiedEditor,
			body:       `{"query":["space"],"where":{"path":["hasEditor","Editor","name"],"operator":"Equal","valueText":"x"}}`,
			wantStatus: http.StatusBadRequest,
			wantGated:  []string{qualifiedMovie, qualifiedEditor},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deps := namespacedTestHandler(t)
			authz := &denyCollections{
				denied:  map[string]bool{tt.refused: true},
				refusal: usecasesNamespaces.ErrNamespaceSuspended,
			}
			deps.handler.authorizer = authz

			_, apiErr := doNearText(t, deps, nil, qualifiedMovie, tt.body)

			require.NotNil(t, apiErr)
			assert.Equal(t, tt.wantStatus, apiErr.Status)
			assert.Contains(t, apiErr.Error(), "namespace is suspended")

			gated := make([]string, 0, len(authz.Calls()))
			for _, call := range authz.Calls() {
				gated = append(gated, call.Class)
			}
			require.Equal(t, tt.wantGated, gated,
				"the named collection is gated first, then the one the request also reaches")
		})
	}
}

// TestSearchDeniedAliasReAuthorizesWithoutTheGate pins that hideAliasTarget keeps
// plain Authorize. It re-authorizes only after the caller was denied the target,
// so a gate there would answer that denied caller with a namespace's state.
func TestSearchDeniedAliasReAuthorizesWithoutTheGate(t *testing.T) {
	principal := &models.Principal{Username: "someone"}
	deps := namespacedTestHandlerWithAliases(t, map[string]string{"Films": qualifiedMovie})
	deps.authorizer.SetErr(autherrs.NewForbidden(principal, "read", "collections/Films"))

	_, apiErr := doNearText(t, deps, principal, "Films", `{"query":["space"]}`)

	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusForbidden, apiErr.Status)
	require.Len(t, deps.authorizer.Calls(), 2, "the denial must reach the re-authorization")
	assert.Equal(t, mocks.MethodAuthorize, deps.authorizer.Calls()[1].Method)
}

// TestSearchHidesTheAliasTargetOfARefusedNamespace pins that hideAliasTarget stays
// on plain Authorize. The alias oracle it closes is reached only for a Forbidden,
// so a namespace refusal passes it untouched and names no target collection.
func TestSearchHidesTheAliasTargetOfARefusedNamespace(t *testing.T) {
	deps := namespacedTestHandlerWithAliases(t, map[string]string{"Films": qualifiedMovie})
	deps.authorizer.SetErr(usecasesNamespaces.ErrNamespaceSuspended)

	_, apiErr := doNearText(t, deps, &models.Principal{Username: "someone"}, "Films", `{"query":["space"]}`)

	require.NotNil(t, apiErr)
	require.ErrorIs(t, apiErr.Err, usecasesNamespaces.ErrNamespaceSuspended)
	assert.NotContains(t, apiErr.Error(), "Movie", "a refusal must not name the alias target")
	require.Len(t, deps.authorizer.Calls(), 1, "the re-authorization must not run for a namespace refusal")
}

func TestClassGetterWithAuthzMemoizesPerClass(t *testing.T) {
	deps := namespacedTestHandler(t)
	principal := &models.Principal{}
	getClass := deps.handler.classGetterWithAuthz(context.Background(), principal, "")

	for _, name := range []string{qualifiedMovie, qualifiedAuthor, qualifiedMovie, qualifiedAuthor} {
		_, err := getClass(name)
		require.NoError(t, err)
	}

	require.Equal(t, []mocks.AuthZReq{
		{
			Principal: principal, Verb: authorization.READ, Resources: authorization.CollectionsData(qualifiedMovie),
			Method: mocks.MethodAuthorizeAndRequireActiveNamespace, Class: qualifiedMovie,
		},
		{
			Principal: principal, Verb: authorization.READ, Resources: authorization.CollectionsData(qualifiedAuthor),
			Method: mocks.MethodAuthorizeAndRequireActiveNamespace, Class: qualifiedAuthor,
		},
	}, deps.authorizer.Calls())
}

func TestClassGetterWithAuthzDoesNotMemoizeDenied(t *testing.T) {
	deps := namespacedTestHandler(t)
	deps.authorizer.SetErr(usecasesNamespaces.ErrNamespaceSuspended)
	principal := &models.Principal{}
	getClass := deps.handler.classGetterWithAuthz(context.Background(), principal, "")

	for range 2 {
		_, err := getClass(qualifiedMovie)
		require.ErrorIs(t, err, usecasesNamespaces.ErrNamespaceSuspended)
	}

	gated := mocks.AuthZReq{
		Principal: principal, Verb: authorization.READ, Resources: authorization.CollectionsData(qualifiedMovie),
		Method: mocks.MethodAuthorizeAndRequireActiveNamespace, Class: qualifiedMovie,
	}
	require.Equal(t, []mocks.AuthZReq{gated, gated}, deps.authorizer.Calls())
}
