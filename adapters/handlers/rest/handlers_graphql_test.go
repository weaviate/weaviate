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

package rest

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libgraphql "github.com/weaviate/weaviate/adapters/handlers/graphql"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/schema"
)

type fakeGQLProvider struct{ gql libgraphql.GraphQL }

func (f fakeGQLProvider) GetGraphQL() libgraphql.GraphQL { return f.gql }

// TestGraphQLHandlersDisabledGate covers both the POST and batch handlers. The
// graph is kept current even while disabled, so this explicit gate — not a nil
// graph — is what rejects requests when DISABLE_GRAPHQL is set.
func TestGraphQLHandlersDisabledGate(t *testing.T) {
	mgr := &schema.Manager{Authorizer: &authorization.DummyAuthorizer{}}
	principal := &models.Principal{Username: "tester"}
	req := httptest.NewRequest(http.MethodPost, "/v1/graphql", nil)

	setup := func(disabled *runtime.DynamicValue[bool]) *operations.WeaviateAPI {
		api := &operations.WeaviateAPI{}
		setupGraphQLHandlers(api, fakeGQLProvider{}, mgr, disabled, false, nil, logrus.New())
		return api
	}

	postMsg := func(t *testing.T, api *operations.WeaviateAPI) string {
		t.Helper()
		resp := api.GraphqlGraphqlPostHandler.Handle(
			graphql.GraphqlPostParams{HTTPRequest: req, Body: &models.GraphQLQuery{Query: "{ x }"}}, principal)
		ue, ok := resp.(*graphql.GraphqlPostUnprocessableEntity)
		require.Truef(t, ok, "expected 422 responder, got %T", resp)
		require.NotNil(t, ue.Payload)
		require.NotEmpty(t, ue.Payload.Error)
		return ue.Payload.Error[0].Message
	}

	batchMsg := func(t *testing.T, api *operations.WeaviateAPI) string {
		t.Helper()
		resp := api.GraphqlGraphqlBatchHandler.Handle(
			graphql.GraphqlBatchParams{HTTPRequest: req, Body: models.GraphQLQueries{{Query: "{ x }"}}}, principal)
		ue, ok := resp.(*graphql.GraphqlBatchUnprocessableEntity)
		require.Truef(t, ok, "expected 422 responder, got %T", resp)
		require.NotNil(t, ue.Payload)
		require.NotEmpty(t, ue.Payload.Error)
		return ue.Payload.Error[0].Message
	}

	t.Run("disabled gates both POST and batch", func(t *testing.T) {
		api := setup(runtime.NewDynamicValue(true))
		assert.Equal(t, "graphql api is disabled", postMsg(t, api))
		assert.Equal(t, "graphql api is disabled", batchMsg(t, api))
	})

	t.Run("enabled passes the gate", func(t *testing.T) {
		// nil graph → "no graphql provider present"; reaching it proves the
		// disabled gate did not fire.
		api := setup(runtime.NewDynamicValue(false))
		assert.Contains(t, postMsg(t, api), "no graphql provider present")
		assert.Contains(t, batchMsg(t, api), "no graphql provider present")
	})

	t.Run("toggle is read per request on the live handler", func(t *testing.T) {
		// The handler closure captures the DynamicValue pointer, so the overrides
		// reload loop flipping it changes behavior without re-registering handlers.
		dv := runtime.NewDynamicValue(false)
		api := setup(dv)
		assert.Contains(t, postMsg(t, api), "no graphql provider present")

		require.NoError(t, dv.SetValue(true))
		assert.Equal(t, "graphql api is disabled", postMsg(t, api))
		assert.Equal(t, "graphql api is disabled", batchMsg(t, api))

		require.NoError(t, dv.SetValue(false))
		assert.Contains(t, postMsg(t, api), "no graphql provider present")
		assert.Contains(t, batchMsg(t, api), "no graphql provider present")
	})
}

// TestGraphQLHandlersNamespacesWinOverRuntimeToggle: on a namespace-enabled
// cluster the namespaces gate must win over the runtime DISABLE_GRAPHQL flag, so
// a push of disable_graphql=false can never open GraphQL (its schema can't model
// namespace-qualified class names). Both endpoints stay 410 Gone regardless of
// the flag.
func TestGraphQLHandlersNamespacesWinOverRuntimeToggle(t *testing.T) {
	mgr := &schema.Manager{Authorizer: &authorization.DummyAuthorizer{}}
	principal := &models.Principal{Username: "tester"}
	req := httptest.NewRequest(http.MethodPost, "/v1/graphql", nil)

	dv := runtime.NewDynamicValue(false) // flag says "enabled" — must still be Gone
	api := &operations.WeaviateAPI{}
	setupGraphQLHandlers(api, fakeGQLProvider{}, mgr, dv, true /* namespacesEnabled */, nil, logrus.New())

	assertGone := func(t *testing.T, label string) {
		t.Helper()
		postResp := api.GraphqlGraphqlPostHandler.Handle(
			graphql.GraphqlPostParams{HTTPRequest: req, Body: &models.GraphQLQuery{Query: "{ x }"}}, principal)
		_, postGone := postResp.(*graphql.GraphqlPostGone)
		require.Truef(t, postGone, "%s POST: expected GraphqlPostGone on namespace cluster, got %T", label, postResp)

		batchResp := api.GraphqlGraphqlBatchHandler.Handle(
			graphql.GraphqlBatchParams{HTTPRequest: req, Body: models.GraphQLQueries{{Query: "{ x }"}}}, principal)
		_, batchGone := batchResp.(*graphql.GraphqlBatchGone)
		require.Truef(t, batchGone, "%s batch: expected GraphqlBatchGone on namespace cluster, got %T", label, batchResp)
	}

	assertGone(t, "flag=false")
	require.NoError(t, dv.SetValue(true))
	assertGone(t, "flag=true")
	require.NoError(t, dv.SetValue(false)) // operator tries to enable GraphQL at runtime
	assertGone(t, "flag=false after runtime push")
}

func TestGraphqlNamespacesBlockedCounter(t *testing.T) {
	t.Run("nil metrics yields no counter and a no-op log", func(t *testing.T) {
		m := newGraphqlRequestsTotal(nil, logrus.New())
		require.Nil(t, m.namespacesBlocked)
		require.NotPanics(t, m.logNamespacesBlocked)
	})

	t.Run("increments once per blocked request", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		m := newGraphqlRequestsTotal(&monitoring.PrometheusMetrics{Registerer: reg}, logrus.New())
		require.NotNil(t, m.namespacesBlocked)

		require.Equal(t, float64(0), testutil.ToFloat64(m.namespacesBlocked))
		m.logNamespacesBlocked()
		m.logNamespacesBlocked()
		require.Equal(t, float64(2), testutil.ToFloat64(m.namespacesBlocked))
	})

	t.Run("re-registration reuses the existing counter", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		metrics := &monitoring.PrometheusMetrics{Registerer: reg}
		first := newGraphqlRequestsTotal(metrics, logrus.New())
		second := newGraphqlRequestsTotal(metrics, logrus.New())
		require.NotNil(t, first.namespacesBlocked)
		require.NotNil(t, second.namespacesBlocked)

		first.logNamespacesBlocked()
		second.logNamespacesBlocked()
		require.Equal(t, float64(2), testutil.ToFloat64(second.namespacesBlocked))
	})
}
