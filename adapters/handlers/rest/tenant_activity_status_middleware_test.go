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
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
)

func TestMarkEmptyTenantActivityStatusOnCreate(t *testing.T) {
	t.Run("marks empty activity status on tenant create and restores request body", func(t *testing.T) {
		body := `[{"name":"tenant_empty","activityStatus":""}]`
		handler := markEmptyTenantActivityStatusOnCreate(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			err, ok := emptyTenantActivityStatusFromContext(r.Context())
			require.True(t, ok)
			require.EqualError(t, err, `invalid activity status '' for tenant "tenant_empty"`)

			restored, readErr := io.ReadAll(r.Body)
			require.NoError(t, readErr)
			require.Equal(t, body, string(restored))
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodPost, "/v1/schema/TestTenant/tenants", strings.NewReader(body))
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("allows omitted activity status without context marker", func(t *testing.T) {
		body := `[{"name":"tenant_default"}]`
		handler := markEmptyTenantActivityStatusOnCreate(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, ok := emptyTenantActivityStatusFromContext(r.Context())
			require.False(t, ok)

			restored, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			require.Equal(t, body, string(restored))
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodPost, "/v1/schema/TestTenant/tenants", strings.NewReader(body))
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("ignores other routes", func(t *testing.T) {
		nextCalled := false
		handler := markEmptyTenantActivityStatusOnCreate(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			nextCalled = true
			_, ok := emptyTenantActivityStatusFromContext(r.Context())
			require.False(t, ok)
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodPost, "/v1/schema/TestTenant", strings.NewReader(`{"activityStatus":""}`))
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		require.True(t, nextCalled)
		require.Equal(t, http.StatusOK, w.Code)
	})
}

func TestCreateTenantsRejectsMarkedEmptyTenantActivityStatus(t *testing.T) {
	err := fmt.Errorf(`invalid activity status '' for tenant "tenant_empty"`)
	req := httptest.NewRequest(http.MethodPost, "/v1/schema/TestTenant/tenants", nil)
	ctx := context.WithValue(req.Context(), emptyTenantActivityStatusContextKey{}, err)
	req = req.WithContext(ctx)

	handler := &schemaHandlers{metricRequestsTotal: &fakeMetricRequestsTotal{}}

	res := handler.createTenants(schema.TenantsCreateParams{
		ClassName:   "TestTenant",
		HTTPRequest: req,
	}, nil)

	typed, ok := res.(*schema.TenantsCreateUnprocessableEntity)
	require.True(t, ok)
	require.NotNil(t, typed.Payload)
	require.Len(t, typed.Payload.Error, 1)
	require.Equal(t, `invalid activity status '' for tenant "tenant_empty"`, typed.Payload.Error[0].Message)
}
