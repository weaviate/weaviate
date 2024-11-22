//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package queryschema

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTenantInfo(t *testing.T) {
	t.Run("unauthorized", func(t *testing.T) {
		schema := mockSchema(t, nil, "", 0, nil, http.StatusUnauthorized)
		ctx := context.Background()
		svr := httptest.NewServer(schema)
		info := NewSchemaInfo(svr.URL, DefaultSchemaPrefix)

		_, _, _, err := info.TenantStatus(ctx, "sample-collection", "captain-america")
		require.ErrorContains(t, err, "unauthorized")
	})

	t.Run("not found", func(t *testing.T) {
		schema := mockSchema(t, nil, "", 0, nil, http.StatusNotFound)
		ctx := context.Background()
		svr := httptest.NewServer(schema)
		info := NewSchemaInfo(svr.URL, DefaultSchemaPrefix)

		_, _, _, err := info.TenantStatus(ctx, "sample-collection", "captain-america")
		require.ErrorIs(t, err, ErrTenantNotFound)
	})

	t.Run("error code but no error set on body", func(t *testing.T) {
		schema := mockSchema(t, nil, "", 0, nil, http.StatusInternalServerError)
		ctx := context.Background()
		svr := httptest.NewServer(schema)
		info := NewSchemaInfo(svr.URL, DefaultSchemaPrefix)

		_, _, _, err := info.TenantStatus(ctx, "sample-collection", "captain-america")
		require.ErrorContains(t, err, "error is not set")
	})

	t.Run("errors in body", func(t *testing.T) {
		errMessages := []ErrorResponse{{"some important error"}, {"another important error"}}
		schema := mockSchema(t, errMessages, "", 0, nil, http.StatusInternalServerError)
		ctx := context.Background()
		svr := httptest.NewServer(schema)
		info := NewSchemaInfo(svr.URL, DefaultSchemaPrefix)

		_, _, _, err := info.TenantStatus(ctx, "sample-collection", "captain-america")
		require.ErrorContains(t, err, "some important error")
		require.ErrorContains(t, err, "another important error")
	})

	t.Run("tenant with frozen state", func(t *testing.T) {
		schema := mockSchema(t, nil, "FROZEN", 1, []string{"node-1", "node-2"}, 0)
		ctx := context.Background()
		svr := httptest.NewServer(schema)
		info := NewSchemaInfo(svr.URL, DefaultSchemaPrefix)

		status, belongsToNodes, dataVersion, err := info.TenantStatus(ctx, "sample-collection", "captain-america")
		require.NoError(t, err)
		assert.Equal(t, "FROZEN", status)
		assert.Equal(t, []string{"node-1", "node-2"}, belongsToNodes)
		assert.Equal(t, int64(1), dataVersion)
	})

	t.Run("tenant with data version 2", func(t *testing.T) {
		schema := mockSchema(t, nil, "FROZEN", 2, []string{"node-1", "node-2"}, 0)
		ctx := context.Background()
		svr := httptest.NewServer(schema)
		info := NewSchemaInfo(svr.URL, DefaultSchemaPrefix)

		status, belongsToNodes, dataVersion, err := info.TenantStatus(ctx, "sample-collection", "captain-america")
		require.NoError(t, err)
		assert.Equal(t, "FROZEN", status)
		assert.Equal(t, []string{"node-1", "node-2"}, belongsToNodes)
		assert.Equal(t, int64(2), dataVersion)
	})

	t.Run("tenant with five belongs to nodes", func(t *testing.T) {
		schema := mockSchema(t, nil, "FROZEN", 1, []string{"node-1", "node-2", "node-3", "node-4", "node-5"}, 0)
		ctx := context.Background()
		svr := httptest.NewServer(schema)
		info := NewSchemaInfo(svr.URL, DefaultSchemaPrefix)

		status, belongsToNodes, dataVersion, err := info.TenantStatus(ctx, "sample-collection", "captain-america")
		require.NoError(t, err)
		assert.Equal(t, "FROZEN", status)
		assert.Equal(t, []string{"node-1", "node-2", "node-3", "node-4", "node-5"}, belongsToNodes)
		assert.Equal(t, int64(1), dataVersion)
	})

	t.Run("tenant with active state", func(t *testing.T) {
		schema := mockSchema(t, nil, "ACTIVE", 0, []string{"node-1", "node-2"}, 0)
		ctx := context.Background()
		svr := httptest.NewServer(schema)
		info := NewSchemaInfo(svr.URL, DefaultSchemaPrefix)

		status, belongsToNodes, dataVersion, err := info.TenantStatus(ctx, "sample-collection", "captain-america")
		require.NoError(t, err)
		assert.Equal(t, "ACTIVE", status)
		assert.Equal(t, []string{"node-1", "node-2"}, belongsToNodes)
		assert.Equal(t, int64(0), dataVersion)
	})
}

type schema struct {
	t                    *testing.T
	returnErrors         []ErrorResponse
	returnBelongsToNodes []string
	returnStatus         string
	returnDataVersion    int64
	// if 0, then WriteHeader will not be called
	// so the default status will be 200
	httpStatus int
}

// ServerHTTP json encodes the mock response and uses the httpStatus if set.
// If httpStatus is 0, then the default status will be 200.
func (s *schema) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	res := Response{
		Error:          s.returnErrors,
		BelongsToNodes: s.returnBelongsToNodes,
		Status:         s.returnStatus,
		DataVersion:    s.returnDataVersion,
	}
	if s.httpStatus != 0 {
		w.WriteHeader(s.httpStatus)
	}
	if err := json.NewEncoder(w).Encode(res); err != nil {
		require.NoError(s.t, err)
	}
}

func mockSchema(t *testing.T, returnErrors []ErrorResponse, returnStatus string,
	returnDataVersion int64, returnBelongsToNodes []string, httpStatus int,
) *schema {
	return &schema{
		t:                    t,
		returnErrors:         returnErrors,
		returnStatus:         returnStatus,
		returnDataVersion:    returnDataVersion,
		returnBelongsToNodes: returnBelongsToNodes,
		httpStatus:           httpStatus,
	}
}
