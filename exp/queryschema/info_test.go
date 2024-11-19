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
	t.Run("tenant with frozen state", func(t *testing.T) {
		schema := mockSchema(t)
		ctx := context.Background()
		svr := httptest.NewServer(schema)
		info := NewSchemaInfo(svr.URL, DefaultSchemaPrefix)

		status, belongsToNodes, _, err := info.TenantStatus(ctx, "sample-collection", "captain-america")
		require.NoError(t, err)
		assert.Equal(t, status, "FROZEN")
		assert.Equal(t, []string{"node-1, node-2"}, belongsToNodes)
	})

	t.Run("tenant with non-frozen state", func(t *testing.T) {
		schema := mockSchema(t)
		schema.returnActive = true // return non-frozen tenant

		ctx := context.Background()
		svr := httptest.NewServer(schema)
		info := NewSchemaInfo(svr.URL, DefaultSchemaPrefix)

		status, _, _, err := info.TenantStatus(ctx, "sample-collection", "captain-america")
		require.ErrorIs(t, err, ErrTenantNotFound)
		require.Empty(t, status)
	})

	t.Run("error getting tenant status", func(t *testing.T) {
		schema := mockSchema(t)
		schema.returnError = true // return error in getting tenant info

		ctx := context.Background()
		svr := httptest.NewServer(schema)
		info := NewSchemaInfo(svr.URL, DefaultSchemaPrefix)

		status, _, _, err := info.TenantStatus(ctx, "sample-collection", "captain-america")
		require.ErrorContains(t, err, "some important error")
		require.Empty(t, status)
	})

	t.Run("empty tenants", func(t *testing.T) {
		schema := mockSchema(t)
		schema.returnEmpty = true // return error in getting tenant info

		ctx := context.Background()
		svr := httptest.NewServer(schema)
		info := NewSchemaInfo(svr.URL, DefaultSchemaPrefix)

		status, _, _, err := info.TenantStatus(ctx, "sample-collection", "captain-america")
		require.ErrorIs(t, err, ErrTenantNotFound)
		require.Empty(t, status)
	})
}

type schema struct {
	t            *testing.T
	returnError  bool
	returnActive bool
	returnEmpty  bool
}

func (s *schema) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	res := Response{}

	switch {
	case s.returnError:
		res = Response{
			Error: []ErrorResponse{
				{
					Message: "some important error",
				},
			},
		}
	case s.returnActive:
		res = Response{
			Name:           "iron-man",
			Status:         "ACTIVE",
			BelongsToNodes: []string{"node-1"},
		}

	case s.returnEmpty:
		// do not append anything
	default:
		res = Response{
			Name:           "captain-america",
			Status:         "FROZEN",
			BelongsToNodes: []string{"node-1, node-2"},
		}

	}

	if err := json.NewEncoder(w).Encode(res); err != nil {
		require.NoError(s.t, err)
	}
}

func mockSchema(t *testing.T) *schema {
	return &schema{
		t:            t,
		returnError:  false,
		returnActive: false,
		returnEmpty:  false,
	}
}
