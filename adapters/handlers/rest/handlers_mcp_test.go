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

	"github.com/stretchr/testify/require"
)

func TestMCPGate(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		enabled    bool
		wantStatus int
		wantServed bool
	}{
		{
			name:       "POST while disabled reports 503",
			method:     http.MethodPost,
			enabled:    false,
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name:       "POST while enabled reaches the MCP server",
			method:     http.MethodPost,
			enabled:    true,
			wantStatus: http.StatusOK,
			wantServed: true,
		},
		{
			name:       "DELETE while disabled reports 503",
			method:     http.MethodDelete,
			enabled:    false,
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name:       "DELETE while enabled reaches the MCP server",
			method:     http.MethodDelete,
			enabled:    true,
			wantStatus: http.StatusOK,
			wantServed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			served := false
			gate := mcpGate(
				func() bool { return tt.enabled },
				http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					served = true
					w.WriteHeader(http.StatusOK)
				}),
			)

			w := httptest.NewRecorder()
			gate.ServeHTTP(w, httptest.NewRequest(tt.method, "/v1/mcp", nil))

			require.Equal(t, tt.wantStatus, w.Code)
			require.Equal(t, tt.wantServed, served)
			if !tt.enabled {
				require.Equal(t, "application/json", w.Header().Get("Content-Type"))
				require.JSONEq(t, mcpDisabledBody, w.Body.String())
			}
		})
	}
}
