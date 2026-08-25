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
		wantAllow  string
		wantBody   string
		wantServed bool
	}{
		{
			name:       "GET while enabled is refused with 405",
			method:     http.MethodGet,
			enabled:    true,
			wantStatus: http.StatusMethodNotAllowed,
			wantAllow:  "POST, DELETE",
			wantBody:   mcpGetNotSupportedBody,
		},
		{
			name:       "GET while disabled reports 503",
			method:     http.MethodGet,
			enabled:    false,
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   mcpDisabledBody,
		},
		{
			name:       "POST while disabled reports 503",
			method:     http.MethodPost,
			enabled:    false,
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   mcpDisabledBody,
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
			wantBody:   mcpDisabledBody,
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
			gate := mcpGate{
				enabled: func() bool { return tt.enabled },
				server: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					served = true
					w.WriteHeader(http.StatusOK)
				}),
			}

			w := httptest.NewRecorder()
			r := httptest.NewRequest(tt.method, "/v1/mcp", nil)
			if tt.method == http.MethodGet {
				gate.rejectGet(w, r)
			} else {
				gate.serve(w, r)
			}

			require.Equal(t, tt.wantStatus, w.Code)
			require.Equal(t, tt.wantServed, served)
			require.Equal(t, tt.wantAllow, w.Header().Get("Allow"))
			if tt.wantBody != "" {
				require.Equal(t, "application/json", w.Header().Get("Content-Type"))
				require.JSONEq(t, tt.wantBody, w.Body.String())
			}
		})
	}
}
