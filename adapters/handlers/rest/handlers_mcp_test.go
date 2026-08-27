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

	mcplib "github.com/mark3labs/mcp-go/mcp"
	"github.com/stretchr/testify/require"
)

func TestMCPGate(t *testing.T) {
	const bogusVersionBody = `{"error":"unsupported MCP protocol version: bogus"}`

	tests := []struct {
		name       string
		method     string
		enabled    bool
		version    string
		wantStatus int
		wantServed bool
		wantBody   string
	}{
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
		{
			name:       "POST with a supported protocol version reaches the MCP server",
			method:     http.MethodPost,
			enabled:    true,
			version:    mcplib.LATEST_PROTOCOL_VERSION,
			wantStatus: http.StatusOK,
			wantServed: true,
		},
		{
			name:       "POST with an unsupported protocol version reports 400",
			method:     http.MethodPost,
			enabled:    true,
			version:    "bogus",
			wantStatus: http.StatusBadRequest,
			wantBody:   bogusVersionBody,
		},
		{
			name:       "an unsupported protocol version while disabled still reports 503",
			method:     http.MethodPost,
			enabled:    false,
			version:    "bogus",
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   mcpDisabledBody,
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

			req := httptest.NewRequest(tt.method, "/v1/mcp", nil)
			if tt.version != "" {
				req.Header.Set("MCP-Protocol-Version", tt.version)
			}
			w := httptest.NewRecorder()
			gate.ServeHTTP(w, req)

			require.Equal(t, tt.wantStatus, w.Code)
			require.Equal(t, tt.wantServed, served)
			if tt.wantBody != "" {
				require.Equal(t, "application/json", w.Header().Get("Content-Type"))
				require.JSONEq(t, tt.wantBody, w.Body.String())
			}
		})
	}
}
