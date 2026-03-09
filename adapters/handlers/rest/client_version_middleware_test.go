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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAddClientVersionToContext(t *testing.T) {
	tests := []struct {
		name          string
		headerValue   string
		expectInCtx   bool
		expectedValue string
	}{
		{
			name:          "python client header",
			headerValue:   "weaviate-client-python/4.10.0",
			expectInCtx:   true,
			expectedValue: "weaviate-client-python/4.10.0",
		},
		{
			name:          "go client header",
			headerValue:   "weaviate-client-go/2.5.0",
			expectInCtx:   true,
			expectedValue: "weaviate-client-go/2.5.0",
		},
		{
			name:        "no header",
			headerValue: "",
			expectInCtx: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var capturedVersion interface{}
			inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				capturedVersion = r.Context().Value("clientVersion")
			})

			handler := addClientVersionToContext(inner)

			req, err := http.NewRequest("GET", "/v1/objects", nil)
			require.NoError(t, err)
			if tt.headerValue != "" {
				req.Header.Set("X-Weaviate-Client", tt.headerValue)
			}

			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)

			if tt.expectInCtx {
				assert.Equal(t, tt.expectedValue, capturedVersion)
			} else {
				assert.Nil(t, capturedVersion)
			}
		})
	}
}
