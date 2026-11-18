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

package clients

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRankErrorStatus(t *testing.T) {
	tests := []struct {
		name         string
		status       int
		contentType  string
		body         string
		wantContains []string
		wantNotBody  string
	}{
		{
			name:         "503 with empty body",
			status:       http.StatusServiceUnavailable,
			wantContains: []string{"Contextual AI API failed with status 503"},
		},
		{
			name:         "502 with HTML body",
			status:       http.StatusBadGateway,
			contentType:  "text/html",
			body:         "<html><body>Bad Gateway</body></html>",
			wantContains: []string{"Contextual AI API failed with status 502"},
			wantNotBody:  "Bad Gateway",
		},
		{
			name:         "429 with JSON error message",
			status:       http.StatusTooManyRequests,
			contentType:  "application/json",
			body:         `{"message":"rate limit exceeded"}`,
			wantContains: []string{"Contextual AI API failed with status 429", "rate limit exceeded"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tt.contentType != "" {
					w.Header().Set("Content-Type", tt.contentType)
				}
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer server.Close()

			c := New("apiKey", 0, logrus.New())
			c.host = server.URL

			_, err := c.Rank(context.Background(), "I work at Apple", []string{"Where do I work?"}, fakeClassConfig{classConfig: map[string]any{"model": testModel}})

			require.Error(t, err)
			for _, want := range tt.wantContains {
				assert.Contains(t, err.Error(), want)
			}
			if tt.wantNotBody != "" {
				assert.NotContains(t, err.Error(), tt.wantNotBody)
			}
		})
	}
}
