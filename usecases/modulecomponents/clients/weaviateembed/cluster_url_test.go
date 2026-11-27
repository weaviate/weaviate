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

package weaviateembed

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClusterURLHeader(t *testing.T) {
	tests := []struct {
		name     string
		header   string
		validate bool
		wantURL  string
		wantErr  string
	}{
		{name: "missing header", wantErr: "no cluster URL found in request header: X-Weaviate-Cluster-Url"},
		{name: "header is used", header: "https://cluster.example.com", wantURL: "https://cluster.example.com"},
		{
			name:     "header is checked when validation is on",
			header:   "https://10.0.0.5",
			validate: true,
			wantErr:  "baseURL cannot target internal addresses",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.validate {
				t.Setenv("MODULES_VALIDATE_BASE_URL", "true")
			}
			ctx := context.Background()
			if tt.header != "" {
				ctx = context.WithValue(ctx, "X-Weaviate-Cluster-Url", []string{tt.header})
			}

			got, err := getClusterURL(ctx)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantURL, got)
		})
	}
}
