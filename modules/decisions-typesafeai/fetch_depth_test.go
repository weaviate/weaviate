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

package moddecisionstypesafeai

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/modulecapabilities"
)

func TestRerankFetchDepth(t *testing.T) {
	tests := []struct {
		name     string
		settings map[string]any
		header   string
		// pageEnd is offset+limit of the page; 10 when unset.
		pageEnd int
		want    int
		wantErr string
	}{
		{name: "off by default", settings: map[string]any{}, want: 0},
		{name: "class setting", settings: map[string]any{"fetchDepth": 40}, want: 40},
		{name: "header wins over the class setting", settings: map[string]any{"fetchDepth": 40}, header: "25", want: 25},
		{name: "header of 0 switches it off", settings: map[string]any{"fetchDepth": 40}, header: "0", want: 0},
		{name: "a page past the depth fetches up to its end", settings: map[string]any{"fetchDepth": 40}, pageEnd: 60, want: 60},
		{name: "a page that ends at maxDocuments", settings: map[string]any{"fetchDepth": 40}, pageEnd: 100, want: 100},
		{
			name: "a page beyond maxDocuments", settings: map[string]any{"fetchDepth": 40}, pageEnd: 101,
			wantErr: "the page ends at 101, beyond maxDocuments 100: lower offset+limit or raise maxDocuments",
		},
		{name: "off: the page is not checked", settings: map[string]any{}, pageEnd: 101, want: 0},
		{
			name: "the bound is the same as Rank's", settings: map[string]any{"maxDocuments": 5000}, header: "3000",
			wantErr: "X-Typesafeai-Fetch-Depth must be a whole number between 0 and maxDocuments 1000, got \"3000\"",
		},
		{
			name: "header above maxDocuments", settings: map[string]any{"maxDocuments": 50}, header: "51",
			wantErr: "X-Typesafeai-Fetch-Depth must be a whole number between 0 and maxDocuments 50, got \"51\"",
		},
		{
			name: "header not a number", settings: map[string]any{}, header: "many",
			wantErr: "X-Typesafeai-Fetch-Depth must be a whole number between 0 and maxDocuments 100, got \"many\"",
		},
		{
			name: "header negative", settings: map[string]any{}, header: "-1",
			wantErr: "X-Typesafeai-Fetch-Depth must be a whole number between 0 and maxDocuments 100, got \"-1\"",
		},
		{
			name: "class setting above maxDocuments", settings: map[string]any{"fetchDepth": 200},
			wantErr: "fetchDepth must be between 0 and maxDocuments 100, got 200",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.header != "" {
				ctx = context.WithValue(ctx, "X-Typesafeai-Fetch-Depth", []string{tt.header})
			}

			pageEnd := tt.pageEnd
			if pageEnd == 0 {
				pageEnd = 10
			}

			got, err := New().RerankFetchDepth(ctx, classConfig(tt.settings), pageEnd)

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestValidateClassFetchDepth(t *testing.T) {
	tests := []struct {
		name     string
		settings map[string]any
		wantErr  string
	}{
		{name: "fetchDepth within maxDocuments", settings: map[string]any{"fetchDepth": 100}},
		{name: "fetchDepth above maxDocuments", settings: map[string]any{"fetchDepth": 101}, wantErr: "fetchDepth must be between 0 and maxDocuments 100, got 101"},
		{name: "fetchDepth negative", settings: map[string]any{"fetchDepth": -3}, wantErr: "fetchDepth must be between 0 and maxDocuments 100, got -3"},
		{name: "fetchDepth of the wrong type", settings: map[string]any{"fetchDepth": "deep"}, wantErr: "fetchDepth must be between 0 and maxDocuments 100, got -1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := New().ValidateClass(context.Background(), nil, classConfig(tt.settings))
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

var _ = modulecapabilities.RerankFetchDepthProvider(New())
