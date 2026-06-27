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

package objects

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/config"
)

func TestLocalOffsetLimit(t *testing.T) {
	cfg := &config.WeaviateConfig{}
	cfg.Config.QueryDefaults.Limit = 20
	cfg.Config.QueryMaximumResults = 100
	m := &Manager{config: cfg}

	ptr := func(i int64) *int64 { return &i }

	tests := []struct {
		name       string
		offset     *int64
		limit      *int64
		wantOffset int
		wantLimit  int
		wantErr    bool
		wantErrMsg string
	}{
		{
			name:       "nil offset and nil limit uses default limit",
			offset:     nil,
			limit:      nil,
			wantOffset: 0,
			wantLimit:  20,
		},
		{
			name:       "valid offset and limit",
			offset:     ptr(5),
			limit:      ptr(10),
			wantOffset: 5,
			wantLimit:  10,
		},
		{
			name:       "zero offset and limit",
			offset:     ptr(0),
			limit:      ptr(0),
			wantOffset: 0,
			wantLimit:  0,
		},
		{
			name:       "nil offset with valid limit",
			offset:     nil,
			limit:      ptr(5),
			wantOffset: 0,
			wantLimit:  5,
		},
		{
			name:       "negative offset is rejected",
			offset:     ptr(-1),
			limit:      ptr(10),
			wantErr:    true,
			wantErrMsg: "offset must be non-negative, got -1",
		},
		{
			name:       "negative limit is rejected",
			offset:     ptr(0),
			limit:      ptr(-1),
			wantErr:    true,
			wantErrMsg: "limit must be non-negative, got -1",
		},
		{
			name:       "negative offset and negative limit rejects offset first",
			offset:     ptr(-5),
			limit:      ptr(-10),
			wantErr:    true,
			wantErrMsg: "offset must be non-negative, got -5",
		},
		{
			name:       "large negative limit is rejected",
			offset:     nil,
			limit:      ptr(-9999),
			wantErr:    true,
			wantErrMsg: "limit must be non-negative, got -9999",
		},
		{
			name:       "offset plus limit exceeding maximum is rejected",
			offset:     ptr(50),
			limit:      ptr(60),
			wantErr:    true,
			wantErrMsg: "query maximum results exceeded",
		},
		{
			name:       "offset at maximum boundary is allowed",
			offset:     ptr(40),
			limit:      ptr(60),
			wantOffset: 40,
			wantLimit:  60,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotOffset, gotLimit, err := m.localOffsetLimit(tt.offset, tt.limit)
			if tt.wantErr {
				require.Error(t, err)
				assert.True(t, errors.As(err, &ErrInvalidUserInput{}),
					"expected ErrInvalidUserInput, got %T: %v", err, err)
				assert.Contains(t, err.Error(), tt.wantErrMsg)
				assert.Equal(t, 0, gotOffset)
				assert.Equal(t, 0, gotLimit)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantOffset, gotOffset)
			assert.Equal(t, tt.wantLimit, gotLimit)
		})
	}
}
