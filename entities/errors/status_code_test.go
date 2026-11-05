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

package errors

import (
	stderrors "errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStatusCode(t *testing.T) {
	t.Parallel()

	plain := stderrors.New("boom")
	rate := NewErrRateLimit()
	tests := []struct {
		name     string
		err      error
		wantCode int
		wantOK   bool
	}{
		{name: "nil", wantCode: 0, wantOK: false},
		{name: "plain error has no code", err: plain, wantOK: false},
		{name: "rate limit is 429", err: rate, wantCode: http.StatusTooManyRequests, wantOK: true},
		{
			name:     "wrapped rate limit is 429",
			err:      fmt.Errorf("explorer: get class: %w", rate),
			wantCode: http.StatusTooManyRequests,
			wantOK:   true,
		},
		{
			name:     "Get ingress wraps the rate limit and keeps 429",
			err:      NewErrGraphQLUser(rate, "Get", "SomeAction"),
			wantCode: http.StatusTooManyRequests,
			wantOK:   true,
		},
		{
			name:     "double wrap through the Get ingress still yields 429",
			err:      NewErrGraphQLUser(fmt.Errorf("shed: %w", rate), "Get", "SomeAction"),
			wantCode: http.StatusTooManyRequests,
			wantOK:   true,
		},
		{
			name:   "graphql user error without a status stays uncoded",
			err:    NewErrGraphQLUser(plain, "Get", "SomeAction"),
			wantOK: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			code, ok := StatusCode(tt.err)
			require.Equal(t, tt.wantOK, ok)
			require.Equal(t, tt.wantCode, code)
		})
	}
}
