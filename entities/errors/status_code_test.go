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
		{
			// Same method set as adapters/clients.HTTPError. That status is an
			// internal cluster response and must not become a public code,
			// even when a handler wraps it on the way to a 500.
			name:   "internal StatusCode is not a public code",
			err:    fmt.Errorf("forwarded object write: %w", statusCodeOnly{code: http.StatusUnauthorized}),
			wantOK: false,
		},
		{
			name:     "public code wins over a wrapped internal status",
			err:      fmt.Errorf("auth: %w", publicOverInternal{inner: statusCodeOnly{code: http.StatusUnauthorized}}),
			wantCode: http.StatusForbidden,
			wantOK:   true,
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

// statusCodeOnly mirrors adapters/clients.HTTPError: StatusCode is an
// internal response, not the public error contract.
type statusCodeOnly struct{ code int }

func (e statusCodeOnly) Error() string {
	return fmt.Sprintf("status code: %d, error: unauthorized", e.code)
}

func (e statusCodeOnly) StatusCode() int { return e.code }

// publicOverInternal is a public authorization failure that unwraps to an
// internal status. The public code must win.
type publicOverInternal struct{ inner error }

func (e publicOverInternal) Error() string { return e.inner.Error() }

func (e publicOverInternal) Unwrap() error { return e.inner }

func (e publicOverInternal) PublicErrorCode() int { return http.StatusForbidden }
