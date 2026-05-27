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

package usagelimits

import (
	"errors"
	"fmt"
	"testing"
)

func TestLimitExceededErrorString(t *testing.T) {
	tests := []struct {
		name string
		err  *LimitExceededError
		want string
	}{
		{
			name: "rendered message takes precedence",
			err:  &LimitExceededError{Limit: LimitObjects, Value: 100, RenderedMessage: "custom"},
			want: "custom",
		},
		{
			name: "fallback when rendered is empty",
			err:  &LimitExceededError{Limit: LimitObjects, Value: 100},
			want: "usage limit exceeded: objects count limit of 100 reached",
		},
		{
			name: "nil receiver returns empty",
			err:  nil,
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.want {
				t.Errorf("Error() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestAsLimitExceeded(t *testing.T) {
	original := &LimitExceededError{Limit: LimitObjects, Value: 10}

	t.Run("direct match", func(t *testing.T) {
		got, ok := AsLimitExceeded(original)
		if !ok || got != original {
			t.Fatalf("expected direct match, got ok=%v err=%v", ok, got)
		}
	})

	t.Run("wrapped match", func(t *testing.T) {
		wrapped := fmt.Errorf("wrapped: %w", original)
		got, ok := AsLimitExceeded(wrapped)
		if !ok || got != original {
			t.Fatalf("expected wrapped match, got ok=%v err=%v", ok, got)
		}
	})

	t.Run("non-match", func(t *testing.T) {
		got, ok := AsLimitExceeded(errors.New("plain error"))
		if ok || got != nil {
			t.Fatalf("expected non-match, got ok=%v err=%v", ok, got)
		}
	})

	t.Run("nil error", func(t *testing.T) {
		got, ok := AsLimitExceeded(nil)
		if ok || got != nil {
			t.Fatalf("expected nil-error → false, got ok=%v err=%v", ok, got)
		}
	})
}

func TestFromBodyJSON(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		want   *LimitExceededError
		wantOK bool
	}{
		{
			name:   "usage-limit body parses",
			body:   `{"errorCode":"USAGE_LIMIT_EXCEEDED","limit":"objects","value":10,"message":"hit cap"}`,
			want:   &LimitExceededError{Limit: LimitObjects, Value: 10, RenderedMessage: "hit cap"},
			wantOK: true,
		},
		{
			name: "wrong errorCode is not a usage-limit body",
			body: `{"errorCode":"OTHER","limit":"objects","value":10}`,
		},
		{
			name: "non-JSON body is not a usage-limit body",
			body: `status code: 429, error: oops`,
		},
		{
			name: "empty body",
			body: ``,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := FromBodyJSON([]byte(tt.body))
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v (got=%v)", ok, tt.wantOK, got)
			}
			if !tt.wantOK {
				return
			}
			if got.Limit != tt.want.Limit || got.Value != tt.want.Value || got.RenderedMessage != tt.want.RenderedMessage {
				t.Errorf("FromBodyJSON = %+v, want %+v", got, tt.want)
			}
		})
	}
}
