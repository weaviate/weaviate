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

package restrictions

import (
	"errors"
	"fmt"
	"testing"
)

func TestViolationErrorString(t *testing.T) {
	tests := []struct {
		name string
		err  *ViolationError
		want string
	}{
		{
			name: "rendered message takes precedence",
			err: &ViolationError{
				Restriction:     RestrictionVectorIndexType,
				Value:           "flat",
				Allowed:         []string{"hfresh", "hnsw"},
				RenderedMessage: "custom message",
			},
			want: "custom message",
		},
		{
			name: "fallback when rendered is empty",
			err: &ViolationError{
				Restriction: RestrictionVectorIndexType,
				Value:       "flat",
				Allowed:     []string{"hfresh", "hnsw"},
			},
			want: `config not allowed: vector_index_type="flat" not in allow-list [hfresh, hnsw]`,
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

func TestAsViolation(t *testing.T) {
	original := &ViolationError{
		Restriction: RestrictionCompression,
		Value:       "pq",
		Allowed:     []string{"rq-8"},
	}

	t.Run("direct match", func(t *testing.T) {
		got, ok := AsViolation(original)
		if !ok || got != original {
			t.Fatalf("expected direct match, got ok=%v err=%v", ok, got)
		}
	})

	t.Run("wrapped match", func(t *testing.T) {
		wrapped := fmt.Errorf("wrapped: %w", original)
		got, ok := AsViolation(wrapped)
		if !ok || got != original {
			t.Fatalf("expected wrapped match, got ok=%v err=%v", ok, got)
		}
	})

	t.Run("non-match", func(t *testing.T) {
		got, ok := AsViolation(errors.New("plain error"))
		if ok || got != nil {
			t.Fatalf("expected non-match, got ok=%v err=%v", ok, got)
		}
	})

	t.Run("nil error", func(t *testing.T) {
		got, ok := AsViolation(nil)
		if ok || got != nil {
			t.Fatalf("expected nil-error → false, got ok=%v err=%v", ok, got)
		}
	})
}

func TestNewViolationError(t *testing.T) {
	tests := []struct {
		name        string
		template    string
		restriction RestrictionName
		value       string
		allowed     []string
		wantMessage string
	}{
		{
			name:        "default template renders all placeholders",
			template:    "",
			restriction: RestrictionVectorIndexType,
			value:       "flat",
			allowed:     []string{"hnsw", "hfresh"},
			wantMessage: "flat is not allowed for vector_index_type. Allowed values: hfresh, hnsw.",
		},
		{
			name:        "custom template wins",
			template:    "rejected: {value} for {restriction}; pick one of {allowed}",
			restriction: RestrictionCompression,
			value:       "pq",
			allowed:     []string{"rq-8"},
			wantMessage: "rejected: pq for compression; pick one of rq-8",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewViolationError(tt.template, tt.restriction, tt.value, tt.allowed)
			if got.Restriction != tt.restriction {
				t.Errorf("Restriction = %q, want %q", got.Restriction, tt.restriction)
			}
			if got.Value != tt.value {
				t.Errorf("Value = %q, want %q", got.Value, tt.value)
			}
			if got.RenderedMessage != tt.wantMessage {
				t.Errorf("RenderedMessage = %q, want %q", got.RenderedMessage, tt.wantMessage)
			}
		})
	}
}
