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
	"errors"
	"fmt"
	"testing"
)

func TestNotEnoughReplicasError(t *testing.T) {
	rootCause := errors.New("dial tcp 10.0.1.5:8080: i/o timeout")

	tests := []struct {
		name       string
		required   int
		got        int
		cause      error
		additional int
		wantMsg    string
	}{
		{
			name:    "no context",
			wantMsg: ErrReplicas.Error(),
		},
		{
			name:    "cause only",
			cause:   rootCause,
			wantMsg: ErrReplicas.Error() + ": " + rootCause.Error(),
		},
		{
			name:     "counts only",
			required: 3,
			got:      1,
			wantMsg:  ErrReplicas.Error() + ": required 3 replicas, got 1",
		},
		{
			name:       "full message",
			required:   3,
			got:        1,
			cause:      rootCause,
			additional: 2,
			wantMsg: ErrReplicas.Error() +
				": required 3 replicas, got 1: " +
				rootCause.Error() +
				" (and 2 more errors)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := NewNotEnoughReplicasErrorWithCounts(tc.required, tc.got, tc.cause, tc.additional)

			if got := err.Error(); got != tc.wantMsg {
				t.Fatalf("Error() = %q, want %q", got, tc.wantMsg)
			}

			// errors.Is must match the sentinel for backward compatibility.
			if !errors.Is(err, ErrReplicas) {
				t.Fatalf("errors.Is(err, ErrReplicas) = false, want true")
			}

			// errors.Is must traverse to the underlying cause.
			if tc.cause != nil && !errors.Is(err, tc.cause) {
				t.Fatalf("errors.Is(err, cause) = false, want true")
			}

			// Unwrap returns the underlying cause.
			if got := errors.Unwrap(err); got != tc.cause {
				t.Fatalf("Unwrap() = %v, want %v", got, tc.cause)
			}
		})
	}
}

func TestNotEnoughReplicasError_Simple(t *testing.T) {
	rootCause := errors.New("build routing plan: shard not found")
	err := NewNotEnoughReplicasError(rootCause)

	want := ErrReplicas.Error() + ": " + rootCause.Error()
	if got := err.Error(); got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
	if !errors.Is(err, ErrReplicas) {
		t.Fatalf("errors.Is(err, ErrReplicas) = false, want true")
	}
	if !errors.Is(err, rootCause) {
		t.Fatalf("errors.Is(err, rootCause) = false, want true")
	}
}

func TestNotEnoughReplicasError_WrapsFmtErrorf(t *testing.T) {
	rootCause := errors.New("connection refused")
	wrapped := fmt.Errorf("commit: %w", NewNotEnoughReplicasErrorWithCounts(3, 1, rootCause, 0))

	if !errors.Is(wrapped, ErrReplicas) {
		t.Fatalf("errors.Is(wrapped, ErrReplicas) = false, want true")
	}
	if !errors.Is(wrapped, rootCause) {
		t.Fatalf("errors.Is(wrapped, rootCause) = false, want true")
	}
	if want := "connection refused"; !contains(wrapped.Error(), want) {
		t.Fatalf("Error() = %q, want it to contain %q", wrapped.Error(), want)
	}
}

func TestReadError(t *testing.T) {
	rootCause := errors.New("context deadline exceeded")
	err := NewReadError(rootCause)

	if got, want := err.Error(), ErrRead.Error()+": "+rootCause.Error(); got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}

	if !errors.Is(err, ErrRead) {
		t.Fatalf("errors.Is(err, ErrRead) = false, want true")
	}
	if !errors.Is(err, rootCause) {
		t.Fatalf("errors.Is(err, rootCause) = false, want true")
	}
	if got := errors.Unwrap(err); got != rootCause {
		t.Fatalf("Unwrap() = %v, want %v", got, rootCause)
	}
}

func TestReadError_NilCause(t *testing.T) {
	err := NewReadError(nil)

	if got := err.Error(); got != ErrRead.Error() {
		t.Fatalf("Error() = %q, want %q", got, ErrRead.Error())
	}
	if !errors.Is(err, ErrRead) {
		t.Fatalf("errors.Is(err, ErrRead) = false, want true")
	}
}

func TestRepairError(t *testing.T) {
	rootCause := errors.New("digest mismatch on shard-1")
	err := NewRepairError(rootCause)

	if got, want := err.Error(), ErrRepair.Error()+": "+rootCause.Error(); got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}

	if !errors.Is(err, ErrRepair) {
		t.Fatalf("errors.Is(err, ErrRepair) = false, want true")
	}
	if !errors.Is(err, rootCause) {
		t.Fatalf("errors.Is(err, rootCause) = false, want true")
	}
	if got := errors.Unwrap(err); got != rootCause {
		t.Fatalf("Unwrap() = %v, want %v", got, rootCause)
	}
}

func TestRepairError_NilCause(t *testing.T) {
	err := NewRepairError(nil)

	if got := err.Error(); got != ErrRepair.Error() {
		t.Fatalf("Error() = %q, want %q", got, ErrRepair.Error())
	}
	if !errors.Is(err, ErrRepair) {
		t.Fatalf("errors.Is(err, ErrRepair) = false, want true")
	}
}

// TestErrors_DistinctSentinels ensures the three sentinels do not satisfy
// each other's Is checks (e.g. a read error must not match ErrRepair).
func TestErrors_DistinctSentinels(t *testing.T) {
	cases := []struct {
		name    string
		err     error
		matches error
		other   []error
	}{
		{
			name:    "not enough replicas",
			err:     NewNotEnoughReplicasError(errors.New("x")),
			matches: ErrReplicas,
			other:   []error{ErrRead, ErrRepair},
		},
		{
			name:    "read",
			err:     NewReadError(errors.New("x")),
			matches: ErrRead,
			other:   []error{ErrReplicas, ErrRepair},
		},
		{
			name:    "repair",
			err:     NewRepairError(errors.New("x")),
			matches: ErrRepair,
			other:   []error{ErrReplicas, ErrRead},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !errors.Is(tc.err, tc.matches) {
				t.Fatalf("errors.Is(%v, %v) = false, want true", tc.err, tc.matches)
			}
			for _, o := range tc.other {
				if errors.Is(tc.err, o) {
					t.Fatalf("errors.Is(%v, %v) = true, want false", tc.err, o)
				}
			}
		})
	}
}

func contains(haystack, needle string) bool {
	if len(needle) == 0 {
		return true
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
