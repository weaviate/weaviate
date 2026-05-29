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
	"fmt"
	"strings"
	"testing"
)

// TestErrPreconditionFailedWrapChain verifies that both errors.As and errors.Is
// work correctly through a fmt.Errorf("%w", …) wrap chain for the conditional
// write error types. This is the primary contract the rest of the CAS
// implementation relies on: handler layers can unwrap to the sentinel for simple
// branching, while the replicator and REST layers can unwrap to the structured
// type for response payload construction.
func TestErrPreconditionFailedWrapChain(t *testing.T) {
	t.Run("errors.As extracts ErrPreconditionFailed through wrap chain", func(t *testing.T) {
		original := &ErrPreconditionFailed{
			ObjectID:        "00000000-0000-0000-0000-000000000001",
			Reason:          "object already exists",
			ExpectedVersion: 0,
			ActualVersion:   0,
			PredicatePath:   "",
		}
		wrapped := fmt.Errorf("outer context: %w", original)

		var target *ErrPreconditionFailed
		if !errors.As(wrapped, &target) {
			t.Fatal("errors.As did not find *ErrPreconditionFailed through one wrap level")
		}
		if target.ObjectID != original.ObjectID {
			t.Errorf("ObjectID: got %q, want %q", target.ObjectID, original.ObjectID)
		}
		if target.Reason != original.Reason {
			t.Errorf("Reason: got %q, want %q", target.Reason, original.Reason)
		}
	})

	t.Run("errors.Is matches ErrConditionalCheckFailed sentinel through wrap chain", func(t *testing.T) {
		original := &ErrPreconditionFailed{
			ObjectID: "00000000-0000-0000-0000-000000000002",
			Reason:   "version mismatch",
		}
		wrapped := fmt.Errorf("outer context: %w", original)

		if !errors.Is(wrapped, ErrConditionalCheckFailed) {
			t.Fatal("errors.Is did not match ErrConditionalCheckFailed through one wrap level")
		}
	})

	t.Run("errors.Is matches ErrConditionalCheckFailed through two wrap levels", func(t *testing.T) {
		original := &ErrPreconditionFailed{
			ObjectID: "00000000-0000-0000-0000-000000000003",
			Reason:   "field predicate mismatch",
		}
		inner := fmt.Errorf("layer one: %w", original)
		outer := fmt.Errorf("layer two: %w", inner)

		if !errors.Is(outer, ErrConditionalCheckFailed) {
			t.Fatal("errors.Is did not match ErrConditionalCheckFailed through two wrap levels")
		}
	})

	t.Run("errors.As extracts ErrPreconditionFailed through two wrap levels", func(t *testing.T) {
		expectedVersion := uint64(42)
		original := &ErrPreconditionFailed{
			ObjectID:        "00000000-0000-0000-0000-000000000004",
			Reason:          "version mismatch: expected 42, actual 7",
			ExpectedVersion: expectedVersion,
			ActualVersion:   7,
		}
		inner := fmt.Errorf("layer one: %w", original)
		outer := fmt.Errorf("layer two: %w", inner)

		var target *ErrPreconditionFailed
		if !errors.As(outer, &target) {
			t.Fatal("errors.As did not find *ErrPreconditionFailed through two wrap levels")
		}
		if target.ExpectedVersion != expectedVersion {
			t.Errorf("ExpectedVersion: got %d, want %d", target.ExpectedVersion, expectedVersion)
		}
		if target.ActualVersion != 7 {
			t.Errorf("ActualVersion: got %d, want 7", target.ActualVersion)
		}
	})

	t.Run("unrelated error does not match ErrConditionalCheckFailed", func(t *testing.T) {
		unrelated := fmt.Errorf("some other error")
		if errors.Is(unrelated, ErrConditionalCheckFailed) {
			t.Fatal("errors.Is incorrectly matched ErrConditionalCheckFailed for an unrelated error")
		}
	})

	t.Run("ErrPreconditionFailed.Error() contains ObjectID and Reason", func(t *testing.T) {
		e := &ErrPreconditionFailed{
			ObjectID: "my-uuid",
			Reason:   "already exists",
		}
		msg := e.Error()
		if msg == "" {
			t.Fatal("Error() returned empty string")
		}
		if !strings.Contains(msg, "my-uuid") {
			t.Errorf("Error() %q does not contain ObjectID %q", msg, "my-uuid")
		}
		if !strings.Contains(msg, "already exists") {
			t.Errorf("Error() %q does not contain Reason %q", msg, "already exists")
		}
	})
}
