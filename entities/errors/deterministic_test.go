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

	pkgerrors "github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/storagestate"
)

// TestDeterministic pins the marker contract of the typed error taxonomy
// (structural — the API is new): the mark is visible through wrapping, never
// alters the message, and leaves the original chain reachable.
func TestDeterministic(t *testing.T) {
	base := errors.New("invalid vector length 3, expected 128")

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "unmarked error", err: base, want: false},
		{name: "marked error", err: Deterministic(base), want: true},
		{name: "marked then fmt-wrapped", err: fmt.Errorf("apply: %w", Deterministic(base)), want: true},
		{name: "marked then pkg/errors-wrapped", err: pkgerrors.Wrap(Deterministic(base), "validate"), want: true},
		{name: "mark around a pkg/errors chain", err: Deterministic(pkgerrors.Wrap(base, "validate")), want: true},
		{
			// The park polarity depends on this: an environmental error must
			// never classify deterministic just because it sits near one.
			name: "unmarked read-only flip",
			err:  storagestate.ErrStatusReadOnlyWithReason("resource pressure"),
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsDeterministic(tc.err))
		})
	}

	t.Run("nil stays nil", func(t *testing.T) {
		assert.NoError(t, Deterministic(nil))
	})

	t.Run("message text is unchanged", func(t *testing.T) {
		assert.Equal(t, base.Error(), Deterministic(base).Error())
	})

	t.Run("original chain stays reachable", func(t *testing.T) {
		inner := storagestate.ErrStatusReadOnly
		marked := Deterministic(fmt.Errorf("outer: %w", inner))
		assert.ErrorIs(t, marked, inner)
	})
}
