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

package concurrency

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBudget(t *testing.T) {
	fallback := 12

	ctx := context.Background()
	budget := BudgetFromCtx(ctx, fallback)
	// no budget set
	assert.Equal(t, fallback, budget)

	// extract previously set budget
	ctx = CtxWithBudget(ctx, 32)
	budget = BudgetFromCtx(ctx, fallback)
	assert.Equal(t, 32, budget)

	// reduce budget by factor
	ctx = ContextWithFractionalBudget(ctx, 2, fallback)
	budget = BudgetFromCtx(ctx, fallback)
	assert.Equal(t, 16, budget)

	// fractional reduction of fallback
	ctx = ContextWithFractionalBudget(context.Background(), 3, fallback)
	budget = BudgetFromCtx(ctx, fallback)
	assert.Equal(t, 4, budget)
}

func TestBudgetFromCtxCapped(t *testing.T) {
	const cap = 8

	tests := []struct {
		name     string
		budget   *int // nil => no budget in ctx
		expected int
	}{
		{name: "absent budget falls back to cap", budget: nil, expected: cap},
		{name: "budget above cap is clamped to cap", budget: ptr(cap + 5), expected: cap},
		{name: "budget below cap is preserved", budget: ptr(3), expected: 3},
		{name: "zero budget floored to 1", budget: ptr(0), expected: 1},
		{name: "negative budget floored to 1", budget: ptr(-4), expected: 1},
		{name: "budget equal to cap", budget: ptr(cap), expected: cap},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.budget != nil {
				ctx = CtxWithBudget(ctx, *tt.budget)
			}
			assert.Equal(t, tt.expected, BudgetFromCtxCapped(ctx, cap))
		})
	}

	t.Run("cap of 1 always yields 1", func(t *testing.T) {
		assert.Equal(t, 1, BudgetFromCtxCapped(context.Background(), 1))
		assert.Equal(t, 1, BudgetFromCtxCapped(CtxWithBudget(context.Background(), 16), 1))
	})
}

func TestClampBudget(t *testing.T) {
	tests := []struct {
		name     string
		b        int
		cap      int
		expected int
	}{
		{name: "above cap branch", b: 20, cap: 8, expected: 8},
		{name: "below floor branch", b: 0, cap: 8, expected: 1},
		{name: "in range unchanged", b: 5, cap: 8, expected: 5},
		{name: "at cap", b: 8, cap: 8, expected: 8},
		{name: "at floor", b: 1, cap: 8, expected: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, clampBudget(tt.b, tt.cap))
		})
	}
}

func ptr(i int) *int { return &i }
