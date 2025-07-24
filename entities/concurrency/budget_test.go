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
