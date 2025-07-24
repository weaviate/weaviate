package concurrency

import "context"

type budgetKey struct{}

func (budgetKey) String() string {
	return "concurrency_budget"
}

func CtxWithBudget(ctx context.Context, budget int) context.Context {
	return context.WithValue(ctx, budgetKey{}, budget)
}

func BudgetFromCtx(ctx context.Context, fallback int) int {
	budget, ok := ctx.Value(budgetKey{}).(int)
	if !ok {
		return fallback
	}

	return budget
}

func ContextWithFractionalBudget(ctx context.Context, factor, fallback int) context.Context {
	budget := BudgetFromCtx(ctx, fallback)
	newBudget := FractionOf(budget, factor)

	return CtxWithBudget(ctx, newBudget)
}
