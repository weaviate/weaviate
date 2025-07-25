//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package concurrency

import (
	"context"
)

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
