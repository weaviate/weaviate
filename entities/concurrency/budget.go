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
	"os"

	entcfg "github.com/weaviate/weaviate/entities/config"
)

// budgetCapDisabled restores pre-P1a behavior where sroar merge concurrency
// was a fixed constant per call site, ignoring the per-query budget.
// Escape hatch only; to be removed once the fix has soaked.
var budgetCapDisabled = entcfg.Enabled(os.Getenv("DISABLE_SROAR_MERGE_BUDGET"))

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

// BudgetFromCtxCapped returns the per-query concurrency budget from ctx,
// clamped to [1, cap]. cap doubles as the fallback when ctx carries no
// budget (background callers: compaction, cursors, flush), which preserves
// pre-budget behavior exactly. The floor of 1 is load-bearing: sroar's
// *Conc merge ops treat maxConcurrency <= 0 as "unlimited".
func BudgetFromCtxCapped(ctx context.Context, cap int) int {
	if budgetCapDisabled {
		return cap
	}
	return clampBudget(BudgetFromCtx(ctx, cap), cap)
}

func clampBudget(b, cap int) int {
	if b > cap {
		b = cap
	}
	if b < 1 {
		b = 1
	}
	return b
}
