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

// budgetCapDisabled is a temporary escape hatch: when set, sroar merge
// concurrency ignores the per-query budget and uses the fixed constant again.
var budgetCapDisabled = entcfg.Enabled(os.Getenv("DISABLE_SROAR_MERGE_BUDGET"))

// BudgetCapDisabled reports whether the sroar merge budget kill switch is set.
func BudgetCapDisabled() bool {
	return budgetCapDisabled
}

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

// BudgetFromCtxCapped returns ctx's per-query budget clamped to [1, limit];
// limit is also the fallback when ctx carries no budget. The floor of 1 is
// load-bearing: sroar's *Conc ops treat maxConcurrency<=0 as "unlimited".
func BudgetFromCtxCapped(ctx context.Context, limit int) int {
	if budgetCapDisabled {
		// even with the cap disabled, keep the floor of 1: a limit of 0 would
		// otherwise mean "unlimited" to sroar's *Conc ops (and hang an
		// errgroup.SetLimit(0)), turning the kill switch into a footgun.
		return max(limit, 1)
	}
	return clampBudget(BudgetFromCtx(ctx, limit), limit)
}

func clampBudget(b, limit int) int {
	if b > limit {
		b = limit
	}
	if b < 1 {
		b = 1
	}
	return b
}
