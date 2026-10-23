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
	"sync"
	"sync/atomic"

	entcfg "github.com/weaviate/weaviate/entities/config"
)

// budgetCapDisabled is a temporary escape hatch: when set, sroar merge
// concurrency ignores the per-query budget and uses the fixed constant again.
var budgetCapDisabled = entcfg.Enabled(os.Getenv("DISABLE_SROAR_MERGE_BUDGET"))

// BudgetCapDisabled reports whether the sroar merge budget kill switch is set.
func BudgetCapDisabled() bool {
	return budgetCapDisabled
}

// Pool is where a resizable budget's units come from: the node's query
// admission limiter.
type Pool interface {
	// Resize moves a holding of held units toward n and returns the new holding.
	Resize(held, n int) int
}

// Budget is the per-query worker budget every goroutine of one query reads.
// With a pool it can give units back and take them again. Without one it is
// fixed, since shrinking it would free nothing for anyone.
type Budget struct {
	mu    sync.Mutex // serializes resize and Detach; size never takes it
	pool  Pool
	units atomic.Int64
}

func NewBudget(pool Pool, size int) *Budget {
	b := &Budget{pool: pool}
	b.units.Store(int64(size))
	return b
}

func (b *Budget) size() int {
	return int(b.units.Load())
}

// resize moves the budget toward n and returns its new size. A fixed budget
// keeps its size.
func (b *Budget) resize(n int) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.pool == nil {
		return b.size()
	}
	size := b.pool.Resize(b.size(), n)
	b.units.Store(int64(size))
	return size
}

// Detach makes the budget fixed and returns the units it holds, which the
// caller then owes to the pool.
func (b *Budget) Detach() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pool = nil
	return b.size()
}

type budgetKey struct{}

func (budgetKey) String() string {
	return "concurrency_budget"
}

// CtxWithBudget stores a fixed budget, so a share split off for a child cannot
// resize the budget it was split from.
func CtxWithBudget(ctx context.Context, budget int) context.Context {
	return CtxWithResizableBudget(ctx, NewBudget(nil, budget))
}

func CtxWithResizableBudget(ctx context.Context, b *Budget) context.Context {
	return context.WithValue(ctx, budgetKey{}, b)
}

// CtxWithBudgetIfAbsent seeds a budget only if ctx doesn't already carry one,
// so an upstream admission grant isn't overwritten by a downstream default.
func CtxWithBudgetIfAbsent(ctx context.Context, budget int) context.Context {
	if _, ok := ctx.Value(budgetKey{}).(*Budget); ok {
		return ctx
	}
	return CtxWithBudget(ctx, budget)
}

func BudgetFromCtx(ctx context.Context, fallback int) int {
	b, ok := ctx.Value(budgetKey{}).(*Budget)
	if !ok {
		return fallback
	}

	return b.size()
}

// ResizeBudget resizes ctx's budget toward n and returns its new size, or n
// when ctx carries none.
func ResizeBudget(ctx context.Context, n int) int {
	b, ok := ctx.Value(budgetKey{}).(*Budget)
	if !ok {
		return n
	}
	return b.resize(n)
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
		// even disabled, floor at 1: limit=0 means "unlimited" to sroar's *Conc
		// ops and would hang an error group with SetLimit(0); a kill-switch footgun.
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

// NumWorkers returns the per-query worker count for a fan-out over nItems:
// the context budget when present, capped at fallback and nItems, floored
// at 1 so a zero or negative budget (or nItems of 0) can never silently
// skip the work. Unlike BudgetFromCtxCapped it deliberately does not honor
// the sroar merge kill switch: that escape hatch is scoped to sroar merge
// concurrency, not to vector-index fan-outs.
func NumWorkers(ctx context.Context, nItems, fallback int) int {
	return max(1, min(BudgetFromCtx(ctx, fallback), fallback, nItems))
}
