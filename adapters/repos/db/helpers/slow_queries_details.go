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

package helpers

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"time"
)

type SlowQueryDetails struct {
	sync.Mutex
	values map[string]any
	// reducers, set by AnnotateSlowQueryLogAppendReducible, summarize a key's list.
	reducers map[string]func(any) any
}

func NewSlowQueryDetails() *SlowQueryDetails {
	return &SlowQueryDetails{
		values:   make(map[string]any),
		reducers: make(map[string]func(any) any),
	}
}

func InitSlowQueryDetails(ctx context.Context) context.Context {
	d := NewSlowQueryDetails()
	return context.WithValue(ctx, "slow_query_details", d)
}

func slowQueryDetailsFromContext(ctx context.Context) *SlowQueryDetails {
	if ctx == nil {
		return nil
	}
	details, _ := ctx.Value("slow_query_details").(*SlowQueryDetails)
	return details
}

func AnnotateSlowQueryLog(ctx context.Context, key string, value any) {
	details := slowQueryDetailsFromContext(ctx)
	if details == nil {
		return
	}

	details.Lock()
	defer details.Unlock()

	details.values[key] = value

	if asTime, ok := value.(time.Duration); ok {
		details.values[key+"_string"] = asTime.String()
	}
}

func AnnotateSlowQueryLogAppend[T any](ctx context.Context, key string, value T) {
	AnnotateSlowQueryLogAppendFunc(ctx, key, func() T { return value })
}

// AnnotateSlowQueryLogAppendFunc is AnnotateSlowQueryLogAppend with the value
// built lazily: build runs only when the ctx carries slow-query details, so
// callers on hot paths pay nothing to construct a value that would be
// discarded. build is called outside the details lock. A nil build is
// tolerated like every other bad input here: diagnostics never fail loudly.
func AnnotateSlowQueryLogAppendFunc[T any](ctx context.Context, key string, build func() T) {
	details := slowQueryDetailsFromContext(ctx)
	if details == nil || build == nil {
		return
	}

	value := build()

	details.Lock()
	defer details.Unlock()

	appendValueLocked(details, key, value)
}

// AnnotateSlowQueryLogAppendReducible appends value under key and registers
// reduce for ExtractSlowQueryDetails. reduce runs without details.Lock, so it
// must not retain or mutate its input.
func AnnotateSlowQueryLogAppendReducible[T any, R any](ctx context.Context, key string,
	value T, reduce func([]T) R,
) {
	details := slowQueryDetailsFromContext(ctx)
	if details == nil || reduce == nil {
		return
	}

	details.Lock()
	defer details.Unlock()

	if !appendValueLocked(details, key, value) {
		return
	}

	registerReducerLocked(details, key, reduce)
}

// AnnotateSlowQueryLogAppendManyReducible is AnnotateSlowQueryLogAppendReducible
// for several values in one lock acquisition, so a fan-out that resolves many
// keys does not serialise its workers on the details lock.
func AnnotateSlowQueryLogAppendManyReducible[T any, R any](ctx context.Context, key string,
	values []T, reduce func([]T) R,
) {
	details := slowQueryDetailsFromContext(ctx)
	if details == nil || reduce == nil || len(values) == 0 {
		return
	}

	details.Lock()
	defer details.Unlock()

	if !appendValuesLocked(details, key, values) {
		return
	}

	registerReducerLocked(details, key, reduce)
}

// HasSlowQueryDetails reports whether ctx collects slow-query details, so a
// caller can skip building values nothing will read.
func HasSlowQueryDetails(ctx context.Context) bool {
	return slowQueryDetailsFromContext(ctx) != nil
}

// registerReducerLocked sets reduce as the reducer for key unless it has one.
func registerReducerLocked[T any, R any](details *SlowQueryDetails, key string, reduce func([]T) R) {
	if _, ok := details.reducers[key]; ok {
		return
	}
	details.reducers[key] = func(list any) any {
		typed, ok := list.([]T)
		if !ok {
			return list
		}
		return reduce(typed)
	}
}

// appendValueLocked returns false when key holds a list of another element type.
func appendValueLocked[T any](details *SlowQueryDetails, key string, value T) bool {
	prev, ok := details.values[key]
	if !ok {
		prev = make([]T, 0)
	}

	asList, ok := prev.([]T)
	if !ok {
		return false
	}

	details.values[key] = append(asList, value)
	return true
}

// appendValuesLocked is appendValueLocked for several values.
func appendValuesLocked[T any](details *SlowQueryDetails, key string, values []T) bool {
	prev, ok := details.values[key]
	if !ok {
		prev = make([]T, 0, len(values))
	}

	asList, ok := prev.([]T)
	if !ok {
		return false
	}

	details.values[key] = append(asList, values...)
	return true
}

func SprintfWithNesting(nesting int, format string, args ...any) string {
	nestingPrefix := "  "
	listItem := " - "
	prefix := ""
	for i := 0; i < nesting; i++ {
		prefix += nestingPrefix
	}
	prefix += listItem
	return fmt.Sprintf("%s%s", prefix, fmt.Sprintf(format, args...))
}

// ExtractSlowQueryDetails reduces a copy and leaves the stored lists intact,
// because a query that logs and reports a profile extracts twice.
func ExtractSlowQueryDetails(ctx context.Context) map[string]any {
	details := slowQueryDetailsFromContext(ctx)
	if details == nil {
		return nil
	}

	details.Lock()
	values := maps.Clone(details.values)
	reducers := maps.Clone(details.reducers)
	details.Unlock()

	for key, reduce := range reducers {
		if value, ok := values[key]; ok {
			values[key] = reduce(value)
		}
	}

	return values
}
