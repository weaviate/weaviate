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
}

func NewSlowQueryDetails() *SlowQueryDetails {
	return &SlowQueryDetails{
		values: make(map[string]any),
	}
}

func InitSlowQueryDetails(ctx context.Context) context.Context {
	d := NewSlowQueryDetails()
	return context.WithValue(ctx, "slow_query_details", d)
}

func AnnotateSlowQueryLog(ctx context.Context, key string, value any) {
	if ctx == nil {
		return
	}
	val := ctx.Value("slow_query_details")
	if val == nil {
		return
	}

	details, ok := val.(*SlowQueryDetails)
	if !ok {
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

// HasSlowQueryDetails reports whether ctx collects slow-query details, so a
// caller can skip building values nothing will read.
func HasSlowQueryDetails(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	_, ok := ctx.Value("slow_query_details").(*SlowQueryDetails)
	return ok
}

// AnnotateSlowQueryLogAppendMany appends values under key in one lock
// acquisition, so a fan-out that resolves many keys does not serialise its
// workers on the details lock. If key already holds a value of another type,
// the values are dropped.
func AnnotateSlowQueryLogAppendMany[T any](ctx context.Context, key string, values []T) {
	if ctx == nil || len(values) == 0 {
		return
	}
	val := ctx.Value("slow_query_details")
	if val == nil {
		return
	}

	details, ok := val.(*SlowQueryDetails)
	if !ok {
		return
	}

	details.Lock()
	defer details.Unlock()

	prev, ok := details.values[key]
	if !ok {
		prev = make([]T, 0, len(values))
	}

	asList, ok := prev.([]T)
	if !ok {
		return
	}

	details.values[key] = append(asList, values...)
}

// AnnotateSlowQueryLogAppendFunc is AnnotateSlowQueryLogAppend with the value
// built lazily: build runs only when the ctx carries slow-query details, so
// callers on hot paths pay nothing to construct a value that would be
// discarded. build is called outside the details lock. A nil build is
// tolerated like every other bad input here: diagnostics never fail loudly.
func AnnotateSlowQueryLogAppendFunc[T any](ctx context.Context, key string, build func() T) {
	if ctx == nil || build == nil {
		return
	}
	val := ctx.Value("slow_query_details")
	if val == nil {
		return
	}

	details, ok := val.(*SlowQueryDetails)
	if !ok {
		return
	}

	value := build()

	details.Lock()
	defer details.Unlock()

	prev, ok := details.values[key]
	if !ok {
		prev = make([]T, 0)
	}

	asList, ok := prev.([]T)
	if !ok {
		return
	}

	asList = append(asList, value)
	details.values[key] = asList
}

// DropSlowQueryEntry removes key, so a caller that decided nothing will read
// the value can stop anything else from logging it. It is cheaper than reducing
// the value, and it closes the window where a reporter switched on after the
// decision picks the raw entries up.
func DropSlowQueryEntry(ctx context.Context, key string) {
	if ctx == nil {
		return
	}
	details, ok := ctx.Value("slow_query_details").(*SlowQueryDetails)
	if !ok {
		return
	}

	details.Lock()
	defer details.Unlock()

	delete(details.values, key)
}

func ReplaceSlowQueryEntry[in any, out any](ctx context.Context, key string, replaceFunc func(old in) out) {
	if ctx == nil {
		return
	}
	val := ctx.Value("slow_query_details")
	if val == nil {
		return
	}

	details, ok := val.(*SlowQueryDetails)
	if !ok {
		return
	}

	details.Lock()
	defer details.Unlock()

	prev, ok := details.values[key]
	if !ok {
		return // nothing to replace
	}

	typed, ok := prev.(in)
	if !ok {
		return
	}

	details.values[key] = replaceFunc(typed)
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

func ExtractSlowQueryDetails(ctx context.Context) map[string]any {
	val := ctx.Value("slow_query_details")
	if val == nil {
		return nil
	}

	details, ok := val.(*SlowQueryDetails)
	if !ok {
		return nil
	}

	details.Lock()
	defer details.Unlock()

	values := maps.Clone(details.values)

	return values
}
