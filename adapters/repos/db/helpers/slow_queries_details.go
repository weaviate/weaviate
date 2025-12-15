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
		prev = make([]T, 0)
	}

	asList, ok := prev.([]T)
	if !ok {
		return
	}

	asList = append(asList, value)
	details.values[key] = asList
}

func ReplaceSlowQueryEntry[in any, out any](ctx context.Context, key string, replaceFunc func(old in) out) {
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
