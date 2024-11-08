package helpers

import (
	"context"
	"fmt"
	"maps"
	"sync"
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

func WriteSlowQueryDetail(ctx context.Context, key string, value any) {
	val := ctx.Value("slow_query_details")
	if val == nil {
		fmt.Printf("slow_query_details is nil\n")
		return
	}

	details, ok := val.(*SlowQueryDetails)
	if !ok {
		fmt.Printf("slow_query_details is not *SlowQueryDetails\n")
		return
	}

	details.Lock()
	defer details.Unlock()

	details.values[key] = value
}

func ExtractSlowQueryDetails(ctx context.Context) map[string]any {
	val := ctx.Value("slow_query_details")
	if val == nil {
		fmt.Printf("extract: slow_query_details is nil\n")
		return nil
	}

	details, ok := val.(*SlowQueryDetails)
	if !ok {
		fmt.Printf("extract: slow_query_details is not *SlowQueryDetails\n")
		return nil
	}

	details.Lock()
	defer details.Unlock()

	values := maps.Clone(details.values)
	fmt.Printf("extract: %v\n", values)

	return values
}
