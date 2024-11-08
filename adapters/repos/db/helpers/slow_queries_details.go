package helpers

import (
	"context"
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
