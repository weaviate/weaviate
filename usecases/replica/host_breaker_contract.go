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

package replica

import (
	"context"
	"errors"
)

// The breakers and all their state live in adapters/clients, which already imports this package;
// only the sentinel and the waiver marker the read path needs are here.

// ErrHostCircuitOpen means "skip this replica", never "this read failed"
var ErrHostCircuitOpen = errors.New("replica host marked unhealthy by circuit breaker")

// bypassHostBreakerKey marks a context as a last-resort attempt
type bypassHostBreakerKey struct{}

// WithoutHostBreaker marks ctx as the only replica left to try: the client must contact the host even with its breaker open
func WithoutHostBreaker(ctx context.Context) context.Context {
	return context.WithValue(ctx, bypassHostBreakerKey{}, struct{}{})
}

// HostBreakerBypassed reports whether ctx was marked by WithoutHostBreaker
func HostBreakerBypassed(ctx context.Context) bool {
	return ctx.Value(bypassHostBreakerKey{}) != nil
}
