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

package storobj

import "context"

// conditionalContextKey is the unexported context-key type for the per-request
// Conditional. Using a private type prevents accidental collisions with other
// packages that store values in context.
type conditionalContextKey struct{}

// ContextWithConditional returns a child context that carries the given
// Conditional. Callers at the wire-handler boundary (REST, gRPC) store the
// condition here; the db adapter (crud.go) retrieves it and applies it to the
// storobj.Object immediately after FromObject().
//
// A zero-value Conditional (IsZero() == true) is a no-op: ContextWithConditional
// still stores it, but ConditionalFromContext returns the zero value which the
// shard treats as unconditional.
func ContextWithConditional(ctx context.Context, c Conditional) context.Context {
	return context.WithValue(ctx, conditionalContextKey{}, c)
}

// ConditionalFromContext extracts the Conditional stored by ContextWithConditional.
// If no value was set, it returns the zero Conditional (IsZero() == true),
// preserving the unconditional hot-path behaviour with zero extra allocation.
func ConditionalFromContext(ctx context.Context) Conditional {
	if v := ctx.Value(conditionalContextKey{}); v != nil {
		if c, ok := v.(Conditional); ok {
			return c
		}
	}
	return Conditional{}
}
