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

package tracing

import "context"

// Area identifies a traceable codepath. It is a string so the same value can
// double as the runtime-config key, the env-var stem, and a span attribute.
// Adding an area touches only this const block and the single place that builds
// Flags — never the call sites.
type Area string

const (
	AreaVector Area = "vector"
	AreaBM25   Area = "bm25"
	AreaHybrid Area = "hybrid"
)

// Flags controls which areas create spans for a single request. It is built
// once at the request edge (Traverser.GetClass) and read-only below.
//
// Flags carries a map by reference, so copying the struct shares the map. This
// is safe because the map is never mutated after WithFlags injects it.
type Flags struct {
	areas    map[Area]bool
	forceAll bool
}

// NewFlags builds a Flags from a per-area enabled map. A nil map is allowed and
// yields a Flags that enables nothing (unless WithForceAll is applied).
func NewFlags(areas map[Area]bool) Flags {
	return Flags{areas: areas}
}

// WithForceAll returns a copy of f with every area forced on. Used by the
// per-request toggle, which ORs over the runtime per-area toggles.
func WithForceAll(f Flags) Flags {
	f.forceAll = true
	return f
}

// IsEnabled reports whether the given area should emit spans. forceAll wins over
// the per-area map; an unknown or zero Area returns false.
func (f Flags) IsEnabled(a Area) bool {
	return f.forceAll || f.areas[a]
}

// anyEnabled reports whether any area is enabled or forceAll is set. Used to
// gate the root span without coupling it to one specific area.
func (f Flags) anyEnabled() bool {
	if f.forceAll {
		return true
	}
	for _, on := range f.areas {
		if on {
			return true
		}
	}
	return false
}

type flagsKey struct{}

// WithFlags injects tracing flags into the context. Call this once at the
// request entry point (e.g. Traverser.GetClass); every StartSpan below reads it.
func WithFlags(ctx context.Context, f Flags) context.Context {
	return context.WithValue(ctx, flagsKey{}, f)
}

func flagsFromContext(ctx context.Context) (Flags, bool) {
	f, ok := ctx.Value(flagsKey{}).(Flags)
	return f, ok
}
