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

// TracePath identifies a traceable codepath. Used to gate span creation.
type TracePath int

const (
	PathVectorSearch TracePath = iota
)

// Flags controls which codepaths create spans for a given request.
// Injected into context at the request edge, read by StartSpan.
type Flags struct {
	VectorSearch bool
}

type flagsKey struct{}

// WithFlags injects tracing flags into the context.
// Call this once at the request entry point (e.g., Traverser.GetClass).
func WithFlags(ctx context.Context, f Flags) context.Context {
	return context.WithValue(ctx, flagsKey{}, f)
}

func getFlags(ctx context.Context) Flags {
	f, _ := ctx.Value(flagsKey{}).(Flags)
	return f
}

func (f Flags) isEnabled(path TracePath) bool {
	switch path {
	case PathVectorSearch:
		return f.VectorSearch
	default:
		return false
	}
}
