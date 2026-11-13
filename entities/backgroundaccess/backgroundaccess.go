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

// Package backgroundaccess marks a context as background work, whose shard
// pins never count as access and which leaves an unloaded shard unloaded.
package backgroundaccess

import (
	"context"
	"net/http"
)

// Header carries the mark on internal REST requests; a receiver that does not know it serves the request as a user request.
const Header = "X-Weaviate-Background-Access"

// MetadataKey carries the mark on internal gRPC requests, with the same fallback.
const MetadataKey = "x-weaviate-background-access"

const marked = "true"

type key struct{}

// With marks ctx as background work.
func With(ctx context.Context) context.Context {
	return context.WithValue(ctx, key{}, struct{}{})
}

// Is reports whether ctx was marked by With.
func Is(ctx context.Context) bool {
	return ctx.Value(key{}) != nil
}

// SetHeader copies the mark of req's context onto req.
func SetHeader(req *http.Request) {
	if Is(req.Context()) {
		req.Header.Set(Header, marked)
	}
}

// FromHeader marks ctx when h carries the mark.
func FromHeader(ctx context.Context, h http.Header) context.Context {
	return FromValue(ctx, h.Get(Header))
}

// FromValue marks ctx when v is the mark's wire value.
func FromValue(ctx context.Context, v string) context.Context {
	if v == marked {
		return With(ctx)
	}
	return ctx
}

// Value is the mark's wire value for ctx, empty when unmarked.
func Value(ctx context.Context) string {
	if Is(ctx) {
		return marked
	}
	return ""
}
