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

import "context"

// MaxQueryDedupeTokenLen bounds an untrusted peer's token so it can't become an
// oversized map key on the data node.
const MaxQueryDedupeTokenLen = 64

// QueryDedupeTokenHeader carries the token to remote shards. A node that
// doesn't know it ignores it, so mixed versions need no negotiation.
const QueryDedupeTokenHeader = "X-Query-Dedupe-Token"

type queryDedupeTokenKey struct{}

// CtxWithQueryDedupeToken tags ctx as one query so shards can coalesce
// duplicate sub-query work (e.g. hybrid search's dense and sparse legs). An
// empty or over-long token is ignored, falling back to per-call behaviour.
func CtxWithQueryDedupeToken(ctx context.Context, token string) context.Context {
	if token == "" || len(token) > MaxQueryDedupeTokenLen {
		return ctx
	}
	return context.WithValue(ctx, queryDedupeTokenKey{}, token)
}

// QueryDedupeToken returns the token set by CtxWithQueryDedupeToken, or "".
func QueryDedupeToken(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	token, _ := ctx.Value(queryDedupeTokenKey{}).(string)
	return token
}
