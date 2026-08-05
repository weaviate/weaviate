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

package objects

import "context"

type lwwReplayGuardCtxKey struct{}

// WithLWWReplayGuard marks a write as an at-least-once (re)apply — async-
// replication change-log replay, or a RAFT FSM apply that may be a retried
// or re-delivered command. Guarded writes are timestamp-arbitrated at the
// shard: a put or delete strictly OLDER than the locally stored object (or
// tombstone) is dropped instead of clobbering the newer state. The
// comparison runs against local state under the shard's per-doc lock, so
// replicas applying the same sequence reach the same outcome.
func WithLWWReplayGuard(ctx context.Context) context.Context {
	return context.WithValue(ctx, lwwReplayGuardCtxKey{}, true)
}

// HasLWWReplayGuard reports whether ctx marks a LWW-guarded (re)apply.
func HasLWWReplayGuard(ctx context.Context) bool {
	v, _ := ctx.Value(lwwReplayGuardCtxKey{}).(bool)
	return v
}
