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

package errors

import (
	"context"

	"github.com/pkg/errors"
)

// ErrShardRecovering: shard data is missing locally and being copied
// from a peer (SELF_RECOVERY). Defense-in-depth for callers that
// bypass the router (which already filters via the replication FSM).
var ErrShardRecovering = errors.New("shard recovering from peer")

func IsShardRecovering(err error) bool {
	return errors.Is(err, ErrShardRecovering)
}

// startupDBLoadKey marks a context as originating from the one-shot
// startup DB-load pass (reloadDBFromSchema -> ReloadLocalDB), which runs
// after the node has caught its schema up to the cluster — whether that
// catch-up came from a RAFT snapshot install or from replaying committed
// log entries. It distinguishes "loading a shard the schema says should
// already exist" (a SELF_RECOVERY candidate when the on-disk dir is
// missing) from "creating a brand-new shard at runtime" (where a missing
// dir is normal).
type startupDBLoadKey struct{}

// WithStartupDBLoad tags the ctx as the startup DB-load pass.
func WithStartupDBLoad(ctx context.Context) context.Context {
	return context.WithValue(ctx, startupDBLoadKey{}, true)
}

// IsStartupDBLoad reports whether ctx was tagged via WithStartupDBLoad.
func IsStartupDBLoad(ctx context.Context) bool {
	v, _ := ctx.Value(startupDBLoadKey{}).(bool)
	return v
}
