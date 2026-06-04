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

// Package restcompat synthesises legacy fields on REST responses only.
// Internal JSON paths (RAFT, snapshots, backups) keep using the bare
// entities/models types.
package restcompat

import "sync/atomic"

// asyncReplicationGloballyDisabled mirrors ASYNC_REPLICATION_DISABLED so the
// producer can derive the legacy replicationConfig.asyncEnabled field.
var asyncReplicationGloballyDisabled atomic.Bool

// SetAsyncReplicationGloballyDisabled must be called at startup and from the
// AsyncReplicationDisabled runtime-config hook.
func SetAsyncReplicationGloballyDisabled(disabled bool) {
	asyncReplicationGloballyDisabled.Store(disabled)
}
