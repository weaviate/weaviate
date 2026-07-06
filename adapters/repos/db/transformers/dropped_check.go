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

package transformers

import "sync"

// DroppedTargetCheck reports whether targetVector on className is still marked
// dropped in the schema. It is the transformer's own last-line guard: every
// upstream layer (enqueue/arm-time validation, op deletion at completion, orphan
// sweeps) can be defeated by a failure window — e.g. a bolt delete error on a
// loaded shard while the task still finalizes — leaving an armed op behind. With
// this check the armed orphan degrades to a no-op instead of stripping a
// re-created same-name vector at the moment of destruction.
//
// Reading the LOCAL schema is sound here: an op in a shard's sidecar implies this
// node applied the task, which implies it applied the earlier marker commit — so
// a locally-live entry genuinely means the name was re-created (or finalized and
// gone), never a lagging pre-drop view.
type DroppedTargetCheck func(className, targetVector string) bool

var (
	droppedCheckMu sync.RWMutex
	droppedCheck   DroppedTargetCheck
)

// SetDroppedTargetCheck installs the schema-backed check. Like the registry it is
// package-level so per-bucket wiring stays free of task-specific plumbing; nil
// (the default, and in unit tests) disables the guard and ops strip unfiltered.
func SetDroppedTargetCheck(fn DroppedTargetCheck) {
	droppedCheckMu.Lock()
	droppedCheck = fn
	droppedCheckMu.Unlock()
}

// stillDropped applies the installed check; without one every target passes.
func stillDropped(className, targetVector string) bool {
	droppedCheckMu.RLock()
	fn := droppedCheck
	droppedCheckMu.RUnlock()
	if fn == nil {
		return true
	}
	return fn(className, targetVector)
}
