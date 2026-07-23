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

package backup

import (
	"strconv"
	"sync/atomic"
)

// Op identifies one runtime instance of a backup operation. Backup IDs are
// user-supplied and reusable across a cancel→retry, so the ID alone cannot
// scope a release; Fence distinguishes the instance. A release carrying an Op
// resumes only the shard halts that same Op placed, and a stale Op's release
// is inert against an identically-named successor (its halt keys don't match
// and the admission-gate clear is guarded on Op equality) — a fencing token.
//
// Mint via NewOp exactly once per operation, at the point that owns the
// operation's lifecycle (the uploader); construct literals only in tests.
// Process-local: never serialized, never crosses a node boundary.
type Op struct {
	ID    string // user-supplied backup id
	Fence string // process-unique per-instance token
}

var opFence atomic.Uint64

// NewOp mints an operation identity with a fresh process-unique Fence.
func NewOp(id string) Op {
	return Op{ID: id, Fence: strconv.FormatUint(opFence.Add(1), 10)}
}

// HaltOwner is the shard-halt owner key this operation halts and resumes
// under. Namespaced "backup:" — see the owner-key scheme at the builders in
// adapters/repos/db/shard_backup.go, which this must stay disjoint from.
func (o Op) HaltOwner() string { return "backup:" + o.ID + ":" + o.Fence }
