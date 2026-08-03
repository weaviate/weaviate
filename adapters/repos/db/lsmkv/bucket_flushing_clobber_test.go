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

package lsmkv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFlushAndSwitch_DoesNotClobberFailedFlushingMemtable pins a KNOWN
// pre-existing bug (present on main, independent of drop-vector; skipped
// until the upstream fix lands — it needs its own change to the core flush
// machinery): a failed cycle flush leaves its memtable in b.flushing
// (flushAndSwitchLocked returns mid-way and nothing retries that memtable),
// and the next atomicallySwitchMemtable overwrites the field unconditionally.
// The orphaned memtable's acknowledged, fsynced writes become unreadable
// until a restart replays its WAL. During a drop the orphan's WAL can hold
// pre-arm bytes; without a restart before finalize nothing ever strips them —
// the WAL-recovery pend only helps when a restart happens while the op still
// exists. Desired behavior, asserted below: a switch must never discard a
// non-nil b.flushing (drain it first, or refuse and retry).
func TestFlushAndSwitch_DoesNotClobberFailedFlushingMemtable(t *testing.T) {
	t.Skip("pins the known b.flushing clobber — pre-existing on main; unskip with the upstream flush-machinery fix")

	bucket, _ := newReplaceBucketWithEditOps(t, prefixTransformer)
	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))

	// A failed flush's aftermath: memtable switched into flushing, the flush
	// itself never completed, nothing cleared the field.
	switched, err := bucket.atomicallySwitchMemtable(bucket.createNewActiveMemtable)
	require.NoError(t, err)
	require.True(t, switched)
	require.NotNil(t, bucket.flushing)

	// New writes land in the fresh active memtable, and the next threshold
	// crossing (or a drop's arm) switches again.
	require.NoError(t, bucket.Put([]byte("k2"), []byte("v2")))
	_, err = bucket.atomicallySwitchMemtable(bucket.createNewActiveMemtable)
	require.NoError(t, err)

	v, err := bucket.Get([]byte("k1"))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), v,
		"acknowledged writes must stay readable — the failed flush's memtable may not be silently orphaned")
}
