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

// TestFlushAndSwitch_RetriesLeftoverFlushingMemtable pins the b.flushing
// clobber fix: a failed cycle flush leaves its memtable in b.flushing
// (flushAndSwitchLocked returned mid-way, nothing else retries it), and the
// next switch used to overwrite the field unconditionally — orphaning
// acknowledged, fsynced writes until a restart's WAL replay, and, mid-drop,
// leaving pre-arm bytes outside every snapshot (resurrection after finalize
// if no restart intervened). The next flush attempt must instead complete the
// leftover flush first, then proceed with its own.
func TestFlushAndSwitch_RetriesLeftoverFlushingMemtable(t *testing.T) {
	bucket, _ := newReplaceBucketWithEditOps(t, prefixTransformer)
	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))

	// Aftermath of a failed flush: the memtable was switched into flushing,
	// the flush itself never completed, and nothing cleared the field.
	switched, err := bucket.atomicallySwitchMemtable(bucket.createNewActiveMemtable)
	require.NoError(t, err)
	require.True(t, switched)
	require.NotNil(t, bucket.flushing)

	// A direct switch over the leftover must refuse — the structural guard
	// that makes the clobber impossible even if a future caller bypasses
	// flushAndSwitchLocked's drain.
	_, err = bucket.atomicallySwitchMemtable(bucket.createNewActiveMemtable)
	require.ErrorContains(t, err, "refusing to overwrite")

	// The next real flush drains the leftover first, then flushes its own.
	require.NoError(t, bucket.Put([]byte("k2"), []byte("v2")))
	require.NoError(t, bucket.FlushAndSwitch())
	require.Nil(t, bucket.flushing)
	require.Len(t, segIDsOf(bucket), 2, "both the leftover and the new memtable must land in segments")

	v, err := bucket.Get([]byte("k1"))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), v, "the failed flush's acknowledged writes must survive the next flush cycle")
	v, err = bucket.Get([]byte("k2"))
	require.NoError(t, err)
	require.Equal(t, []byte("v2"), v)
}
