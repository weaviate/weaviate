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
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFlushAndSwitch_RetriesLeftoverFlushingMemtable pins the b.flushing
// clobber fix end to end with a REAL failed flush: mt.flush() closes the
// commitlog before writing the segment, so a failure past that point (here: a
// read-only bucket directory) leaves a memtable in b.flushing whose commitlog
// is already closed. The next switch used to overwrite the field — orphaning
// acknowledged, fsynced writes until a restart's WAL replay, and, mid-drop,
// leaving pre-arm bytes outside every snapshot. The next flush attempt must
// instead COMPLETE the leftover flush (which requires commitLogger.close and
// delete to be re-entrant) and then proceed with its own.
func TestFlushAndSwitch_RetriesLeftoverFlushingMemtable(t *testing.T) {
	bucket, _ := newReplaceBucketWithEditOps(t, prefixTransformer)
	dir := bucket.GetDir()

	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))

	// Stage k1 into flushing while the directory is still writable (the
	// switch creates the NEW active memtable's commitlog file), then make the
	// segment write fail.
	switched, err := bucket.atomicallySwitchMemtable(bucket.createNewActiveMemtable)
	require.NoError(t, err)
	require.True(t, switched)
	require.NoError(t, os.Chmod(dir, 0o555))
	restore := func() { require.NoError(t, os.Chmod(dir, 0o755)) }
	defer restore()

	// A real failed flush: commitlog closed, segment write denied.
	require.Error(t, bucket.FlushAndSwitch())
	require.NotNil(t, bucket.flushing, "the failed flush's memtable must stay in place, not be discarded")

	v, err := bucket.Get([]byte("k1"))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), v, "acknowledged writes stay readable through the leftover memtable")

	// A direct switch over the leftover must refuse — the structural guard
	// that makes the clobber impossible even if a future caller bypasses
	// flushAndSwitchLocked's drain.
	_, err = bucket.atomicallySwitchMemtable(bucket.createNewActiveMemtable)
	require.ErrorContains(t, err, "refusing to overwrite")

	// Cause gone: the next flush completes the leftover (re-entering its
	// closed commitlog), then flushes its own memtable.
	restore()
	require.NoError(t, bucket.Put([]byte("k2"), []byte("v2")))
	require.NoError(t, bucket.FlushAndSwitch())
	require.Nil(t, bucket.flushing)
	require.Len(t, segIDsOf(bucket), 2, "both the leftover and the new memtable must land in segments")

	v, err = bucket.Get([]byte("k1"))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), v, "the failed flush's acknowledged writes must survive the retry cycle")
	v, err = bucket.Get([]byte("k2"))
	require.NoError(t, err)
	require.Equal(t, []byte("v2"), v)
}
