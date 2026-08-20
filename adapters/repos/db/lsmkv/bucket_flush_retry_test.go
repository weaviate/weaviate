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
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/cyclemanager"
)

var errInjectedFlushFailure = errors.New("injected flush failure")

// failNFlushesMemtable wraps a real memtable and makes its next N flush()
// calls fail, so tests can drive a real Bucket through a genuine failed
// flush without faking the whole memtable.
type failNFlushesMemtable struct {
	memtable
	failN int
}

func (m *failNFlushesMemtable) flush() (string, error) {
	if m.failN > 0 {
		m.failN--
		return "", errInjectedFlushFailure
	}
	return m.memtable.flush()
}

func newFlushRetryTestBucket(t *testing.T, dir string) *Bucket {
	t.Helper()
	noopCB := cyclemanager.NewCallbackGroupNoop()
	logger, _ := test.NewNullLogger()
	b, err := NewBucketCreator().NewBucket(context.Background(), dir, "", logger, nil,
		noopCB, noopCB, WithStrategy(StrategyReplace))
	require.NoError(t, err)
	return b
}

// TestFlushAndSwitchRetainsFailedFlush pins the core bug: a memtable that
// failed to flush must be retried by the next FlushAndSwitch call, not
// discarded by it. Before the fix, atomicallySwitchMemtable overwrites
// b.flushing unconditionally, so the second FlushAndSwitch call below drops
// row A's memtable on the floor: it was never added to a disk segment, and
// it's no longer reachable from b.active or b.flushing, so every reader
// stops seeing it until the WAL is replayed on restart.
func TestFlushAndSwitchRetainsFailedFlush(t *testing.T) {
	ctx := context.Background()
	b := newFlushRetryTestBucket(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, b.Shutdown(ctx)) })

	require.NoError(t, b.Put([]byte("A"), []byte("vA")))

	// Wrap the memtable that already holds row A so its flush fails once.
	b.active = &failNFlushesMemtable{memtable: b.active, failN: 1}

	err := b.FlushAndSwitch()
	require.Error(t, err)

	vA, err := b.Get([]byte("A"))
	require.NoError(t, err)
	require.Equal(t, []byte("vA"), vA)

	// Goes to the real active memtable created by the failed attempt's
	// switch; only the first memtable was wrapped.
	require.NoError(t, b.Put([]byte("B"), []byte("vB")))

	require.NoError(t, b.FlushAndSwitch())

	vA, err = b.Get([]byte("A"))
	require.NoError(t, err)
	require.Equal(t, []byte("vA"), vA)

	vB, err := b.Get([]byte("B"))
	require.NoError(t, err)
	require.Equal(t, []byte("vB"), vB)

	require.Nil(t, b.flushing)
}

// TestFlushAndSwitchRetryAfterPermissionFailure drives a real memtable
// through a flush failure that happens after its commit log has already
// been closed (the tmp-segment OpenFile call, once the bucket directory is
// unwritable). The retry re-enters Memtable.flush() on the same memtable,
// which re-closes the already-closed commit log; without idempotent close()
// that second close fails on the closed file descriptor.
func TestFlushAndSwitchRetryAfterPermissionFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root bypasses directory permission checks")
	}

	ctx := context.Background()
	dir := t.TempDir()
	b := newFlushRetryTestBucket(t, dir)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(ctx)) })

	require.NoError(t, b.Put([]byte("key"), []byte("value")))

	require.NoError(t, os.Chmod(dir, 0o500))
	t.Cleanup(func() { require.NoError(t, os.Chmod(dir, 0o700)) })

	require.Error(t, b.FlushAndSwitch())

	require.NoError(t, os.Chmod(dir, 0o700))

	require.NoError(t, b.FlushAndSwitch())

	require.Nil(t, b.flushing)
	require.Zero(t, b.active.Size())

	v, err := b.Get([]byte("key"))
	require.NoError(t, err)
	require.Equal(t, []byte("value"), v)
}

// TestCommitLoggerCloseDeleteIdempotent covers the idempotency that a
// retried Memtable.flush() relies on directly: a second close() or delete()
// call, on a commit log the first flush attempt already closed/deleted,
// must be a no-op rather than an error.
func TestCommitLoggerCloseDeleteIdempotent(t *testing.T) {
	newLogger := func(t *testing.T) *commitLogger {
		t.Helper()
		path := filepath.Join(t.TempDir(), "segment")
		cl, err := newCommitLogger(path, StrategyReplace, 0)
		require.NoError(t, err)
		return cl
	}

	tests := []struct {
		name string
		run  func(t *testing.T, cl *commitLogger)
	}{
		{
			name: "close twice",
			run: func(t *testing.T, cl *commitLogger) {
				require.NoError(t, cl.close())
				require.NoError(t, cl.close())
			},
		},
		{
			name: "delete twice",
			run: func(t *testing.T, cl *commitLogger) {
				require.NoError(t, cl.delete())
				require.NoError(t, cl.delete())
			},
		},
		{
			name: "close then delete",
			run: func(t *testing.T, cl *commitLogger) {
				require.NoError(t, cl.close())
				require.NoError(t, cl.delete())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl := newLogger(t)
			tt.run(t, cl)
		})
	}
}

// TestFlushAndSwitchIfThresholdsMetRetriesPendingFlush covers the trigger
// gap: once a flush fails and leaves b.flushing retained, the periodic
// flush-cycle callback must keep retrying it even while the new active
// memtable is quiet and would otherwise never cross a threshold.
func TestFlushAndSwitchIfThresholdsMetRetriesPendingFlush(t *testing.T) {
	ctx := context.Background()
	b := newFlushRetryTestBucket(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, b.Shutdown(ctx)) })

	require.NoError(t, b.Put([]byte("A"), []byte("vA")))

	b.active = &failNFlushesMemtable{memtable: b.active, failN: 1}

	require.Error(t, b.FlushAndSwitch())
	require.NotNil(t, b.flushing)

	// The new active memtable is fresh and empty: every threshold is false,
	// so only the retained-flush check can drive a retry here.
	noAbort := func() bool { return false }
	require.True(t, b.flushAndSwitchIfThresholdsMet(noAbort))

	require.Nil(t, b.flushing)

	v, err := b.Get([]byte("A"))
	require.NoError(t, err)
	require.Equal(t, []byte("vA"), v)
}
