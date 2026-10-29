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
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// failingCloseCommitLog fails the close that starts a flush. With closeFirst the
// commit log is durable before the failure, otherwise its buffered writes never
// reach the file, the way a write error on a full disk leaves it.
type failingCloseCommitLog struct {
	memtableCommitLogger
	closeFirst bool
}

func (c failingCloseCommitLog) close() error {
	if c.closeFirst {
		_ = c.memtableCommitLogger.close()
	}
	return errors.New("no space left on device")
}

// A flush that failed leaves its memtable flushing, and nothing clears it. Shutdown
// must not wait for it, and must either persist its data or report that it could not.
func TestBucket_ShutdownAfterFailedFlush(t *testing.T) {
	tests := []struct {
		name          string
		closeFirst    bool
		readOnlyDir   bool
		expectPersist bool
	}{
		{name: "commit log durable", closeFirst: true, expectPersist: true},
		{name: "commit log missing writes", closeFirst: false, expectPersist: true},
		{name: "segment cannot be written", closeFirst: false, readOnlyDir: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.readOnlyDir && os.Geteuid() == 0 {
				t.Skip("root ignores directory permissions")
			}

			ctx := context.Background()
			dir := t.TempDir()

			b := openReplaceBucket(t, dir)
			require.NoError(t, b.Put([]byte("key"), []byte("value")))
			active := b.active.(*Memtable)
			active.commitlog = failingCloseCommitLog{active.commitlog, tt.closeFirst}
			require.Error(t, b.FlushAndSwitch())
			require.NotNil(t, b.flushing)

			if tt.readOnlyDir {
				require.NoError(t, os.Chmod(dir, 0o500))
				t.Cleanup(func() { os.Chmod(dir, 0o700) })
			}

			done := make(chan error, 1)
			go func() { done <- b.Shutdown(ctx) }()
			select {
			case err := <-done:
				if !tt.expectPersist {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
			case <-time.After(30 * time.Second):
				require.FailNow(t, "shutdown waits forever for a flush that failed")
			}

			reloaded := openReplaceBucket(t, dir)
			defer reloaded.Shutdown(ctx)
			value, err := reloaded.Get([]byte("key"))
			require.NoError(t, err)
			require.Equal(t, []byte("value"), value)
		})
	}
}

func openReplaceBucket(t *testing.T, dir string) *Bucket {
	t.Helper()
	logger, _ := test.NewNullLogger()
	b, err := NewBucketCreator().NewBucket(context.Background(), dir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace))
	require.NoError(t, err)
	return b
}

func requireValues(t *testing.T, b *Bucket, want map[string]string) {
	t.Helper()
	for k, v := range want {
		got, err := b.Get([]byte(k))
		require.NoError(t, err)
		require.Equal(t, []byte(v), got, "key %q", k)
	}
}

// failFlush leaves k1 in a flushing memtable whose commit log never received it.
func failFlush(t *testing.T, b *Bucket) {
	t.Helper()
	require.NoError(t, b.Put([]byte("k1"), []byte("v1")))
	active := b.active.(*Memtable)
	active.commitlog = failingCloseCommitLog{active.commitlog, false}
	require.Error(t, b.FlushAndSwitch())
	require.NotNil(t, b.flushing)
}

// A flush after a failed one must write out the failed memtable first instead of
// replacing it, or its writes are lost. If that write fails again, the flush fails
// and the writes stay readable.
func TestBucket_FlushAfterFailedFlush(t *testing.T) {
	tests := []struct {
		name          string
		retryReadOnly bool
	}{
		{name: "retry succeeds"},
		{name: "retry fails, then succeeds", retryReadOnly: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.retryReadOnly && os.Geteuid() == 0 {
				t.Skip("root ignores directory permissions")
			}

			ctx := context.Background()
			dir := t.TempDir()
			want := map[string]string{"k1": "v1", "k2": "v2"}

			b := openReplaceBucket(t, dir)
			failFlush(t, b)
			require.NoError(t, b.Put([]byte("k2"), []byte("v2")))
			_, err := b.atomicallySwitchMemtable(b.createNewActiveMemtable)
			require.Error(t, err, "switch must not replace the failed memtable")

			if tt.retryReadOnly {
				require.NoError(t, os.Chmod(dir, 0o500))
				require.Error(t, b.FlushAndSwitch())
				require.NoError(t, os.Chmod(dir, 0o700))
				requireValues(t, b, want)
			}

			require.NoError(t, b.FlushAndSwitch())
			require.Nil(t, b.flushing)
			requireValues(t, b, want)
			require.NoError(t, b.Shutdown(ctx))

			reloaded := openReplaceBucket(t, dir)
			defer reloaded.Shutdown(ctx)
			requireValues(t, reloaded, want)
		})
	}
}

// A flush can fail after it wrote the segment and removed the commit log. Writing
// that memtable out again must not fail on the missing commit log.
func TestBucket_FailedFlushAfterSegmentWritten(t *testing.T) {
	tests := []struct {
		name  string
		drain func(*Bucket) error
	}{
		{name: "next flush", drain: func(b *Bucket) error { return b.FlushAndSwitch() }},
		{name: "shutdown", drain: func(b *Bucket) error { return b.Shutdown(context.Background()) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()

			b := openReplaceBucket(t, dir)
			require.NoError(t, b.Put([]byte("k1"), []byte("v1")))
			switched, err := b.atomicallySwitchMemtable(b.createNewActiveMemtable)
			require.NoError(t, err)
			require.True(t, switched)
			// what a flush leaves when adding its segment to the bucket fails
			_, err = b.flushing.flush()
			require.NoError(t, err)

			require.NoError(t, tt.drain(b))
			b.Shutdown(ctx)

			reloaded := openReplaceBucket(t, dir)
			defer reloaded.Shutdown(ctx)
			requireValues(t, reloaded, map[string]string{"k1": "v1"})
		})
	}
}
