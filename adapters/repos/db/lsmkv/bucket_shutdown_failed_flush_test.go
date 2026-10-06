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
			logger, _ := test.NewNullLogger()
			dir := t.TempDir()
			open := func() *Bucket {
				b, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
					cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
					WithStrategy(StrategyReplace))
				require.NoError(t, err)
				return b
			}

			b := open()
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

			reloaded := open()
			defer reloaded.Shutdown(ctx)
			value, err := reloaded.Get([]byte("key"))
			require.NoError(t, err)
			require.Equal(t, []byte("value"), value)
		})
	}
}
