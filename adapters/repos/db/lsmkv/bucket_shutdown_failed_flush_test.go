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
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// failingCloseCommitLog fails the flush the way a full disk does: the commit log is
// closed, and the flush gives up before writing a segment.
type failingCloseCommitLog struct {
	memtableCommitLogger
}

func (c failingCloseCommitLog) close() error {
	_ = c.memtableCommitLogger.close()
	return errors.New("no space left on device")
}

// A flush that failed leaves its memtable flushing, and nothing clears it. Shutdown
// must not wait for it, and the next load must still find its data.
func TestBucket_ShutdownAfterFailedFlush(t *testing.T) {
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
	active.commitlog = failingCloseCommitLog{active.commitlog}
	require.Error(t, b.FlushAndSwitch())
	require.NotNil(t, b.flushing)

	done := make(chan error, 1)
	go func() { done <- b.Shutdown(ctx) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		require.FailNow(t, "shutdown waits forever for a flush that failed")
	}

	reloaded := open()
	defer reloaded.Shutdown(ctx)
	value, err := reloaded.Get([]byte("key"))
	require.NoError(t, err)
	require.Equal(t, []byte("value"), value)
}
