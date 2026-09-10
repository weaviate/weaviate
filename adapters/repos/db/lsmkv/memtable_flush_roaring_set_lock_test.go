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
	"bufio"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// discardingSegmentFile writes a flush nowhere, so a test can call
// writeRoaringSetNodes without producing a segment to read back.
func discardingSegmentFile() *segmentindex.SegmentFile {
	return segmentindex.NewSegmentFile(
		segmentindex.WithBufferedWriter(bufio.NewWriter(io.Discard)))
}

func TestFlushRoaringSetBlocksOnMemtableWriteLock(t *testing.T) {
	logger, _ := test.NewNullLogger()
	m := newRoaringSetFlushFixture(t, flushFixtureShapes(), false)

	m.Lock()
	var once sync.Once
	release := func() { once.Do(m.Unlock) }
	// Cleanup as well as the release below, so a FailNow between the two cannot
	// leave the flush goroutine parked on the lock.
	t.Cleanup(release)

	flushed := make(chan error, 1)
	enterrors.GoWrapper(func() {
		_, err := m.writeRoaringSetNodes(discardingSegmentFile())
		flushed <- err
	}, logger)

	select {
	case <-flushed:
		t.Fatal("writeRoaringSetNodes read the memtable while a writer held its lock")
	case <-time.After(100 * time.Millisecond):
	}

	release()

	select {
	case err := <-flushed:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("writeRoaringSetNodes did not finish after the writer released the lock")
	}
}

// TestFlushRoaringSetConcurrentWrite is a -race probe rather than a
// deterministic gate: whether the writer overlaps the walk is up to the
// scheduler, and TestFlushRoaringSetBlocksOnMemtableWriteLock is what fails
// every time.
func TestFlushRoaringSetConcurrentWrite(t *testing.T) {
	logger, _ := test.NewNullLogger()
	m := newRoaringSetFlushFixture(t, flushFixtureShapes(), false)

	written := make(chan error, 1)
	enterrors.GoWrapper(func() {
		for i := 0; i < 200; i++ {
			if err := m.roaringSetAddList([]byte("additions"), []uint64{uint64(i)}); err != nil {
				written <- err
				return
			}
		}
		written <- nil
	}, logger)

	_, err := m.writeRoaringSetNodes(discardingSegmentFile())
	require.NoError(t, err)

	select {
	case err := <-written:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("the writer goroutine never reported")
	}
}
