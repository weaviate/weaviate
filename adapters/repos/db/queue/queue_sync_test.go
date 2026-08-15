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

package queue

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDiskQueue_SyncDurability pins the queue's snapshot-durability gate:
// after Sync, every record accepted by Push must be recoverable from the
// on-disk state alone. The no-sync leg pins the hazard the gate exists for —
// Push writes through a 256KiB buffered writer, so a record straddling the
// buffer boundary is auto-flushed only partially and a crash leaves a torn
// tail that recovery truncates (the live "queue ended abruptly ... EOF"
// event observed under voter crash-cycling). This is an engine-level
// simulation of the crash-visible state: the first queue is abandoned
// without Flush or Close, and a second queue over the same directory sees
// exactly the bytes a killed process would have left behind. It cannot
// distinguish fsynced bytes from page-cache bytes (a true power-loss red is
// not deterministically buildable in-process); the fsync side of Sync is
// pinned structurally by TestDiskQueue_SyncDrainsPromotedChunks.
func TestDiskQueue_SyncDurability(t *testing.T) {
	// Three records of 100KiB: the third straddles the 256KiB buffer
	// boundary, so its prefix is auto-flushed to disk and its remainder
	// stays in the process buffer — a torn record after a crash.
	const recordSize = 100 * 1024

	tests := []struct {
		name          string
		sync          bool
		wantRecovered int64
	}{
		{name: "without sync the torn tail record is lost", sync: false, wantRecovered: 2},
		{name: "sync makes every pushed record recoverable", sync: true, wantRecovered: 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			s := makeScheduler(t)
			q := makeQueueWith(t, s, discardExecutor(), 0, dir)

			for i := 0; i < 3; i++ {
				require.NoError(t, q.Push(bytes.Repeat([]byte{byte('a' + i)}, recordSize)))
			}
			if tc.sync {
				require.NoError(t, q.Sync())
			}

			// Simulated crash: abandon q (no Flush, no Close) and recover
			// from the directory contents alone.
			q2 := makeQueueWith(t, s, discardExecutor(), 0, dir)
			require.Equal(t, tc.wantRecovered, q2.Size(),
				"recovered record count after simulated crash")
		})
	}
}

// TestDiskQueue_SyncDrainsPromotedChunks pins the promoted-chunk half of the
// gate: chunks promoted on the keep-open path are tracked as unsynced (their
// bytes reached the page cache but were never fsynced) and Sync must fsync
// and drain the whole set — the queue-wide durability gate covers the
// promoted backlog, not just the partial chunk.
func TestDiskQueue_SyncDrainsPromotedChunks(t *testing.T) {
	s := makeScheduler(t)
	// Tiny chunks: every few records promote the partial chunk via the
	// keep-open (no fsync) path.
	q := makeQueueSize(t, s, discardExecutor(), 50)

	pushMany(t, q, 1, 100, 200, 300, 400, 500, 600)

	q.r.m.Lock()
	unsynced := len(q.r.unsynced)
	q.r.m.Unlock()
	require.NotZero(t, unsynced, "promotions must have left kept-open chunks unsynced")

	require.NoError(t, q.Sync())

	q.r.m.Lock()
	unsynced = len(q.r.unsynced)
	q.r.m.Unlock()
	require.Zero(t, unsynced, "Sync must fsync and drain every promoted-but-unsynced chunk")
}
