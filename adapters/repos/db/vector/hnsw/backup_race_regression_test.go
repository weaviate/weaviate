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

package hnsw

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestBackup_ListFilesExcludesActiveFile_Deterministic shows the bug
// deterministically: once any byte is written to the active commit log file
// after PrepareForBackup, ListFiles will include it. The new size>0 filter
// is the *only* mechanism keeping the active file out of the backup list,
// so this is sufficient to break the "flush first, then only copy
// immutable files" guarantee — any subsequent write to the file makes the
// backup contain a mutable file.
func TestBackup_ListFilesExcludesActiveFile_Deterministic(t *testing.T) {
	ctx := context.Background()
	dirName := t.TempDir()
	indexID := "backup-race-deterministic"

	idx, err := New(Config{
		RootPath:         dirName,
		ID:               indexID,
		Logger:           logrus.New(),
		DistanceProvider: distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: testVectorForID,
		GetViewThunk:     func() common.BucketView { return &backupNoopBucketView{} },
		MakeCommitLoggerThunk: func(opts ...CommitlogOption) (CommitLogger, error) {
			return NewCommitLogger(dirName, indexID, logrus.New(), cyclemanager.NewCallbackGroupNoop(), opts...)
		},
	}, enthnsw.NewDefaultUserConfig(), cyclemanager.NewCallbackGroupNoop(), nil)
	require.NoError(t, err)
	idx.PostStartup(context.Background())
	t.Cleanup(func() { _ = idx.Shutdown(ctx) })

	cl := idx.commitLog.(*hnswCommitLogger)

	// Seed the first commit log with at least one byte and force it onto disk
	// so that PrepareForBackup will rotate to a new (still-empty) file with a
	// distinct timestamp. Sleep > 1s so the new file gets a different
	// `time.Now().Unix()` second.
	require.NoError(t, cl.AddNode(&vertex{id: 1, level: 0}))
	require.NoError(t, cl.Flush())
	time.Sleep(1100 * time.Millisecond)

	// Switch — this is what the shard backup machinery does when it calls
	// idx.PrepareForBackup. After it returns, the *new* commit log file is
	// empty and the old one is sealed.
	require.NoError(t, idx.PrepareForBackup(ctx))

	// Capture the active file name *as the backup machinery would observe it*
	// (everything between PrepareForBackup and ListFiles is happening with
	// the queue resumed and writers free to add to the index).
	activeBase := filepath.Base(cl.currentFileName)

	// Simulate a single write that races into the commit log between
	// PrepareForBackup and ListFiles. This is the gap that the QA pipeline
	// is hitting under load: the queue's `defer q.Resume()` lets workers
	// dequeue tasks and call into the HNSW writers as soon as the queue's
	// PrepareForBackup returns, which happens before idx.PrepareForBackup,
	// and writes can continue all the way through ListFiles.
	require.NoError(t, cl.AddNode(&vertex{id: 2, level: 0}))
	require.NoError(t, cl.Flush())

	files, err := idx.ListFiles(ctx, dirName)
	require.NoError(t, err)

	for _, f := range files {
		if filepath.Base(f) == activeBase {
			t.Fatalf("ListFiles included the active commit log file %q "+
				"(returned: %v). The backup will copy a file that is "+
				"still being written to, breaking the flush-then-copy-immutable "+
				"guarantee.", activeBase, files)
		}
	}
}

// TestBackup_ListFilesExcludesActiveFile_ConcurrentRace stresses the same bug
// under concurrent writes (matching the scenario in the bug report). A
// background goroutine continuously writes + flushes to the commit log; the
// foreground loops PrepareForBackup → ListFiles and asserts the active file
// is never returned.
//
// This test is deliberately stress-y. If the race is real it is essentially
// guaranteed to fire within a handful of iterations because the writer
// flushes after every write, so the new commit log file becomes non-empty
// almost immediately after the rotation in PrepareForBackup.
func TestBackup_ListFilesExcludesActiveFile_ConcurrentRace(t *testing.T) {
	ctx := context.Background()
	dirName := t.TempDir()
	indexID := "backup-race-concurrent"

	idx, err := New(Config{
		RootPath:         dirName,
		ID:               indexID,
		Logger:           logrus.New(),
		DistanceProvider: distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: testVectorForID,
		GetViewThunk:     func() common.BucketView { return &backupNoopBucketView{} },
		MakeCommitLoggerThunk: func(opts ...CommitlogOption) (CommitLogger, error) {
			return NewCommitLogger(dirName, indexID, logrus.New(), cyclemanager.NewCallbackGroupNoop(), opts...)
		},
	}, enthnsw.NewDefaultUserConfig(), cyclemanager.NewCallbackGroupNoop(), nil)
	require.NoError(t, err)
	idx.PostStartup(context.Background())
	t.Cleanup(func() { _ = idx.Shutdown(ctx) })

	cl := idx.commitLog.(*hnswCommitLogger)

	// Seed and let the first commit log file's timestamp settle, otherwise
	// successive switches in the same wall-clock second collide on filename
	// (filenames are `time.Now().Unix()`).
	require.NoError(t, cl.AddNode(&vertex{id: 1, level: 0}))
	require.NoError(t, cl.Flush())

	var (
		stop     atomic.Bool
		wg       sync.WaitGroup
		writeID  atomic.Uint64
		raceHits atomic.Int64
	)
	writeID.Store(2)

	// Background writer: continuously appends to whatever the current commit
	// log file is. The commit logger's mutex serializes against
	// PrepareForBackup, so this never *prevents* the rotation — it just makes
	// sure that as soon as the rotation releases the lock, the new file
	// receives data and grows past 0 bytes.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			id := writeID.Add(1)
			if err := cl.AddNode(&vertex{id: id, level: 0}); err != nil {
				return
			}
			if err := cl.Flush(); err != nil {
				return
			}
		}
	}()

	// We can only do one rotation per second (filename collision otherwise),
	// so iterate over a few seconds. 6 iterations is plenty: each iteration
	// gives the writer ~hundreds of microseconds to land a write between
	// PrepareForBackup and ListFiles, and the writer flushes after every op.
	const iterations = 6
	for i := 0; i < iterations; i++ {
		require.NoError(t, idx.PrepareForBackup(ctx))
		activeBase := filepath.Base(cl.currentFileName)

		files, err := idx.ListFiles(ctx, dirName)
		require.NoError(t, err)

		for _, f := range files {
			if filepath.Base(f) == activeBase {
				raceHits.Add(1)
				t.Logf("iteration %d: active file %q in backup list (all returned: %v)",
					i, activeBase, files)
				break
			}
		}

		// Sleep long enough that the next switch lands in a fresh second.
		time.Sleep(1100 * time.Millisecond)
	}

	stop.Store(true)
	wg.Wait()

	if raceHits.Load() > 0 {
		t.Fatalf("active commit log file appeared in backup list %d/%d iterations",
			raceHits.Load(), iterations)
	}
}
