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
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"sync"
	"syscall"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/compact"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/multivector"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/vectorindex/compression"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// disableGC keeps the *os.File finalizer from closing a leaked descriptor mid-test.
func disableGC(t *testing.T) {
	t.Helper()
	previous := debug.SetGCPercent(-1)
	t.Cleanup(func() { debug.SetGCPercent(previous) })
}

// openDescriptorCount returns how many of this process's file descriptors
// refer to the file at filePath, matched by device and inode.
func openDescriptorCount(t *testing.T, filePath string) int {
	t.Helper()

	info, err := os.Stat(filePath)
	require.NoError(t, err)
	want := info.Sys().(*syscall.Stat_t)

	entries, err := os.ReadDir("/dev/fd")
	require.NoError(t, err)

	count := 0
	for _, entry := range entries {
		fd, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		var got syscall.Stat_t
		// ReadDir closed the descriptor it listed /dev/fd through, so Fstat fails on it
		if err := syscall.Fstat(fd, &got); err != nil {
			continue
		}
		if got.Dev == want.Dev && got.Ino == want.Ino {
			count++
		}
	}
	return count
}

// commitLogDescriptorCount sums the descriptors held on every file in a commit log directory.
func commitLogDescriptorCount(t *testing.T, dir string) int {
	t.Helper()

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	count := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		count += openDescriptorCount(t, filepath.Join(dir, entry.Name()))
	}
	return count
}

func readCommits(t *testing.T, filePath string) []compact.Commit {
	t.Helper()

	f, err := os.Open(filePath)
	require.NoError(t, err)
	defer f.Close()

	reader := compact.NewWALCommitReader(f, logger)
	var commits []compact.Commit
	for {
		c, err := reader.ReadNextCommit()
		if errors.Is(err, io.EOF) {
			return commits
		}
		require.NoError(t, err)
		commits = append(commits, c)
	}
}

func hasTombstoneCommit(commits []compact.Commit, id uint64) bool {
	for _, c := range commits {
		if tc, ok := c.(*compact.AddTombstoneCommit); ok && tc.ID == id {
			return true
		}
	}
	return false
}

func newShutdownTestCommitLogger(t *testing.T, rootPath string) *hnswCommitLogger {
	t.Helper()

	nullLogger, _ := test.NewNullLogger()
	cl, err := NewCommitLogger(rootPath, "shutdown", nullLogger, cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	cl.InitMaintenance()
	return cl
}

func TestCommitLoggerShutdownReleasesFile(t *testing.T) {
	tests := []struct {
		name       string
		flushed    []uint64
		unflushed  []uint64
		shutdowns  int
		wantOnDisk []uint64
	}{
		{name: "empty log", shutdowns: 1},
		{name: "flushed records", flushed: []uint64{1, 2}, shutdowns: 1, wantOnDisk: []uint64{1, 2}},
		{name: "record appended after the last flush", flushed: []uint64{1}, unflushed: []uint64{2}, shutdowns: 1, wantOnDisk: []uint64{1, 2}},
		{name: "never flushed", unflushed: []uint64{3}, shutdowns: 1, wantOnDisk: []uint64{3}},
		{name: "shutdown twice", flushed: []uint64{1}, unflushed: []uint64{2}, shutdowns: 2, wantOnDisk: []uint64{1, 2}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			disableGC(t)
			cl := newShutdownTestCommitLogger(t, t.TempDir())
			filePath := cl.ActiveFilePath()

			for _, id := range tt.flushed {
				require.NoError(t, cl.AddTombstone(id))
			}
			require.NoError(t, cl.Flush())
			for _, id := range tt.unflushed {
				require.NoError(t, cl.AddTombstone(id))
			}
			require.Equal(t, 1, openDescriptorCount(t, filePath))

			for range tt.shutdowns {
				require.NoError(t, cl.Shutdown(context.Background()))
			}

			commits := readCommits(t, filePath)
			for _, id := range tt.wantOnDisk {
				require.True(t, hasTombstoneCommit(commits, id), "tombstone %d is not on disk after shutdown", id)
			}
			require.Zero(t, openDescriptorCount(t, filePath), "shutdown left the commit log file open")
		})
	}
}

func TestCommitLoggerDropAfterShutdown(t *testing.T) {
	root := t.TempDir()
	cl := newShutdownTestCommitLogger(t, root)
	require.NoError(t, cl.AddTombstone(5))

	require.NoError(t, cl.Shutdown(context.Background()))
	require.NoError(t, cl.Drop(context.Background()))

	_, err := os.Stat(commitLogDirectory(root, "shutdown"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestCommitLoggerWritesAfterShutdownFail(t *testing.T) {
	tests := []struct {
		name  string
		write func(cl *hnswCommitLogger) error
	}{
		{name: "AddNode", write: func(cl *hnswCommitLogger) error { return cl.AddNode(1, 0) }},
		{name: "SetEntryPointWithMaxLayer", write: func(cl *hnswCommitLogger) error { return cl.SetEntryPointWithMaxLayer(1, 0) }},
		{name: "AddLinkAtLevel", write: func(cl *hnswCommitLogger) error { return cl.AddLinkAtLevel(1, 0, 2) }},
		{name: "ReplaceLinksAtLevel", write: func(cl *hnswCommitLogger) error { return cl.ReplaceLinksAtLevel(1, 0, []uint64{2}) }},
		{name: "AddTombstone", write: func(cl *hnswCommitLogger) error { return cl.AddTombstone(1) }},
		{name: "RemoveTombstone", write: func(cl *hnswCommitLogger) error { return cl.RemoveTombstone(1) }},
		{name: "ClearLinks", write: func(cl *hnswCommitLogger) error { return cl.ClearLinks(1) }},
		{name: "ClearLinksAtLevel", write: func(cl *hnswCommitLogger) error { return cl.ClearLinksAtLevel(1, 0) }},
		{name: "DeleteNode", write: func(cl *hnswCommitLogger) error { return cl.DeleteNode(1) }},
		{name: "Reset", write: func(cl *hnswCommitLogger) error { return cl.Reset() }},
		{name: "AddPQCompression", write: func(cl *hnswCommitLogger) error { return cl.AddPQCompression(compression.PQData{}) }},
		{name: "AddSQCompression", write: func(cl *hnswCommitLogger) error { return cl.AddSQCompression(compression.SQData{}) }},
		{name: "AddRQCompression", write: func(cl *hnswCommitLogger) error { return cl.AddRQCompression(compression.RQData{}) }},
		{name: "AddBRQCompression", write: func(cl *hnswCommitLogger) error { return cl.AddBRQCompression(compression.BRQData{}) }},
		{name: "AddMuvera", write: func(cl *hnswCommitLogger) error { return cl.AddMuvera(multivector.MuveraData{}) }},
		{name: "Flush", write: func(cl *hnswCommitLogger) error { return cl.Flush() }},
		{name: "PrepareForBackup forced", write: func(cl *hnswCommitLogger) error { return cl.PrepareForBackup(true) }},
		{name: "PrepareForBackup unforced", write: func(cl *hnswCommitLogger) error { return cl.PrepareForBackup(false) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			cl := newShutdownTestCommitLogger(t, root)
			filePath := cl.ActiveFilePath()
			require.NoError(t, cl.Shutdown(context.Background()))

			var err error
			require.NotPanics(t, func() { err = tt.write(cl) })
			require.ErrorContains(t, err, "commit log is closed")

			entries, readErr := os.ReadDir(commitLogDirectory(root, "shutdown"))
			require.NoError(t, readErr)
			require.Len(t, entries, 1, "a write after shutdown created a new commit log file")
			info, statErr := os.Stat(filePath)
			require.NoError(t, statErr)
			require.Zero(t, info.Size(), "a write after shutdown reached the commit log file")
		})
	}
}

func TestCommitLoggerConcurrentWritesAndShutdown(t *testing.T) {
	cl := newShutdownTestCommitLogger(t, t.TempDir())
	filePath := cl.ActiveFilePath()

	var wg sync.WaitGroup
	start := make(chan struct{})
	for w := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := range 1000 {
				if err := cl.AddTombstone(uint64(w*1000 + i)); err != nil {
					return
				}
			}
		}()
	}
	close(start)
	require.NoError(t, cl.Shutdown(context.Background()))
	wg.Wait()

	require.Zero(t, openDescriptorCount(t, filePath))
	require.ErrorContains(t, cl.AddTombstone(1), "commit log is closed")
}

func newShutdownTestIndex(t *testing.T, rootPath string, vectors [][]float32) *hnsw {
	t.Helper()

	nullLogger, _ := test.NewNullLogger()
	const id = "shutdown-index"
	idx, err := New(Config{
		RootPath:         rootPath,
		ID:               id,
		Logger:           nullLogger,
		DistanceProvider: distancer.NewCosineDistanceProvider(),
		AllocChecker:     memwatch.NewDummyMonitor(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
		TempVectorForIDWithViewThunk: func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
			copy(container.Slice, vectors[int(id)])
			return container.Slice, nil
		},
		GetViewThunk:      func() common.BucketView { return &restoreNoopBucketView{} },
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
		MakeCommitLoggerThunk: func(opts ...CommitlogOption) (CommitLogger, error) {
			return NewCommitLogger(rootPath, id, nullLogger, cyclemanager.NewCallbackGroupNoop(), opts...)
		},
	}, ent.NewDefaultUserConfig(), cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.NoError(t, err)
	idx.PostStartup(context.Background())
	return idx
}

func TestIndexShutdownReleasesCommitLog(t *testing.T) {
	tests := []struct {
		name          string
		flushed       int
		unflushed     int
		shutdowns     int
		dropAfterward bool
	}{
		{name: "nothing inserted", shutdowns: 1},
		{name: "all inserts flushed", flushed: 10, shutdowns: 1},
		{name: "inserts after the last flush", flushed: 10, unflushed: 5, shutdowns: 1},
		{name: "shutdown twice", flushed: 10, unflushed: 5, shutdowns: 2},
		{name: "drop after shutdown", flushed: 10, unflushed: 5, shutdowns: 1, dropAfterward: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			disableGC(t)
			ctx := context.Background()
			root := t.TempDir()
			vectors, _ := testinghelpers.RandomVecs(tt.flushed+tt.unflushed, 0, 8)
			idx := newShutdownTestIndex(t, root, vectors)
			dir := commitLogDirectory(root, "shutdown-index")

			for i := range tt.flushed {
				require.NoError(t, idx.Add(ctx, uint64(i), vectors[i]))
			}
			require.NoError(t, idx.Flush())
			for i := tt.flushed; i < tt.flushed+tt.unflushed; i++ {
				require.NoError(t, idx.Add(ctx, uint64(i), vectors[i]))
			}
			require.Equal(t, 1, commitLogDescriptorCount(t, dir))

			for range tt.shutdowns {
				require.NoError(t, idx.Shutdown(ctx))
			}
			require.Zero(t, commitLogDescriptorCount(t, dir), "index shutdown left its commit log open")
			require.ErrorContains(t, idx.Flush(), "commit log is closed")

			if tt.dropAfterward {
				require.NoError(t, idx.Drop(ctx))
				_, err := os.Stat(dir)
				require.ErrorIs(t, err, os.ErrNotExist)
				return
			}

			reloaded := newShutdownTestIndex(t, root, vectors)
			for i := range tt.flushed + tt.unflushed {
				require.True(t, reloaded.ContainsDoc(uint64(i)), "node %d is missing after reload", i)
			}
			require.NoError(t, reloaded.Shutdown(ctx))
		})
	}
}

type orderRecorder struct {
	mu    sync.Mutex
	steps []string
}

func (r *orderRecorder) record(step string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.steps = append(r.steps, step)
}

type recordingCommitLogger struct {
	NoopCommitLogger
	recorder *orderRecorder
}

func (l *recordingCommitLogger) Shutdown(context.Context) error {
	l.recorder.record("commit log shutdown")
	return nil
}

type recordingCallbackCtrl struct {
	cyclemanager.CycleCallbackCtrl
	recorder *orderRecorder
}

func (c *recordingCallbackCtrl) Unregister(context.Context) error {
	c.recorder.record("tombstone cleanup unregister")
	return nil
}

func TestIndexShutdownStopsTombstoneCleanupBeforeClosingCommitLog(t *testing.T) {
	recorder := &orderRecorder{}
	index := &hnsw{
		shutdownCtxCancel:            func() {},
		compressActionLock:           &sync.RWMutex{},
		commitLog:                    &recordingCommitLogger{recorder: recorder},
		tombstoneCleanupCallbackCtrl: &recordingCallbackCtrl{recorder: recorder},
		cache:                        newFakeCache(),
	}

	require.NoError(t, index.Shutdown(context.Background()))
	require.Equal(t, []string{"tombstone cleanup unregister", "commit log shutdown"}, recorder.steps)
}

var (
	errInjectedUnregister = errors.New("injected unregister failure")
	errInjectedWrite      = errors.New("injected write failure")
	errInjectedClose      = errors.New("injected close failure")
)

type shutdownFaultFS struct {
	common.FS
	writeErr error
	closeErr error
}

func (f *shutdownFaultFS) OpenFile(name string, flag int, perm os.FileMode) (common.File, error) {
	file, err := f.FS.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &shutdownFaultFile{File: file, fs: f}, nil
}

type shutdownFaultFile struct {
	common.File
	fs *shutdownFaultFS
}

func (f *shutdownFaultFile) Write(p []byte) (int, error) {
	if f.fs.writeErr != nil {
		return 0, f.fs.writeErr
	}
	return f.File.Write(p)
}

func (f *shutdownFaultFile) Close() error {
	err := f.File.Close()
	if f.fs.closeErr != nil {
		return f.fs.closeErr
	}
	return err
}

type failOnceCallbackCtrl struct {
	cyclemanager.CycleCallbackCtrl
	err    error
	failed bool
}

func (c *failOnceCallbackCtrl) Unregister(ctx context.Context) error {
	if !c.failed {
		c.failed = true
		return c.err
	}
	return c.CycleCallbackCtrl.Unregister(ctx)
}

func TestCommitLoggerShutdownFailures(t *testing.T) {
	tests := []struct {
		name          string
		unregisterErr error
		writeErr      error
		closeErr      error
	}{
		{name: "unregister fails", unregisterErr: errInjectedUnregister},
		{name: "flush fails", writeErr: errInjectedWrite},
		{name: "close fails", closeErr: errInjectedClose},
		{name: "flush and close fail", writeErr: errInjectedWrite, closeErr: errInjectedClose},
		{name: "unregister and flush fail", unregisterErr: errInjectedUnregister, writeErr: errInjectedWrite},
		{name: "unregister, flush and close fail", unregisterErr: errInjectedUnregister, writeErr: errInjectedWrite, closeErr: errInjectedClose},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			disableGC(t)
			ctx := context.Background()
			root := t.TempDir()
			fs := &shutdownFaultFS{FS: common.NewOSFS()}
			nullLogger, _ := test.NewNullLogger()
			cl, err := NewCommitLogger(root, "shutdown", nullLogger, cyclemanager.NewCallbackGroupNoop(), WithFS(fs))
			require.NoError(t, err)
			cl.InitMaintenance()
			if tt.unregisterErr != nil {
				cl.switchLogsCallbackCtrl = &failOnceCallbackCtrl{CycleCallbackCtrl: cl.switchLogsCallbackCtrl, err: tt.unregisterErr}
			}
			filePath := cl.ActiveFilePath()
			require.NoError(t, cl.AddTombstone(1))
			fs.writeErr, fs.closeErr = tt.writeErr, tt.closeErr

			closeErrs := []error{}
			for _, e := range []error{tt.writeErr, tt.closeErr} {
				if e != nil {
					closeErrs = append(closeErrs, e)
				}
			}

			err = cl.Shutdown(ctx)
			require.Error(t, err)
			for _, want := range append([]error{tt.unregisterErr}, closeErrs...) {
				if want != nil {
					require.ErrorIs(t, err, want)
				}
			}
			require.Zero(t, openDescriptorCount(t, filePath), "a failed shutdown left the commit log file open")
			require.ErrorIs(t, cl.AddTombstone(2), ErrCommitLogClosed)

			retryErr := cl.Shutdown(ctx)
			dropErr := cl.Drop(ctx)
			_, statErr := os.Stat(commitLogDirectory(root, "shutdown"))
			if len(closeErrs) == 0 {
				require.NoError(t, retryErr)
				require.NoError(t, dropErr)
				require.ErrorIs(t, statErr, os.ErrNotExist)
				return
			}
			for _, want := range closeErrs {
				require.ErrorIs(t, retryErr, want, "a repeated shutdown hid the failed close")
				require.ErrorIs(t, dropErr, want, "drop ignored the failed close")
			}
			require.NotErrorIs(t, retryErr, errInjectedUnregister)
			require.NoError(t, statErr, "drop deleted the commit log after a failed close")
		})
	}
}
