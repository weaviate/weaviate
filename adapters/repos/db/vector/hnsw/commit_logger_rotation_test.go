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
	"errors"
	"os"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// switchCommitLogs rotates the WAL by Flush -> Sync (old file) -> Open (new file)
// -> swap -> Close (old file). The new file is opened BEFORE the old one is
// closed, so a failure on any step never leaves l.currentFile / l.currentWriter
// pointing at a dead fd. These tests inject IO failures at each step and assert
// the logger keeps accepting writes (they fail against the previous ordering,
// which closed the old file before opening the new and left writes on a dead fd).

var errInjected = errors.New("injected IO error (ENOSPC/EIO)")

type rotationFaultFS struct {
	common.FS
	failNextOpenFile bool
	syncErr          error
	closeErr         error
}

func (f *rotationFaultFS) OpenFile(name string, flag int, perm os.FileMode) (common.File, error) {
	if f.failNextOpenFile {
		return nil, errInjected
	}
	file, err := f.FS.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &rotationFaultFile{File: file, fs: f}, nil
}

type rotationFaultFile struct {
	common.File
	fs *rotationFaultFS
}

func (f *rotationFaultFile) Sync() error {
	if f.fs.syncErr != nil {
		return f.fs.syncErr
	}
	return f.File.Sync()
}

func (f *rotationFaultFile) Close() error {
	if f.fs.closeErr != nil {
		// A real close(2) returning EIO still releases the fd; mirror that.
		_ = f.File.Close()
		return f.fs.closeErr
	}
	return f.File.Close()
}

func newFaultCommitLogger(t *testing.T, fs common.FS) *hnswCommitLogger {
	t.Helper()
	l, err := NewCommitLogger(t.TempDir(), "rotation-fault", logrus.New(),
		cyclemanager.NewCallbackGroupNoop(), WithFS(fs))
	require.NoError(t, err)
	return l
}

// writeThenFlush appends a small commit (buffered) then flushes; the flush is
// where a dead underlying fd would surface, since AddTombstone only fills the
// 32KB bufio buffer.
func writeThenFlush(l *hnswCommitLogger) error {
	if err := l.AddTombstone(1); err != nil {
		return err
	}
	return l.Flush()
}

func TestCommitLogRotation_NewFileOpenFails_KeepsWriting(t *testing.T) {
	fs := &rotationFaultFS{FS: common.NewOSFS()}
	l := newFaultCommitLogger(t, fs)
	require.NoError(t, writeThenFlush(l))

	// Forced rotation; opening the new file fails (e.g. ENOSPC).
	fs.failNextOpenFile = true
	require.Error(t, l.PrepareForBackup(true), "rotation surfaces the open failure")

	// The old file was never closed, so writes continue and a later rotation
	// recovers once the fault clears.
	fs.failNextOpenFile = false
	require.NoError(t, writeThenFlush(l), "writes continue on the still-open old file")
	require.NoError(t, l.PrepareForBackup(true), "rotation recovers once the fault clears")
	require.NoError(t, writeThenFlush(l), "writes continue on the new file after recovery")
}

func TestCommitLogRotation_CloseFails_KeepsWriting(t *testing.T) {
	fs := &rotationFaultFS{FS: common.NewOSFS()}
	l := newFaultCommitLogger(t, fs)
	require.NoError(t, writeThenFlush(l))

	// Closing the old file fails after the new file is already active. The
	// rotation has committed (old bytes flushed+synced), so it must not fail.
	fs.closeErr = errInjected
	require.NoError(t, l.PrepareForBackup(true), "old-file close failure does not fail the rotation")

	fs.closeErr = nil
	require.NoError(t, writeThenFlush(l), "writes continue on the new file")
}

func TestCommitLogRotation_SyncFails_KeepsWriting(t *testing.T) {
	fs := &rotationFaultFS{FS: common.NewOSFS()}
	l := newFaultCommitLogger(t, fs)
	require.NoError(t, writeThenFlush(l))

	// Sync fails before anything is closed; the old fd stays open and usable.
	fs.syncErr = errInjected
	require.Error(t, l.PrepareForBackup(true), "rotation surfaces the sync failure")

	fs.syncErr = nil
	require.NoError(t, writeThenFlush(l), "writes continue on the still-open old file")
}

// TestCommitLogRotation_MaintenanceRecovers drives the production trigger (the
// maintenance cycle via startSwitchLogs, with a threshold so a real rotation is
// attempted) and asserts the logger stays healthy after a failed rotation.
func TestCommitLogRotation_MaintenanceRecovers(t *testing.T) {
	fs := &rotationFaultFS{FS: common.NewOSFS()}
	l, err := NewCommitLogger(t.TempDir(), "rotation-fault", logrus.New(),
		cyclemanager.NewCallbackGroupNoop(), WithFS(fs), WithCommitlogThreshold(1))
	require.NoError(t, err)
	require.NoError(t, writeThenFlush(l)) // file now exceeds the 1-byte threshold

	fs.failNextOpenFile = true
	l.startSwitchLogs(func() bool { return false }) // logs the failure; old file intact

	fs.failNextOpenFile = false
	require.NoError(t, writeThenFlush(l),
		"logger stays healthy after a failed maintenance rotation")
}
