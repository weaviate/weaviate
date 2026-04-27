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
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/compact"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/multivector"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/vectorindex/compression"
)

const defaultCommitLogSize = 500 * 1024 * 1024

type hnswCommitLogger struct {
	// protect against concurrent attempts to write in the underlying file or
	// buffer
	sync.Mutex

	rootPath string
	id       string
	logger   logrus.FieldLogger
	fs       common.FS

	// File management
	currentFile     common.File
	currentFileName string
	currentWriter   *bufio.Writer
	walWriter       *compact.WALWriter

	// Compaction (replaces condensor + combiner)
	compactor *compact.Compactor

	// Maintenance
	switchLogsCallbackCtrl   cyclemanager.CycleCallbackCtrl
	maintainLogsCallbackCtrl cyclemanager.CycleCallbackCtrl
	maintenanceCallbacks     cyclemanager.CycleCallbackGroup

	// Config
	maxSizeIndividual int64
}

func NewCommitLogger(rootPath, name string, logger logrus.FieldLogger,
	maintenanceCallbacks cyclemanager.CycleCallbackGroup, opts ...CommitlogOption,
) (*hnswCommitLogger, error) {
	l := &hnswCommitLogger{
		rootPath:             rootPath,
		id:                   name,
		logger:               logger,
		fs:                   common.NewOSFS(),
		maintenanceCallbacks: maintenanceCallbacks,

		// can be overwritten using functional options
		maxSizeIndividual: defaultCommitLogSize / 5,
	}

	for _, o := range opts {
		if err := o(l); err != nil {
			return nil, err
		}
	}

	// Ensure the commit log directory exists
	dir := commitLogDirectory(rootPath, name)
	if err := l.fs.MkdirAll(dir, os.ModePerm); err != nil {
		return nil, errors.Wrap(err, "create commit logger directory")
	}

	// Always start a fresh raw commit log file. Reusing an existing file is a
	// footgun: getCurrentCommitLogFileName used to select the append target
	// by highest parsed timestamp, which let it hand back a .snapshot /
	// .sorted / .condensed file and the next AddNode would corrupt it (block
	// CRCs become invalid on next SnapshotReader load). Creating a new file
	// every startup eliminates the class of bug — the append path can never
	// land on anything but a freshly created raw file that we own.
	//
	// Any existing zero-byte raw files from previous startups that the
	// compactor hasn't yet absorbed are pruned first so the directory never
	// accumulates more than one empty raw file at a time.
	fd, fileName, err := createNewCommitFile(rootPath, name, l.fs, l.logger)
	if err != nil {
		return nil, err
	}

	l.currentFile = fd
	l.currentFileName = fileName
	l.currentWriter = bufio.NewWriterSize(fd, 32*1024)
	l.walWriter = compact.NewWALWriter(l.currentWriter)

	// Create compactor for maintenance
	compactorCfg := compact.DefaultCompactorConfig(dir)
	compactorCfg.FS = l.fs
	l.compactor = compact.NewCompactor(compactorCfg, logger)

	return l, nil
}

func (l *hnswCommitLogger) InitMaintenance() {
	id := func(elems ...string) string {
		elems = append([]string{"commit_logger"}, elems...)
		elems = append(elems, l.id)
		return strings.Join(elems, "/")
	}

	l.switchLogsCallbackCtrl = l.maintenanceCallbacks.Register(id("switch_logs"), l.startSwitchLogs)
	l.maintainLogsCallbackCtrl = l.maintenanceCallbacks.Register(id("maintain_logs"), l.startCommitLogsMaintenance)
}

func commitLogFileName(rootPath, indexName, fileName string) string {
	return fmt.Sprintf("%s/%s", commitLogDirectory(rootPath, indexName), fileName)
}

func commitLogDirectory(rootPath, name string) string {
	return fmt.Sprintf("%s/%s.hnsw.commitlog.d", rootPath, name)
}

// createNewCommitFile always starts a fresh raw commit log file, regardless
// of what already exists in the directory. Before creating the new file, any
// zero-byte raw files left behind by previous startups (that the compactor
// hasn't yet absorbed into a .sorted file) are pruned so the directory holds
// at most one empty raw file at a time.
//
// This is the append target — so by construction it is:
//   - a raw file (pure-timestamp name, no .snapshot/.sorted/.condensed suffix)
//   - newly created in this process, never reused across restarts
//
// Previous versions picked the highest-timestamp file in the directory as the
// append target, which could return a snapshot/sorted/condensed file and
// corrupt it on the next WAL write.
func createNewCommitFile(rootPath, name string, fs common.FS, logger logrus.FieldLogger) (common.File, string, error) {
	dir := commitLogDirectory(rootPath, name)
	if err := fs.MkdirAll(dir, os.ModePerm); err != nil {
		return nil, "", errors.Wrap(err, "create commit logger directory")
	}

	if err := pruneEmptyRawCommitLogs(dir, logger); err != nil {
		// Pruning is best-effort housekeeping — never fatal. A leftover empty
		// raw file will simply be absorbed by the next compaction cycle.
		logger.WithError(err).
			WithField("action", "hnsw_commit_log_prune_empty").
			Warn("failed to prune empty raw commit log files; continuing")
	}

	fileName := fmt.Sprintf("%d", time.Now().Unix())
	filePath := commitLogFileName(rootPath, name, fileName)
	// Still pass O_CREATE for correctness on a fresh directory; O_EXCL would
	// fail in the (extremely unlikely) case of a 1-second restart collision
	// where a prior empty raw file with the same timestamp still exists, and
	// the consequences of the O_APPEND fallback here are harmless because
	// the prior file is — by definition — also a raw file we own.
	fd, err := fs.OpenFile(filePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o666)
	if err != nil {
		return nil, "", errors.Wrap(err, "create commit log file")
	}

	return fd, filePath, nil
}

// pruneEmptyRawCommitLogs removes zero-byte raw commit log files from the
// directory. Raw files are identified by a pure-numeric filename; any file
// with a suffix like .snapshot/.sorted/.condensed or a range form with '_'
// is not a raw file and is never touched by this function.
func pruneEmptyRawCommitLogs(dir string, logger logrus.FieldLogger) error {
	entries, err := listRawCommitLogFiles(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			// Entry vanished between ReadDir and Info; nothing to prune.
			continue
		}
		if info.Size() != 0 {
			continue
		}
		path := fmt.Sprintf("%s/%s", dir, entry.Name())
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			logger.WithError(err).
				WithField("action", "hnsw_commit_log_prune_empty").
				WithField("file", entry.Name()).
				Warn("failed to remove empty raw commit log file")
			continue
		}
		logger.WithFields(logrus.Fields{
			"action": "hnsw_commit_log_prune_empty",
			"file":   entry.Name(),
		}).Debug("removed empty raw commit log file from a previous startup")
	}
	return nil
}

// listRawCommitLogFiles returns the directory entries that represent raw
// commit log files — i.e. entries whose name is a pure timestamp with no
// ".snapshot" / ".sorted" / ".condensed" suffix and no "_" range separator.
// Hidden files and ".tmp" scratch files are also excluded.
func listRawCommitLogFiles(dirPath string) ([]os.DirEntry, error) {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, errors.Wrap(err, "browse commit logger directory")
	}

	raws := make([]os.DirEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, ".") {
			continue
		}
		if strings.HasSuffix(name, ".tmp") {
			continue
		}
		// A raw file's name must be a pure timestamp: no extension, no range.
		if strings.ContainsAny(name, "._") {
			continue
		}
		if _, err := strconv.ParseInt(name, 10, 64); err != nil {
			continue
		}
		raws = append(raws, entry)
	}
	return raws, nil
}

type HnswCommitType uint8 // 256 options, plenty of room for future extensions

const (
	AddNode HnswCommitType = iota
	SetEntryPointMaxLevel
	AddLinkAtLevel
	ReplaceLinksAtLevel
	AddTombstone
	RemoveTombstone
	ClearLinks
	DeleteNode
	ResetIndex
	ClearLinksAtLevel // added in v1.8.0-rc.1, see https://github.com/weaviate/weaviate/issues/1701
	AddLinksAtLevel   // added in v1.8.0-rc.1, see https://github.com/weaviate/weaviate/issues/1705
	AddPQ
	AddSQ
	AddMuvera
	AddRQ
	AddBRQ
)

func (t HnswCommitType) String() string {
	switch t {
	case AddNode:
		return "AddNode"
	case SetEntryPointMaxLevel:
		return "SetEntryPointWithMaxLayer"
	case AddLinkAtLevel:
		return "AddLinkAtLevel"
	case AddLinksAtLevel:
		return "AddLinksAtLevel"
	case ReplaceLinksAtLevel:
		return "ReplaceLinksAtLevel"
	case AddTombstone:
		return "AddTombstone"
	case RemoveTombstone:
		return "RemoveTombstone"
	case ClearLinks:
		return "ClearLinks"
	case DeleteNode:
		return "DeleteNode"
	case ResetIndex:
		return "ResetIndex"
	case ClearLinksAtLevel:
		return "ClearLinksAtLevel"
	case AddPQ:
		return "AddProductQuantizer"
	case AddSQ:
		return "AddScalarQuantizer"
	case AddMuvera:
		return "AddMuvera"
	case AddRQ:
		return "AddRotationalQuantizer"
	case AddBRQ:
		return "AddBRQCompression"
	}
	return "unknown commit type"
}

func (l *hnswCommitLogger) ID() string {
	return l.id
}

func (l *hnswCommitLogger) AddPQCompression(data compression.PQData) error {
	l.Lock()
	defer l.Unlock()

	return l.walWriter.WriteAddPQ(&data)
}

func (l *hnswCommitLogger) AddSQCompression(data compression.SQData) error {
	l.Lock()
	defer l.Unlock()

	return l.walWriter.WriteAddSQ(&data)
}

func (l *hnswCommitLogger) AddRQCompression(data compression.RQData) error {
	l.Lock()
	defer l.Unlock()

	return l.walWriter.WriteAddRQ(&data)
}

func (l *hnswCommitLogger) AddMuvera(data multivector.MuveraData) error {
	l.Lock()
	defer l.Unlock()

	return l.walWriter.WriteAddMuvera(&data)
}

func (l *hnswCommitLogger) AddBRQCompression(data compression.BRQData) error {
	l.Lock()
	defer l.Unlock()

	return l.walWriter.WriteAddBRQ(&data)
}

// AddNode adds an empty node
func (l *hnswCommitLogger) AddNode(node *vertex) error {
	l.Lock()
	defer l.Unlock()

	return l.walWriter.WriteAddNode(node.id, uint16(node.level))
}

func (l *hnswCommitLogger) SetEntryPointWithMaxLayer(id uint64, level int) error {
	l.Lock()
	defer l.Unlock()

	return l.walWriter.WriteSetEntryPointMaxLevel(id, uint16(level))
}

func (l *hnswCommitLogger) ReplaceLinksAtLevel(nodeid uint64, level int, targets []uint64) error {
	l.Lock()
	defer l.Unlock()

	return l.walWriter.WriteReplaceLinksAtLevel(nodeid, uint16(level), targets)
}

func (l *hnswCommitLogger) AddLinkAtLevel(nodeid uint64, level int, target uint64) error {
	l.Lock()
	defer l.Unlock()

	return l.walWriter.WriteAddLinkAtLevel(nodeid, uint16(level), target)
}

func (l *hnswCommitLogger) AddTombstone(nodeid uint64) error {
	l.Lock()
	defer l.Unlock()

	return l.walWriter.WriteAddTombstone(nodeid)
}

func (l *hnswCommitLogger) RemoveTombstone(nodeid uint64) error {
	l.Lock()
	defer l.Unlock()

	return l.walWriter.WriteRemoveTombstone(nodeid)
}

func (l *hnswCommitLogger) ClearLinks(nodeid uint64) error {
	l.Lock()
	defer l.Unlock()

	return l.walWriter.WriteClearLinks(nodeid)
}

func (l *hnswCommitLogger) ClearLinksAtLevel(nodeid uint64, level uint16) error {
	l.Lock()
	defer l.Unlock()

	return l.walWriter.WriteClearLinksAtLevel(nodeid, level)
}

func (l *hnswCommitLogger) DeleteNode(nodeid uint64) error {
	l.Lock()
	defer l.Unlock()

	return l.walWriter.WriteDeleteNode(nodeid)
}

func (l *hnswCommitLogger) Reset() error {
	l.Lock()
	defer l.Unlock()

	return l.walWriter.WriteResetIndex()
}

// Shutdown waits for ongoing maintenance processes to stop, then cancels their
// scheduling. The caller can be sure that state on disk is immutable after
// calling Shutdown().
func (l *hnswCommitLogger) Shutdown(ctx context.Context) error {
	if l.switchLogsCallbackCtrl != nil {
		if err := l.switchLogsCallbackCtrl.Unregister(ctx); err != nil {
			return errors.Wrap(err, "failed to unregister commitlog switch from maintenance cycle")
		}
	}
	if l.maintainLogsCallbackCtrl != nil {
		if err := l.maintainLogsCallbackCtrl.Unregister(ctx); err != nil {
			return errors.Wrap(err, "failed to unregister commitlog compaction from maintenance cycle")
		}
	}
	return nil
}

func (l *hnswCommitLogger) RootPath() string {
	return l.rootPath
}

func (l *hnswCommitLogger) startSwitchLogs(shouldAbort cyclemanager.ShouldAbortCallback) bool {
	executed, err := l.switchCommitLogs(false)
	if err != nil {
		l.logger.WithError(err).
			WithField("action", "hnsw_commit_log_switch").
			WithField("file", l.rootPath).
			WithField("id", l.id).
			Error("hnsw commit log switch failed")
	}
	return executed
}

func (l *hnswCommitLogger) startCommitLogsMaintenance(shouldAbort cyclemanager.ShouldAbortCallback) bool {
	action, err := l.compactor.RunCycle()
	if err != nil {
		l.logger.WithError(err).
			WithField("action", "hnsw_commit_log_compaction").
			WithField("file", l.rootPath).
			WithField("id", l.id).
			Error("hnsw commit log maintenance (compaction) failed")
	}

	return action != compact.ActionNone
}

func (l *hnswCommitLogger) PrepareForBackup(force bool) error {
	_, err := l.switchCommitLogs(force)
	return err
}

func (l *hnswCommitLogger) ResumeAfterBackup(ctx context.Context) error {
	// nothing to do, as we always write to new files and never modify existing ones, so backup files are always consistent and up-to-date
	return nil
}

func (l *hnswCommitLogger) switchCommitLogs(force bool) (bool, error) {
	l.Lock()
	defer l.Unlock()

	// Get current file size
	info, err := l.currentFile.Stat()
	if err != nil {
		return false, errors.Wrap(err, "get file info")
	}
	size := info.Size()

	if size <= l.maxSizeIndividual && !force {
		return false, nil
	}

	oldFileName := l.currentFileName

	// Flush and close current file
	if err := l.currentWriter.Flush(); err != nil {
		return true, errors.Wrap(err, "flush commit log")
	}
	if err := l.currentFile.Sync(); err != nil {
		return true, errors.Wrap(err, "sync commit log")
	}
	if err := l.currentFile.Close(); err != nil {
		return true, errors.Wrap(err, "close commit log")
	}

	// Create new file with timestamp name
	fileName := fmt.Sprintf("%d", time.Now().Unix())

	if force {
		l.logger.WithField("action", "commit_log_file_switched").
			WithField("id", l.id).
			WithField("old_file_name", oldFileName).
			WithField("old_file_size", size).
			WithField("new_file_name", fileName).
			Debug("commit log switched forced")
	} else {
		l.logger.WithField("action", "commit_log_file_switched").
			WithField("id", l.id).
			WithField("old_file_name", oldFileName).
			WithField("old_file_size", size).
			WithField("new_file_name", fileName).
			Info("commit log size crossed threshold, switching to new file")
	}

	filePath := commitLogFileName(l.rootPath, l.id, fileName)
	fd, err := l.fs.OpenFile(filePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o666)
	if err != nil {
		return true, errors.Wrap(err, "create commit log file")
	}

	l.currentFile = fd
	l.currentFileName = filePath
	l.currentWriter = bufio.NewWriterSize(fd, 32*1024)
	l.walWriter = compact.NewWALWriter(l.currentWriter)

	return true, nil
}

func (l *hnswCommitLogger) Drop(ctx context.Context, keepFiles bool) error {
	l.Lock()
	defer l.Unlock()

	// Flush and close the file
	if err := l.currentWriter.Flush(); err != nil {
		return errors.Wrap(err, "flush hnsw commit logger prior to delete")
	}
	if err := l.currentFile.Close(); err != nil {
		return errors.Wrap(err, "close hnsw commit logger prior to delete")
	}

	// stop all goroutines
	if err := l.Shutdown(ctx); err != nil {
		return errors.Wrap(err, "drop commitlog")
	}

	if keepFiles {
		return nil
	}

	// remove commit log directory if exists
	dir := commitLogDirectory(l.rootPath, l.id)
	if _, err := l.fs.Stat(dir); err == nil {
		if err := l.fs.RemoveAll(dir); err != nil {
			return errors.Wrap(err, "delete commit files directory")
		}
	}

	return nil
}

func (l *hnswCommitLogger) Flush() error {
	l.Lock()
	defer l.Unlock()

	if err := l.currentWriter.Flush(); err != nil {
		return errors.Wrap(err, "flush commit log buffer")
	}
	return l.currentFile.Sync()
}
