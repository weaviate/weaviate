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
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/compactv2"
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

	// File management
	currentFile   *os.File
	currentWriter *bufio.Writer
	walWriter     *compactv2.WALWriter

	// Compaction (replaces condensor + combiner)
	compactor *compactv2.Compactor

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
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return nil, errors.Wrap(err, "create commit logger directory")
	}

	// Create or open the current commit log file
	fd, err := getLatestCommitFileOrCreate(rootPath, name)
	if err != nil {
		return nil, err
	}

	l.currentFile = fd
	l.currentWriter = bufio.NewWriter(fd)
	l.walWriter = compactv2.NewWALWriter(l.currentWriter)

	// Create compactor for maintenance
	l.compactor = compactv2.NewCompactor(
		compactv2.DefaultCompactorConfig(dir),
		logger,
	)

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

func getLatestCommitFileOrCreate(rootPath, name string) (*os.File, error) {
	dir := commitLogDirectory(rootPath, name)
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return nil, errors.Wrap(err, "create commit logger directory")
	}

	fileName, ok, err := getCurrentCommitLogFileName(dir)
	if err != nil {
		return nil, errors.Wrap(err, "find commit logger file in directory")
	}

	if !ok {
		// this is a new commit log, initialize with the current time stamp
		fileName = fmt.Sprintf("%d", time.Now().Unix())
	}

	fd, err := os.OpenFile(commitLogFileName(rootPath, name, fileName),
		os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o666)
	if err != nil {
		return nil, errors.Wrap(err, "create commit log file")
	}

	return fd, nil
}

// getCurrentCommitLogFileName returns the fileName and true if a file was
// present. If no file was present, the second arg is false.
func getCurrentCommitLogFileName(dirPath string) (string, bool, error) {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return "", false, errors.Wrap(err, "browse commit logger directory")
	}

	// Filter and find the most recent file
	var latestName string
	var latestTS int64

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()

		// Skip hidden files and temp files
		if strings.HasPrefix(name, ".") {
			continue
		}
		if strings.HasSuffix(name, ".tmp") {
			continue
		}

		// Parse timestamp from filename
		ts, err := parseTimestamp(name)
		if err != nil {
			continue
		}

		if ts > latestTS {
			latestTS = ts
			latestName = name
		}
	}

	if latestName == "" {
		return "", false, nil
	}

	return latestName, true, nil
}

// parseTimestamp extracts the start timestamp from a commit log filename.
// Handles formats like: "1234567890", "1234567890.condensed", "1234567890.sorted",
// and range formats like "1234567890_1234567891.sorted".
func parseTimestamp(name string) (int64, error) {
	// Remove known suffixes
	baseName := name
	for _, suffix := range []string{".condensed", ".sorted", ".snapshot"} {
		baseName = strings.TrimSuffix(baseName, suffix)
	}

	// Handle range format: {start}_{end}
	if idx := strings.Index(baseName, "_"); idx != -1 {
		baseName = baseName[:idx]
	}

	return strconv.ParseInt(baseName, 10, 64)
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

	return action != compactv2.ActionNone
}

func (l *hnswCommitLogger) SwitchCommitLogs(force bool) error {
	_, err := l.switchCommitLogs(force)
	return err
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

	oldFileName := l.currentFile.Name()

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

	fd, err := os.OpenFile(commitLogFileName(l.rootPath, l.id, fileName),
		os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o666)
	if err != nil {
		return true, errors.Wrap(err, "create commit log file")
	}

	l.currentFile = fd
	l.currentWriter = bufio.NewWriter(fd)
	l.walWriter = compactv2.NewWALWriter(l.currentWriter)

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

	// remove commit log directory if exists
	dir := commitLogDirectory(l.rootPath, l.id)
	if _, err := os.Stat(dir); err == nil && !keepFiles {
		err := os.RemoveAll(dir)
		if err != nil {
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
