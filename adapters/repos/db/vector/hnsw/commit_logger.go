//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/commitlog"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/multivector"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

const defaultCommitLogSize = 500 * 1024 * 1024

type hnswCommitLogger struct {
	// protect against concurrent attempts to write in the underlying file or
	// buffer
	sync.Mutex

	rootPath          string
	id                string
	condensor         Condensor
	logger            logrus.FieldLogger
	maxSizeIndividual int64
	maxSizeCombining  int64
	commitLogger      *commitlog.Logger

	switchLogsCallbackCtrl   cyclemanager.CycleCallbackCtrl
	maintainLogsCallbackCtrl cyclemanager.CycleCallbackCtrl
	maintenanceCallbacks     cyclemanager.CycleCallbackGroup

	allocChecker memwatch.AllocChecker

	snapshotLogger logrus.FieldLogger
	// whether snapshots are disabled
	snapshotDisabled bool
	// minimum interval to create next snapshot out of last one and new commitlogs, 0 = no periodic snapshots
	snapshotCreateInterval time.Duration
	// minimal interval to check if next snapshot should be created
	snapshotCheckInterval time.Duration
	// minimum number of delta commitlogs required to create new snapshot
	snapshotMinDeltaCommitlogsNumber int
	// minimum percentage size of delta commitlogs (compared to last snapshot) required to create new one
	snapshotMinDeltaCommitlogsSizePercentage int
	// time that last snapshot was created at (based on its name, which is based on last included commitlog name;
	// not the actual snapshot file creation time)
	snapshotLastCreatedAt time.Time
	// time that last check if snapshot should be created was made
	snapshotLastCheckedAt time.Time
	// partitions mark commitlogs (left ones) that should not be combined with
	// logs on the right side (newer ones).
	// example: given logs 0001.condensed, 0002.condensed, 0003.condensed and 0004.condensed
	// with partition = "0002", only logs [older or equal 0002.condensed]
	// or [newer than 0002.condensed] can be combined with each other
	// (so 0001+0002 or 0003+0004, NOT 0002+0003)
	// partitions are commitlog filenames (no path, no extension)
	snapshotPartitions []string
}

func NewCommitLogger(rootPath, name string, logger logrus.FieldLogger,
	maintenanceCallbacks cyclemanager.CycleCallbackGroup, opts ...CommitlogOption,
) (*hnswCommitLogger, error) {
	l := &hnswCommitLogger{
		rootPath:             rootPath,
		id:                   name,
		condensor:            NewMemoryCondensor(logger),
		logger:               logger,
		maintenanceCallbacks: maintenanceCallbacks,

		// both can be overwritten using functional options
		maxSizeIndividual: defaultCommitLogSize / 5,
		maxSizeCombining:  defaultCommitLogSize,

		snapshotMinDeltaCommitlogsNumber:         1,
		snapshotMinDeltaCommitlogsSizePercentage: 0,
	}

	for _, o := range opts {
		if err := o(l); err != nil {
			return nil, err
		}
	}

	fd, err := getLatestCommitFileOrCreate(rootPath, name)
	if err != nil {
		return nil, err
	}
	l.commitLogger = commitlog.NewLoggerWithFile(fd)

	if err := l.initSnapshotData(); err != nil {
		return nil, errors.Wrapf(err, "init snapshot data")
	}

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

// getCommitFileNames in order, from old to new
func getCommitFileNames(rootPath, id string, createdAfter int64) ([]string, error) {
	files, err := getCommitFiles(rootPath, id, createdAfter)
	if err != nil {
		return nil, err
	}
	return commitLogFileNames(rootPath, id, files), nil
}

func getCommitFiles(rootPath, id string, createdAfter int64) ([]os.DirEntry, error) {
	dir := commitLogDirectory(rootPath, id)
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return nil, errors.Wrap(err, "create commit logger directory")
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, errors.Wrap(err, "browse commit logger directory")
	}

	files = skipTmpScratchOrHiddenFiles(files)
	files, err = removeTmpCombiningFiles(dir, files)
	if err != nil {
		return nil, errors.Wrap(err, "clean up tmp combining files")
	}

	if createdAfter > 0 {
		files, err = filterNewerCommitLogFiles(files, createdAfter)
		if err != nil {
			return nil, errors.Wrap(err, "remove old commit files")
		}
	}

	if len(files) == 0 {
		return nil, nil
	}

	ec := errorcompounder.New()
	sort.Slice(files, func(a, b int) bool {
		ts1, err := asTimeStamp(files[a].Name())
		if err != nil {
			ec.Add(err)
		}

		ts2, err := asTimeStamp(files[b].Name())
		if err != nil {
			ec.Add(err)
		}
		return ts1 < ts2
	})
	if err := ec.ToError(); err != nil {
		return nil, err
	}

	return files, nil
}

func commitLogFileNames(rootPath, id string, files []os.DirEntry) []string {
	out := make([]string, len(files))
	for i, file := range files {
		out[i] = commitLogFileName(rootPath, id, file.Name())
	}
	return out
}

// getCurrentCommitLogFileName returns the fileName and true if a file was
// present. If no file was present, the second arg is false.
func getCurrentCommitLogFileName(dirPath string) (string, bool, error) {
	files, err := os.ReadDir(dirPath)
	if err != nil {
		return "", false, errors.Wrap(err, "browse commit logger directory")
	}

	if len(files) > 0 {
		files = skipTmpScratchOrHiddenFiles(files)
		files, err = removeTmpCombiningFiles(dirPath, files)
		if err != nil {
			return "", false, errors.Wrap(err, "clean up tmp combining files")
		}
	}

	if len(files) == 0 {
		return "", false, nil
	}

	ec := errorcompounder.New()
	sort.Slice(files, func(a, b int) bool {
		ts1, err := asTimeStamp(files[a].Name())
		if err != nil {
			ec.Add(err)
		}

		ts2, err := asTimeStamp(files[b].Name())
		if err != nil {
			ec.Add(err)
		}
		return ts1 > ts2
	})
	if err := ec.ToError(); err != nil {
		return "", false, err
	}

	return files[0].Name(), true, nil
}

func skipTmpScratchOrHiddenFiles(in []os.DirEntry) []os.DirEntry {
	out := make([]os.DirEntry, len(in))
	i := 0
	for _, entry := range in {
		if strings.HasSuffix(entry.Name(), ".scratch.tmp") {
			continue
		}

		if strings.HasPrefix(entry.Name(), ".") {
			continue
		}

		out[i] = entry
		i++
	}
	return out[:i]
}

func skipEmptyFiles(in []os.DirEntry) ([]os.DirEntry, error) {
	out := make([]os.DirEntry, len(in))
	i := 0
	for _, entry := range in {
		info, err := entry.Info()
		if err != nil {
			return nil, errors.Wrap(err, "get file info")
		}
		if info.Size() == 0 {
			continue
		}
		out[i] = entry
		i++
	}
	return out[:i], nil
}

func removeTmpCombiningFiles(dirPath string, in []os.DirEntry) ([]os.DirEntry, error) {
	out := make([]os.DirEntry, len(in))
	i := 0

	for _, info := range in {
		if strings.HasSuffix(info.Name(), ".combined.tmp") {
			// a temporary combining file was found which means that the combining
			// process never completed, this file is thus considered corrupt (too
			// short) and must be deleted. The original sources still exist (because
			// the only get deleted after the .tmp file is removed), so it's safe to
			// delete this without data loss.

			if err := os.Remove(filepath.Join(dirPath, info.Name())); err != nil {
				return out, errors.Wrap(err, "remove tmp combining file")
			}
			continue
		}

		out[i] = info
		i++
	}

	return out[:i], nil
}

func filterNewerCommitLogFiles(in []os.DirEntry, createdAfter int64) ([]os.DirEntry, error) {
	out := make([]os.DirEntry, len(in))
	i := 0
	for _, entry := range in {
		ts, err := asTimeStamp(entry.Name())
		if err != nil {
			return nil, errors.Wrapf(err, "read commitlog timestamp %q", entry.Name())
		}

		if ts <= createdAfter {
			continue
		}

		out[i] = entry
		i++
	}

	return out[:i], nil
}

func asTimeStamp(in string) (int64, error) {
	return strconv.ParseInt(strings.TrimSuffix(in, ".condensed"), 10, 64)
}

type Condensor interface {
	Do(filename string) error
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
	}
	return "unknown commit type"
}

func (l *hnswCommitLogger) ID() string {
	return l.id
}

func (l *hnswCommitLogger) AddPQCompression(data compressionhelpers.PQData) error {
	l.Lock()
	defer l.Unlock()

	return l.commitLogger.AddPQCompression(data)
}

func (l *hnswCommitLogger) AddSQCompression(data compressionhelpers.SQData) error {
	l.Lock()
	defer l.Unlock()

	return l.commitLogger.AddSQCompression(data)
}

func (l *hnswCommitLogger) AddMuvera(data multivector.MuveraData) error {
	l.Lock()
	defer l.Unlock()

	return l.commitLogger.AddMuvera(data)
}

// AddNode adds an empty node
func (l *hnswCommitLogger) AddNode(node *vertex) error {
	l.Lock()
	defer l.Unlock()

	return l.commitLogger.AddNode(node.id, node.level)
}

func (l *hnswCommitLogger) SetEntryPointWithMaxLayer(id uint64, level int) error {
	l.Lock()
	defer l.Unlock()

	return l.commitLogger.SetEntryPointWithMaxLayer(id, level)
}

func (l *hnswCommitLogger) ReplaceLinksAtLevel(nodeid uint64, level int, targets []uint64) error {
	l.Lock()
	defer l.Unlock()

	return l.commitLogger.ReplaceLinksAtLevel(nodeid, level, targets)
}

func (l *hnswCommitLogger) AddLinkAtLevel(nodeid uint64, level int,
	target uint64,
) error {
	l.Lock()
	defer l.Unlock()

	return l.commitLogger.AddLinkAtLevel(nodeid, level, target)
}

func (l *hnswCommitLogger) AddTombstone(nodeid uint64) error {
	l.Lock()
	defer l.Unlock()

	return l.commitLogger.AddTombstone(nodeid)
}

func (l *hnswCommitLogger) RemoveTombstone(nodeid uint64) error {
	l.Lock()
	defer l.Unlock()

	return l.commitLogger.RemoveTombstone(nodeid)
}

func (l *hnswCommitLogger) ClearLinks(nodeid uint64) error {
	l.Lock()
	defer l.Unlock()

	return l.commitLogger.ClearLinks(nodeid)
}

func (l *hnswCommitLogger) ClearLinksAtLevel(nodeid uint64, level uint16) error {
	l.Lock()
	defer l.Unlock()

	return l.commitLogger.ClearLinksAtLevel(nodeid, level)
}

func (l *hnswCommitLogger) DeleteNode(nodeid uint64) error {
	l.Lock()
	defer l.Unlock()

	return l.commitLogger.DeleteNode(nodeid)
}

func (l *hnswCommitLogger) Reset() error {
	l.Lock()
	defer l.Unlock()

	return l.commitLogger.Reset()
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
			return errors.Wrap(err, "failed to unregister commitlog condense from maintenance cycle")
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
			Error("hnsw commit log switch failed")
	}
	return executed
}

func (l *hnswCommitLogger) startCommitLogsMaintenance(shouldAbort cyclemanager.ShouldAbortCallback) bool {
	executedCombine, err := l.combineLogs()
	if err != nil {
		l.logger.WithError(err).
			WithField("action", "hnsw_commit_log_combining").
			Error("hnsw commit log maintenance (combining) failed")
	}

	executedCondense, err := l.condenseLogs()
	if err != nil {
		l.logger.WithError(err).
			WithField("action", "hnsw_commit_log_condensing").
			Error("hnsw commit log maintenance (condensing) failed")
	}

	executedSnapshot, err := l.createSnapshot(shouldAbort)
	if err != nil {
		l.logger.WithError(err).
			WithField("action", "hnsw_snapshot_creating").
			Error("hnsw commit log maintenance (snapshot) failed")
	}

	return executedCombine || executedCondense || executedSnapshot
}

func (l *hnswCommitLogger) SwitchCommitLogs(force bool) error {
	_, err := l.switchCommitLogs(force)
	return err
}

func (l *hnswCommitLogger) switchCommitLogs(force bool) (bool, error) {
	l.Lock()
	defer l.Unlock()

	size, err := l.commitLogger.FileSize()
	if err != nil {
		return false, err
	}

	if size <= l.maxSizeIndividual && !force {
		return false, nil
	}

	oldFileName, err := l.commitLogger.FileName()
	if err != nil {
		return false, err
	}

	if err := l.commitLogger.Close(); err != nil {
		return true, err
	}

	// this is a new commit log, initialize with the current time stamp
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

	l.commitLogger = commitlog.NewLoggerWithFile(fd)

	return true, nil
}

func (l *hnswCommitLogger) condenseLogs() (bool, error) {
	files, err := getCommitFileNames(l.rootPath, l.id, 0)
	if err != nil {
		return false, err
	}

	if len(files) <= 1 {
		// if there are no files there is nothing to do
		// if there is only a single file, it must still be in use, we can't do
		// anything yet
		return false, nil
	}

	// cut off last element, as that's never a candidate
	candidates := files[:len(files)-1]

	for _, candidate := range candidates {
		if strings.HasSuffix(candidate, ".condensed") {
			// don't attempt to condense logs which are already condensed
			continue
		}

		if l.allocChecker != nil {
			// allocChecker is optional, so we can only check this if it's actually set
			s, err := os.Stat(candidate)
			if err != nil {
				return false, fmt.Errorf("stat candidate file %q: %w", candidate, err)
			}

			// We're estimating here that the in-mem condensor needs about 1B of
			// memory for every byte of data in the log file. This estimate can
			// probably be refined.
			if err := l.allocChecker.CheckAlloc(s.Size()); err != nil {
				l.logger.WithFields(logrus.Fields{
					"action": "hnsw_commit_log_condensing",
					"event":  "condensing_skipped_oom",
					"path":   candidate,
					"size":   s.Size(),
				}).WithError(err).
					Warnf("skipping hnsw condensing due to memory pressure")
				return false, nil
			}

		}

		return true, l.condensor.Do(candidate)
	}

	return false, nil
}

func (l *hnswCommitLogger) combineLogs() (bool, error) {
	// maxSize is the desired final size, since we assume a lot of redundancy we
	// can set the combining threshold higher than the final threshold under the
	// assumption that the combined file will be considerably smaller than the
	// sum of both input files
	threshold := l.logCombiningThreshold()
	return NewCommitLogCombiner(l.rootPath, l.id, threshold, l.logger).Do(l.snapshotPartitions...)
}

// TODO al:snapshot handle should abort
func (l *hnswCommitLogger) createSnapshot(shouldAbort cyclemanager.ShouldAbortCallback) (bool, error) {
	if l.snapshotDisabled || l.snapshotCreateInterval <= 0 {
		return false, nil
	}

	if !time.Now().Add(-l.snapshotCheckInterval).After(l.snapshotLastCheckedAt) {
		return false, nil
	}
	l.snapshotLastCheckedAt = time.Now()

	if !time.Now().Add(-l.snapshotCreateInterval).After(l.snapshotLastCreatedAt) {
		return false, nil
	}

	created, _, err := l.CreateSnapshot()
	return created, err
}

func (l *hnswCommitLogger) logCombiningThreshold() int64 {
	return int64(float64(l.maxSizeCombining) * 1.75)
}

func (l *hnswCommitLogger) Drop(ctx context.Context) error {
	if err := l.commitLogger.Close(); err != nil {
		return errors.Wrap(err, "close hnsw commit logger prior to delete")
	}

	// stop all goroutines
	if err := l.Shutdown(ctx); err != nil {
		return errors.Wrap(err, "drop commitlog")
	}

	// remove commit log directory if exists
	dir := commitLogDirectory(l.rootPath, l.id)
	if _, err := os.Stat(dir); err == nil {
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

	return l.commitLogger.Flush()
}
