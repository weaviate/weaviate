//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package hnsw

import (
	"context"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/commitlog"
	"github.com/semi-technologies/weaviate/entities/cyclemanager"
	"github.com/sirupsen/logrus"
)

const defaultCommitLogSize = 500 * 1024 * 1024

func commitLogFileName(rootPath, indexName, fileName string) string {
	return fmt.Sprintf("%s/%s", commitLogDirectory(rootPath, indexName), fileName)
}

func commitLogDirectory(rootPath, name string) string {
	return fmt.Sprintf("%s/%s.hnsw.commitlog.d", rootPath, name)
}

func NewCommitLogger(rootPath, name string,
	maintainenceInterval time.Duration, logger logrus.FieldLogger,
	opts ...CommitlogOption) (*hnswCommitLogger, error) {
	l := &hnswCommitLogger{
		rootPath:             rootPath,
		id:                   name,
		maintainenceInterval: maintainenceInterval,
		condensor:            NewMemoryCondensor(logger),
		logger:               logger,

		// both can be overwritten using functional options
		maxSizeIndividual: defaultCommitLogSize / 5,
		maxSizeCombining:  defaultCommitLogSize,
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

	l.switchLogCycle = cyclemanager.New(l.maintainenceInterval, l.startSwitchLogs)
	l.condenseCycle = cyclemanager.New(l.maintainenceInterval, l.startCombineAndCondenseLogs)

	l.commitLogger = commitlog.NewLoggerWithFile(fd)
	l.Start()
	return l, nil
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
func getCommitFileNames(rootPath, name string) ([]string, error) {
	dir := commitLogDirectory(rootPath, name)
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return nil, errors.Wrap(err, "create commit logger directory")
	}

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, errors.Wrap(err, "browse commit logger directory")
	}

	files = removeTmpScratchFiles(files)
	files, err = removeTmpCombiningFiles(dir, files)
	if err != nil {
		return nil, errors.Wrap(err, "remove temporary files")
	}

	if len(files) == 0 {
		return nil, nil
	}

	ec := &errorCompounder{}
	sort.Slice(files, func(a, b int) bool {
		ts1, err := asTimeStamp(files[a].Name())
		if err != nil {
			ec.add(err)
		}

		ts2, err := asTimeStamp(files[b].Name())
		if err != nil {
			ec.add(err)
		}
		return ts1 < ts2
	})
	if err := ec.toError(); err != nil {
		return nil, err
	}

	out := make([]string, len(files))
	for i, file := range files {
		out[i] = commitLogFileName(rootPath, name, file.Name())
	}

	return out, nil
}

// getCurrentCommitLogFileName returns the fileName and true if a file was
// present. If no file was present, the second arg is false.
func getCurrentCommitLogFileName(dirPath string) (string, bool, error) {
	files, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return "", false, errors.Wrap(err, "browse commit logger directory")
	}

	if len(files) == 0 {
		return "", false, nil
	}

	files = removeTmpScratchFiles(files)
	files, err = removeTmpCombiningFiles(dirPath, files)
	if err != nil {
		return "", false, errors.Wrap(err, "clean up tmp combining files")
	}

	ec := &errorCompounder{}
	sort.Slice(files, func(a, b int) bool {
		ts1, err := asTimeStamp(files[a].Name())
		if err != nil {
			ec.add(err)
		}

		ts2, err := asTimeStamp(files[b].Name())
		if err != nil {
			ec.add(err)
		}
		return ts1 > ts2
	})
	if err := ec.toError(); err != nil {
		return "", false, err
	}

	return files[0].Name(), true, nil
}

func removeTmpScratchFiles(in []fs.FileInfo) []fs.FileInfo {
	out := make([]fs.FileInfo, len(in))
	i := 0
	for _, info := range in {
		if strings.HasSuffix(info.Name(), ".scratch.tmp") {
			continue
		}

		out[i] = info
		i++
	}

	return out[:i]
}

func removeTmpCombiningFiles(dirPath string,
	in []fs.FileInfo) ([]fs.FileInfo, error) {
	out := make([]fs.FileInfo, len(in))
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

func asTimeStamp(in string) (int64, error) {
	return strconv.ParseInt(strings.TrimSuffix(in, ".condensed"), 10, 64)
}

type condensor interface {
	Do(filename string) error
}

type hnswCommitLogger struct {
	// protect against concurrent attempts to write in the underlying file or
	// buffer
	sync.Mutex

	rootPath             string
	id                   string
	condensor            condensor
	logger               logrus.FieldLogger
	maxSizeIndividual    int64
	maxSizeCombining     int64
	commitLogger         *commitlog.Logger
	maintainenceInterval time.Duration

	switchLogCycle *cyclemanager.CycleManager
	condenseCycle  *cyclemanager.CycleManager
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
	ClearLinksAtLevel // added in v1.8.0-rc.1, see https://github.com/semi-technologies/weaviate/issues/1701
	AddLinksAtLevel   // added in v1.8.0-rc.1, see https://github.com/semi-technologies/weaviate/issues/1705
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
	}
	return "unknown commit type"
}

func (l *hnswCommitLogger) ID() string {
	return l.id
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
	target uint64) error {
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

func (l *hnswCommitLogger) Start() {
	l.switchLogCycle.Start()
	l.condenseCycle.Start()
}

// Shutdown waits for ongoing maintenance processes to stop, then cancels their
// scheduling. The caller can be sure that state on disk is immutable after
// calling Shutdown().
func (l *hnswCommitLogger) Shutdown(ctx context.Context) error {
	switchLogCycleStop := make(chan error)
	condenseCycleStop := make(chan error)

	go func() {
		if err := l.switchLogCycle.StopAndWait(ctx); err != nil {
			switchLogCycleStop <- errors.Wrap(err, "failed to stop commitlog switch cycle")
			return
		}
		switchLogCycleStop <- nil
	}()

	go func() {
		if err := l.condenseCycle.StopAndWait(ctx); err != nil {
			condenseCycleStop <- errors.Wrap(err, "failed to stop commitlog condense cycle")
			return
		}
		condenseCycleStop <- nil
	}()

	switchLogCycleStopErr := <-switchLogCycleStop
	condenseCycleStopErr := <-condenseCycleStop

	if switchLogCycleStopErr != nil && condenseCycleStopErr != nil {
		return errors.Errorf("%s, %s", switchLogCycleStopErr, condenseCycleStopErr)
	}

	if switchLogCycleStopErr != nil {
		// restart condense cycle since it was successfully stopped.
		// both of these cycles work together, and need to work in sync
		l.condenseCycle.Start()
		return switchLogCycleStopErr
	}

	if condenseCycleStopErr != nil {
		// restart switch log cycle since it was successfully stopped.
		// both of these cycles work together, and need to work in sync
		l.switchLogCycle.Start()
		return condenseCycleStopErr
	}

	return nil
}

func (l *hnswCommitLogger) RootPath() string {
	return l.rootPath
}

func (l *hnswCommitLogger) startSwitchLogs() {
	if err := l.SwitchCommitLogs(false); err != nil {
		l.logger.WithError(err).
			WithField("action", "hsnw_commit_log_maintenance").
			Error("hnsw commit log maintenance failed")
	}
}

func (l *hnswCommitLogger) startCombineAndCondenseLogs() {
	if err := l.combineLogs(); err != nil {
		l.logger.WithError(err).
			WithField("action", "hsnw_commit_log_combining").
			Error("hnsw commit log maintenance (combining) failed")
	}

	if err := l.condenseOldLogs(); err != nil {
		l.logger.WithError(err).
			WithField("action", "hsnw_commit_log_condensing").
			Error("hnsw commit log maintenance (condensing) failed")
	}
}

func (l *hnswCommitLogger) SwitchCommitLogs(force bool) error {
	l.Lock()
	defer l.Unlock()

	size, err := l.commitLogger.FileSize()
	if err != nil {
		return err
	}

	if size <= l.maxSizeIndividual && !force {
		return nil
	}

	oldFileName, err := l.commitLogger.FileName()
	if err != nil {
		return err
	}

	if err := l.commitLogger.Close(); err != nil {
		return err
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
		return errors.Wrap(err, "create commit log file")
	}

	l.commitLogger = commitlog.NewLoggerWithFile(fd)

	return nil
}

func (l *hnswCommitLogger) condenseOldLogs() error {
	files, err := getCommitFileNames(l.rootPath, l.id)
	if err != nil {
		return err
	}

	if len(files) <= 1 {
		// if there are no files there is nothing to do
		// if there is only a single file, it must still be in use, we can't do
		// anything yet
		return nil
	}

	// cut off last element, as that's never a candidate
	candidates := files[:len(files)-1]

	for _, candidate := range candidates {
		if strings.HasSuffix(candidate, ".condensed") {
			// don't attempt to condense logs which are already condensed
			continue
		}

		return l.condensor.Do(candidate)
	}

	return nil
}

func (l *hnswCommitLogger) combineLogs() error {
	// maxSize is the desired final size, since we assume a lot of redunancy we
	// can set the combining threshold higher than the final threshold under the
	// assumption that the combined file will be considerably smaller than the
	// sum of both input files
	threshold := int64(float64(l.maxSizeCombining) * 1.75)
	return NewCommitLogCombiner(l.rootPath, l.id, threshold, l.logger).Do()
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
