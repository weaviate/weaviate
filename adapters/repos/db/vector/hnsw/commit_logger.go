//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package hnsw

import (
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/commitlog"
	"github.com/sirupsen/logrus"
)

const maxUncondensedCommitLogSize = 500 * 1024 * 1024

func commitLogFileName(rootPath, indexName, fileName string) string {
	return fmt.Sprintf("%s/%s", commitLogDirectory(rootPath, indexName), fileName)
}

func commitLogDirectory(rootPath, name string) string {
	return fmt.Sprintf("%s/%s.hnsw.commitlog.d", rootPath, name)
}

func NewCommitLogger(rootPath, name string,
	maintainenceInterval time.Duration,
	logger logrus.FieldLogger) (*hnswCommitLogger, error) {
	l := &hnswCommitLogger{
		cancel:               make(chan struct{}),
		rootPath:             rootPath,
		id:                   name,
		maintainenceInterval: maintainenceInterval,
		condensor:            NewMemoryCondensor2(logger),
		logger:               logger,
		maxSize:              maxUncondensedCommitLogSize, // TODO: make configurable
	}

	fd, err := getLatestCommitFileOrCreate(rootPath, name)
	if err != nil {
		return nil, err
	}

	l.commitLogger = commitlog.NewLoggerWithFile(fd)
	l.StartLogging()
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

func asTimeStamp(in string) (int64, error) {
	return strconv.ParseInt(strings.TrimSuffix(in, ".condensed"), 10, 64)
}

type condensor interface {
	Do(filename string) error
}

type hnswCommitLogger struct {
	sync.Mutex
	cancel               chan struct{}
	rootPath             string
	id                   string
	condensor            condensor
	maintainenceInterval time.Duration
	logger               logrus.FieldLogger
	maxSize              int64
	commitLogger         *commitlog.Logger
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
)

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

func (l *hnswCommitLogger) StartLogging() {
	// switch log job
	cancelSwitchLog := l.startSwitchLogs()
	// condense old logs job
	cancelCombineAndCondenseLogs := l.startCombineAndCondenseLogs()
	// cancel maintenance jobs on request
	go func(cancel ...chan struct{}) {
		<-l.cancel
		for _, c := range cancel {
			c <- struct{}{}
		}
	}(cancelCombineAndCondenseLogs, cancelSwitchLog)
}

func (l *hnswCommitLogger) startSwitchLogs() chan struct{} {
	cancelSwitchLog := make(chan struct{})

	go func(cancel <-chan struct{}) {
		if l.maintainenceInterval == 0 {
			l.logger.WithField("action", "commit_logging_skipped").
				WithField("id", l.id).
				Info("commit log switching explitictly turned off")
		}
		maintenance := time.Tick(l.maintainenceInterval)

		for {
			select {
			case <-cancel:
				return
			case <-maintenance:
				if err := l.maintenance(); err != nil {
					l.logger.WithError(err).
						WithField("action", "hsnw_commit_log_maintenance").
						Error("hnsw commit log maintenance failed")
				}
			}
		}
	}(cancelSwitchLog)

	return cancelSwitchLog
}

func (l *hnswCommitLogger) startCombineAndCondenseLogs() chan struct{} {
	cancelFromOutside := make(chan struct{})

	go func(cancel <-chan struct{}) {
		if l.maintainenceInterval == 0 {
			l.logger.WithField("action", "commit_logging_skipped").
				WithField("id", l.id).
				Info("commit log switching explitictly turned off")
		}
		maintenance := time.Tick(l.maintainenceInterval)
		for {
			select {
			case <-cancel:
				return
			case <-maintenance:
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
		}
	}(cancelFromOutside)

	return cancelFromOutside
}

func (l *hnswCommitLogger) maintenance() error {
	l.Lock()
	defer l.Unlock()

	size, err := l.commitLogger.FileSize()
	if err != nil {
		return err
	}

	if size <= l.maxSize {
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

	l.logger.WithField("action", "commit_log_file_switched").
		WithField("id", l.id).
		WithField("old_file_name", oldFileName).
		WithField("old_file_size", size).
		WithField("new_file_name", fileName).
		Info("commit log size crossed threshold, switching to new file")

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
	threshold := int64(float64(l.maxSize) * 1.75)
	return NewCommitLogCombiner(l.rootPath, l.id, threshold, l.logger).Do()
}

func (l *hnswCommitLogger) Drop() error {
	// stop all goroutines
	l.cancel <- struct{}{}
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
