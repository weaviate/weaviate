//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package hnsw

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
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
		events:               make(chan []byte),
		writeErrors:          make(chan error),
		cancel:               make(chan struct{}),
		rootPath:             rootPath,
		id:                   name,
		maintainenceInterval: maintainenceInterval,
		condensor:            NewMemoryCondensor(logger),
		logger:               logger,
		maxSize:              maxUncondensedCommitLogSize, // TODO: make configurable
	}

	fd, err := getLatestCommitFileOrCreate(rootPath, name)
	if err != nil {
		return nil, err
	}
	l.logFile = fd

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

func asTimeStamp(in string) (int64, error) {
	return strconv.ParseInt(strings.TrimSuffix(in, ".condensed"), 10, 64)
}

type condensor interface {
	Do(filename string) error
}

type hnswCommitLogger struct {
	events               chan []byte
	writeErrors          chan error
	cancel               chan struct{}
	logFile              *os.File
	rootPath             string
	id                   string
	condensor            condensor
	maintainenceInterval time.Duration
	logger               logrus.FieldLogger
	maxSize              int64
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
	w := &bytes.Buffer{}
	l.writeCommitType(w, AddNode)
	l.writeUint64(w, node.id)
	l.writeUint16(w, uint16(node.level))

	l.events <- w.Bytes()
	err := <-l.writeErrors
	if err != nil {
		return errors.Wrapf(err, "write node %d to commit log", node.id)
	}

	return nil
}

func (l *hnswCommitLogger) SetEntryPointWithMaxLayer(id uint64, level int) error {
	w := &bytes.Buffer{}
	l.writeCommitType(w, SetEntryPointMaxLevel)
	l.writeUint64(w, id)
	l.writeUint16(w, uint16(level))

	l.events <- w.Bytes()
	err := <-l.writeErrors
	if err != nil {
		return errors.Wrapf(err, "write entrypoint %d (%d) to commit log", id, level)
	}

	return nil
}

func (l *hnswCommitLogger) AddLinkAtLevel(nodeid uint64, level int, target uint64) error {
	w := &bytes.Buffer{}
	l.writeCommitType(w, AddLinkAtLevel)
	l.writeUint64(w, nodeid)
	l.writeUint16(w, uint16(level))
	l.writeUint64(w, target)

	l.events <- w.Bytes()
	err := <-l.writeErrors
	if err != nil {
		return errors.Wrapf(err, "write link at level %d->%d (%d) to commit log",
			nodeid, target, level)
	}

	return nil
}

func (l *hnswCommitLogger) ReplaceLinksAtLevel(nodeid uint64, level int, targets []uint64) error {
	w := &bytes.Buffer{}
	l.writeCommitType(w, ReplaceLinksAtLevel)
	l.writeUint64(w, nodeid)
	l.writeUint16(w, uint16(level))
	targetLength := len(targets)
	if targetLength > math.MaxUint16 {
		// TODO: investigate why we get such massive connections
		targetLength = math.MaxUint16
		l.logger.WithField("action", "hnsw_current_commit_log").
			WithField("id", l.id).
			WithField("original_length", len(targets)).
			WithField("maximum_length", targetLength).
			Warning("condensor length of connections would overflow uint16, cutting off")
	}
	l.writeUint16(w, uint16(targetLength))
	l.writeUint64Slice(w, targets[:targetLength])

	l.events <- w.Bytes()
	err := <-l.writeErrors
	if err != nil {
		return errors.Wrapf(err,
			"write (replacement) links at level %d->%v (%d) to commit log", nodeid,
			targets, level)
	}

	return nil
}

func (l *hnswCommitLogger) AddTombstone(nodeid uint64) error {
	w := &bytes.Buffer{}
	l.writeCommitType(w, AddTombstone)
	l.writeUint64(w, nodeid)

	l.events <- w.Bytes()
	err := <-l.writeErrors
	if err != nil {
		return errors.Wrapf(err,
			"write tombstone %d to commit log", nodeid)
	}

	return nil
}

func (l *hnswCommitLogger) RemoveTombstone(nodeid uint64) error {
	w := &bytes.Buffer{}
	l.writeCommitType(w, RemoveTombstone)
	l.writeUint64(w, nodeid)

	l.events <- w.Bytes()
	err := <-l.writeErrors
	if err != nil {
		return errors.Wrapf(err,
			"write deletion of tombstone %d to commit log", nodeid)
	}

	return nil
}

func (l *hnswCommitLogger) ClearLinks(nodeid uint64) error {
	w := &bytes.Buffer{}
	l.writeCommitType(w, ClearLinks)
	l.writeUint64(w, nodeid)

	l.events <- w.Bytes()
	err := <-l.writeErrors
	if err != nil {
		return errors.Wrapf(err,
			"write clear links of node %d to commit log", nodeid)
	}

	return nil
}

func (l *hnswCommitLogger) DeleteNode(nodeid uint64) error {
	w := &bytes.Buffer{}
	l.writeCommitType(w, DeleteNode)
	l.writeUint64(w, nodeid)

	l.events <- w.Bytes()
	err := <-l.writeErrors
	if err != nil {
		return errors.Wrapf(err,
			"write delete node %d to commit log", nodeid)
	}

	return nil
}

func (l *hnswCommitLogger) Reset() error {
	w := &bytes.Buffer{}
	l.writeCommitType(w, ResetIndex)

	l.events <- w.Bytes()
	err := <-l.writeErrors
	if err != nil {
		return errors.Wrap(err, "reset to commit log")
	}

	return nil
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
			case event := <-l.events:
				_, err := l.logFile.Write(event)
				l.writeErrors <- err
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
	i, err := l.logFile.Stat()
	if err != nil {
		return err
	}

	if i.Size() > l.maxSize {
		l.logFile.Close()

		// this is a new commit log, initialize with the current time stamp
		fileName := fmt.Sprintf("%d", time.Now().Unix())

		l.logger.WithField("action", "commit_log_file_switched").
			WithField("id", l.id).
			WithField("old_file_name", i.Name()).
			WithField("old_file_size", i.Size()).
			WithField("new_file_name", fileName).
			Info("commit log size crossed threshold, switching to new file")

		fd, err := os.OpenFile(commitLogFileName(l.rootPath, l.id, fileName),
			os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o666)
		if err != nil {
			return errors.Wrap(err, "create commit log file")
		}

		l.logFile = fd
	}

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

func (l *hnswCommitLogger) writeUint64(w io.Writer, in uint64) error {
	err := binary.Write(w, binary.LittleEndian, &in)
	if err != nil {
		return errors.Wrap(err, "writing uint64")
	}

	return nil
}

func (l *hnswCommitLogger) writeUint16(w io.Writer, in uint16) error {
	err := binary.Write(w, binary.LittleEndian, &in)
	if err != nil {
		return errors.Wrap(err, "writing uint16")
	}

	return nil
}

func (l *hnswCommitLogger) writeCommitType(w io.Writer, in HnswCommitType) error {
	err := binary.Write(w, binary.LittleEndian, &in)
	if err != nil {
		return errors.Wrap(err, "writing commit type")
	}

	return nil
}

func (l *hnswCommitLogger) writeUint64Slice(w io.Writer, in []uint64) error {
	err := binary.Write(w, binary.LittleEndian, &in)
	if err != nil {
		return errors.Wrap(err, "writing []uint64")
	}

	return nil
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
