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

const maxUncondensedCommitLogSize = 50 * 1024 * 1024

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
		rootPath:             rootPath,
		id:                   name,
		maintainenceInterval: maintainenceInterval,
		condensor:            NewMemoryCondensor(logger),
		logger:               logger,
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
		os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
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
	logFile              *os.File
	rootPath             string
	id                   string
	condensor            condensor
	maintainenceInterval time.Duration
	logger               logrus.FieldLogger
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
	l.writeUint32(w, uint32(node.id))
	l.writeUint16(w, uint16(node.level))

	l.events <- w.Bytes()

	return nil
}

func (l *hnswCommitLogger) SetEntryPointWithMaxLayer(id int, level int) error {
	w := &bytes.Buffer{}
	l.writeCommitType(w, SetEntryPointMaxLevel)
	l.writeUint32(w, uint32(id))
	l.writeUint16(w, uint16(level))

	l.events <- w.Bytes()
	return nil
}

func (l *hnswCommitLogger) AddLinkAtLevel(nodeid int, level int, target uint32) error {
	w := &bytes.Buffer{}
	l.writeCommitType(w, AddLinkAtLevel)
	l.writeUint32(w, uint32(nodeid))
	l.writeUint16(w, uint16(level))
	l.writeUint32(w, target)

	l.events <- w.Bytes()
	return nil
}

func (l *hnswCommitLogger) ReplaceLinksAtLevel(nodeid int, level int, targets []uint32) error {
	w := &bytes.Buffer{}
	l.writeCommitType(w, ReplaceLinksAtLevel)
	l.writeUint32(w, uint32(nodeid))
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
	l.writeUint32Slice(w, targets[:targetLength])

	l.events <- w.Bytes()
	return nil
}

func (l *hnswCommitLogger) AddTombstone(nodeid int) error {
	w := &bytes.Buffer{}
	l.writeCommitType(w, AddTombstone)
	l.writeUint32(w, uint32(nodeid))

	l.events <- w.Bytes()
	return nil
}

func (l *hnswCommitLogger) RemoveTombstone(nodeid int) error {
	w := &bytes.Buffer{}
	l.writeCommitType(w, RemoveTombstone)
	l.writeUint32(w, uint32(nodeid))

	l.events <- w.Bytes()
	return nil
}

func (l *hnswCommitLogger) ClearLinks(nodeid int) error {
	w := &bytes.Buffer{}
	l.writeCommitType(w, ClearLinks)
	l.writeUint32(w, uint32(nodeid))

	l.events <- w.Bytes()
	return nil
}

func (l *hnswCommitLogger) DeleteNode(nodeid int) error {
	w := &bytes.Buffer{}
	l.writeCommitType(w, DeleteNode)
	l.writeUint32(w, uint32(nodeid))

	l.events <- w.Bytes()
	return nil
}

func (l *hnswCommitLogger) Reset() error {
	w := &bytes.Buffer{}
	l.writeCommitType(w, ResetIndex)

	l.events <- w.Bytes()
	return nil
}

func (l *hnswCommitLogger) StartLogging() {
	// switch log
	go func() {
		if l.maintainenceInterval == 0 {
			l.logger.WithField("action", "commit_logging_skipped").
				WithField("id", l.id).
				Info("commit log switching explitictly turned off")
		}
		maintenance := time.NewTicker(l.maintainenceInterval)

		for {
			select {
			case event := <-l.events:
				l.logFile.Write(event)
			case <-maintenance.C:
				if err := l.maintenance(); err != nil {
					l.logger.WithError(err).
						WithField("action", "hsnw_commit_log_maintenance").
						Error("hnsw commit log maintenance failed")
				}
			}
		}
	}()

	// condense old logs
	go func() {
		if l.maintainenceInterval == 0 {
			l.logger.WithField("action", "commit_logging_skipped").
				WithField("id", l.id).
				Info("commit log switching explitictly turned off")
		}
		maintenance := time.NewTicker(l.maintainenceInterval)
		for {
			<-maintenance.C
			if err := l.condenseOldLogs(); err != nil {
				l.logger.WithError(err).
					WithField("action", "hsnw_commit_log_condensing").
					Error("hnsw commit log maintenance failed")
			}
		}
	}()
}

func (l *hnswCommitLogger) maintenance() error {
	i, err := l.logFile.Stat()
	if err != nil {
		return err
	}

	if i.Size() > maxUncondensedCommitLogSize {
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
			os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
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

func (l *hnswCommitLogger) writeUint32(w io.Writer, in uint32) error {
	err := binary.Write(w, binary.LittleEndian, &in)
	if err != nil {
		return fmt.Errorf("writing uint32: %v", err)
	}

	return nil
}

func (l *hnswCommitLogger) writeUint16(w io.Writer, in uint16) error {
	err := binary.Write(w, binary.LittleEndian, &in)
	if err != nil {
		return fmt.Errorf("writing uint16: %v", err)
	}

	return nil
}

func (l *hnswCommitLogger) writeCommitType(w io.Writer, in HnswCommitType) error {
	err := binary.Write(w, binary.LittleEndian, &in)
	if err != nil {
		return fmt.Errorf("writing commit type: %v", err)
	}

	return nil
}

func (l *hnswCommitLogger) writeUint32Slice(w io.Writer, in []uint32) error {
	err := binary.Write(w, binary.LittleEndian, &in)
	if err != nil {
		return fmt.Errorf("writing []uint32: %v", err)
	}

	return nil
}
