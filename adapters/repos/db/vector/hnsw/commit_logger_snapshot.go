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
	"bufio"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/multivector"
	"github.com/weaviate/weaviate/entities/diskio"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

const (
	checkpointChunkSize   = 100_000
	snapshotConcurrency   = 8 // number of goroutines handling snapshot's checkpoints reading
	snapshotDirSuffix     = ".hnsw.snapshot.d"
	snapshotCheckInterval = 10 * time.Minute
)

const (
	SnapshotCompressionTypePQ = iota + 1
	SnapshotCompressionTypeSQ
	SnapshotEncoderTypeMuvera
)

func snapshotName(path string) string {
	base := filepath.Base(path)
	return strings.TrimSuffix(strings.TrimSuffix(base, ".snapshot"), ".snapshot.checkpoints")
}

func snapshotTimestamp(path string) (int64, error) {
	return asTimeStamp(snapshotName(path))
}

func snapshotDirectory(rootPath, name string) string {
	return filepath.Join(rootPath, name+snapshotDirSuffix)
}

// Loads state of last available snapshot. Returns nil if no snaphshot was found.
func (l *hnswCommitLogger) LoadSnapshot() (state *DeserializationResult, createdAt int64, err error) {
	logger, onFinish := l.setupSnapshotLogger(logrus.Fields{"method": "load_snapshot"})
	defer func() { onFinish(err) }()

	snapshotPath, createdAt, err := l.getLastSnapshot()
	if err != nil {
		return nil, 0, errors.Wrapf(err, "get last snapshot")
	}
	if snapshotPath == "" {
		logger.Debug("no last snapshot found")
		return nil, 0, nil
	}
	logger.WithField("snapshot", snapshotPath).Debug("last snapshot found")

	state, err = l.readSnapshot(snapshotPath)
	if err != nil {
		return nil, 0, l.handleReadSnapshotError(logger, snapshotPath, createdAt, err)
	}
	return state, createdAt, nil
}

// Creates a snapshot of the commit log. Returns if snapshot was actually created.
// The snapshot is created from the last snapshot and commitlog files created after,
// or from the entire commit log if there is no previous snapshot.
// The snapshot state contains all but last commitlog (may still be in use and mutable).
func (l *hnswCommitLogger) CreateSnapshot() (created bool, createdAt int64, err error) {
	logger, onFinish := l.setupSnapshotLogger(logrus.Fields{"method": "create_snapshot"})
	defer func() { onFinish(err) }()

	state, createdAt, err := l.createAndOptionallyLoadSnapshot(logger, false)
	return state != nil, createdAt, err
}

// CreateAndLoadSnapshot works like CreateSnapshot, but it will always load the
// last snapshot. It is used at startup to automatically create a snapshot
// while loading the commit log, to avoid having to load the commit log again.
func (l *hnswCommitLogger) CreateAndLoadSnapshot() (state *DeserializationResult, createdAt int64, err error) {
	logger, onFinish := l.setupSnapshotLogger(logrus.Fields{"method": "create_and_load_snapshot"})
	defer func() { onFinish(err) }()

	return l.createAndOptionallyLoadSnapshot(logger, true)
}

func (l *hnswCommitLogger) setupSnapshotLogger(fields logrus.Fields) (logger logrus.FieldLogger, onFinish func(err error)) {
	logger = l.snapshotLogger.WithFields(fields)
	started := time.Now()

	logger.Debug("started")
	return logger, func(err error) {
		l := logger.WithField("took", time.Since(started))
		if err != nil {
			l.WithError(err).Errorf("finished with err")
		} else {
			l.Debug("finished")
		}
	}
}

func (l *hnswCommitLogger) createAndOptionallyLoadSnapshot(logger logrus.FieldLogger, load bool,
) (*DeserializationResult, int64, error) {
	lastSnapshotPath, lastCreatedAt, err := l.getLastSnapshot()
	if err != nil {
		return nil, 0, errors.Wrapf(err, "get last snapshot")
	}

	state, path, createdAt, err := l.createAndOptionallyLoadSnapshotOnLastOne(logger, load, lastSnapshotPath, lastCreatedAt)
	if path != "" {
		l.snapshotLastCreatedAt = time.Now()
		l.snapshotPartitions = []string{snapshotName(path)}
	}
	return state, createdAt, err
}

func (l *hnswCommitLogger) createAndOptionallyLoadSnapshotOnLastOne(logger logrus.FieldLogger,
	load bool, snapshotPath string, createdAt int64,
) (*DeserializationResult, string, int64, error) {
	commitlogPaths, err := l.getDeltaCommitlogs(createdAt)
	if err != nil {
		return nil, "", 0, errors.Wrapf(err, "get delta commitlogs")
	}

	// skip allocCheck on forced loading
	shouldCreateSnapshot := l.shouldCreateSnapshot(logger, snapshotPath, commitlogPaths, load)

	var state *DeserializationResult
	if load || shouldCreateSnapshot {
		if snapshotPath != "" {
			logger.WithField("snapshot", snapshotPath).Debug("last snapshot found")

			state, err = l.readSnapshot(snapshotPath)
			if err != nil {
				if err = l.handleReadSnapshotError(logger, snapshotPath, createdAt, err); err != nil {
					return nil, "", 0, errors.Wrapf(err, "read snapshot")
				}
				// call again without last snapshot
				return l.createAndOptionallyLoadSnapshotOnLastOne(logger, load, "", 0)
			}
		} else {
			logger.Debug("no last snapshot found")
		}
	}

	if !shouldCreateSnapshot {
		return state, "", createdAt, nil
	}

	newState, err := loadCommitLoggerState(l.logger, commitlogPaths, state, nil)
	if err != nil {
		return nil, "", 0, errors.Wrapf(err, "apply delta commitlogs")
	}
	if newState == nil {
		return nil, "", 0, errors.New("empty state")
	}

	ln := len(commitlogPaths)
	newSnapshotPath := l.snapshotFileName(commitlogPaths[ln-1])
	newCreatedAt, err := snapshotTimestamp(newSnapshotPath)
	if err != nil {
		return nil, "", 0, errors.Wrapf(err, "get new snapshot timestamp")
	}
	if err := l.writeSnapshot(newState, newSnapshotPath); err != nil {
		return nil, "", 0, errors.Wrapf(err, "write new snapshot")
	}
	logger.WithFields(logrus.Fields{
		"delta_commitlogs": ln,
		"last_snapshot":    snapshotPath,
		"snapshot":         newSnapshotPath,
	}).Info("new snapshot created")

	if err = l.cleanupSnapshots(newCreatedAt); err != nil {
		return newState, newSnapshotPath, newCreatedAt, errors.Wrapf(err, "cleanup previous snapshot")
	}

	return newState, newSnapshotPath, newCreatedAt, nil
}

func (l *hnswCommitLogger) shouldCreateSnapshot(logger logrus.FieldLogger,
	lastSnapshotPath string, deltaCommitlogPaths []string, skipAllocCheck bool,
) bool {
	if ln := len(deltaCommitlogPaths); ln < l.snapshotMinDeltaCommitlogsNumber {
		logger.Debugf("not enough delta commitlogs (%d of required %d)", ln, l.snapshotMinDeltaCommitlogsNumber)
		return false
	}

	// calculate sizes only if needed
	snapshotSize := int64(0)
	commitlogsSize := int64(0)
	if (!skipAllocCheck && l.allocChecker != nil) ||
		(l.snapshotMinDeltaCommitlogsSizePercentage > 0 && lastSnapshotPath != "") {
		snapshotSize = l.calcSnapshotSize(lastSnapshotPath)
		commitlogsSize = l.calcCommitlogsSize(deltaCommitlogPaths...)
	}

	if l.snapshotMinDeltaCommitlogsSizePercentage > 0 && snapshotSize > 0 {
		percentage := float32(commitlogsSize) * 100 / float32(snapshotSize)
		if percentage < float32(l.snapshotMinDeltaCommitlogsSizePercentage) {
			logger.Debugf("too small delta commitlogs size (%.2f%% of required %d%% of snapshot size)", percentage, l.snapshotMinDeltaCommitlogsSizePercentage)
			return false
		}
	}

	if !skipAllocCheck && l.allocChecker != nil {
		requiredSize := snapshotSize + commitlogsSize
		if err := l.allocChecker.CheckAlloc(requiredSize); err != nil {
			logger.WithField("size", requiredSize).
				WithError(err).
				Warnf("skipping hnsw snapshot due to memory pressure")
			return false
		}
	}
	return true
}

func (l *hnswCommitLogger) initSnapshotData() error {
	dirs := strings.Split(filepath.Clean(l.rootPath), string(os.PathSeparator))
	if ln := len(dirs); ln > 2 {
		dirs = dirs[ln-2:]
	}
	snapshotLogger := l.logger.WithFields(logrus.Fields{
		"action": "hnsw_commit_log_snapshot",
		"id":     l.id,
		"path":   filepath.Join(dirs...),
	})
	fields := logrus.Fields{"enabled": !l.snapshotDisabled}

	defer func() {
		snapshotLogger.WithFields(fields).Debug("snapshot config")
	}()

	snapshotPath, createdAt, err := l.getLastSnapshot()
	if err != nil {
		return errors.Wrapf(err, "get last snapshot")
	}
	l.snapshotPartitions = []string{}
	if snapshotPath != "" {
		l.snapshotPartitions = append(l.snapshotPartitions, snapshotName(snapshotPath))
	}

	fields["last_snapshot"] = snapshotPath
	fields["partitions"] = l.snapshotPartitions

	if !l.snapshotDisabled {
		if err := os.MkdirAll(snapshotDirectory(l.rootPath, l.id), 0o755); err != nil {
			return errors.Wrapf(err, "make snapshot directory")
		}

		l.snapshotLogger = snapshotLogger
		if l.snapshotCreateInterval > 0 {
			l.snapshotLastCreatedAt = time.Unix(createdAt, 0)
			l.snapshotLastCheckedAt = time.Now()
			l.snapshotCheckInterval = min(snapshotCheckInterval, l.snapshotCreateInterval)

			fields["last_created_at"] = l.snapshotLastCreatedAt
			fields["last_checked_at"] = l.snapshotLastCheckedAt
			fields["check_interval"] = l.snapshotCheckInterval
		}

		fields["create_interval"] = l.snapshotCreateInterval
	}
	return nil
}

func (l *hnswCommitLogger) handleReadSnapshotError(logger logrus.FieldLogger,
	snapshotPath string, createdAt int64, err error,
) error {
	logger.WithField("snapshot", snapshotPath).
		WithError(err).
		Warn("snapshot can not be read, cleanup")

	if err := l.cleanupSnapshots(createdAt + 1); err != nil {
		logger.WithField("snapshot", snapshotPath).
			WithError(err).
			Warn("cleaning snapshots")
	}

	// suppress error
	return nil
}

// if file size can not be read, it is skipped
func (l *hnswCommitLogger) calcSnapshotSize(snapshotPath string) int64 {
	if snapshotPath == "" {
		return 0
	}

	totalSize := int64(0)
	if info, err := os.Stat(snapshotPath); err == nil {
		totalSize += info.Size()
	}
	if info, err := os.Stat(snapshotPath + ".checkpoints"); err == nil {
		totalSize += info.Size()
	}
	return totalSize
}

// if file size can not be read, it is skipped
func (l *hnswCommitLogger) calcCommitlogsSize(commitLogPaths ...string) int64 {
	if len(commitLogPaths) == 0 {
		return 0
	}

	totalSize := int64(0)
	for i := range commitLogPaths {
		if info, err := os.Stat(commitLogPaths[i]); err == nil {
			totalSize += info.Size()
		}
	}
	return totalSize
}

func (l *hnswCommitLogger) snapshotFileName(commitLogFileName string) string {
	path := strings.TrimSuffix(commitLogFileName, ".condensed") + ".snapshot"
	return strings.Replace(path, ".hnsw.commitlog.d", snapshotDirSuffix, 1)
}

// read the directory and find the latest snapshot file
func (l *hnswCommitLogger) getLastSnapshot() (path string, createdAt int64, err error) {
	snapshotDir := snapshotDirectory(l.rootPath, l.id)

	entries, err := os.ReadDir(snapshotDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// no snapshot directory, no snapshot
			return "", 0, nil
		}
		return "", 0, errors.Wrapf(err, "read snapshot directory %q", snapshotDir)
	}

	for i := len(entries) - 1; i >= 0; i-- {
		entry := entries[i]

		if entry.IsDir() {
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".snapshot") {
			// not a snapshot file
			continue
		}

		createdAt, err = snapshotTimestamp(entry.Name())
		if err != nil {
			return "", 0, errors.Wrapf(err, "get snapshot timestamp")
		}
		return filepath.Join(snapshotDir, entry.Name()), createdAt, nil
	}

	// no snapshot found
	return "", 0, nil
}

func (l *hnswCommitLogger) getDeltaCommitlogs(createdAfter int64) (paths []string, err error) {
	files, err := getCommitFiles(l.rootPath, l.id, createdAfter)
	if err != nil {
		return nil, err
	}
	// skip last file, may still be in use and mutable
	if ln := len(files); ln > 1 {
		files = files[:ln-1]
	} else {
		return []string{}, nil
	}
	files, err = skipEmptyFiles(files)
	if err != nil {
		return nil, err
	}
	return commitLogFileNames(l.rootPath, l.id, files), nil
}

// cleanupSnapshots removes all snapshots, checkpoints and temporary files older than the given timestamp.
func (l *hnswCommitLogger) cleanupSnapshots(before int64) error {
	snapshotDir := snapshotDirectory(l.rootPath, l.id)

	files, err := os.ReadDir(snapshotDir)
	if err != nil {
		return errors.Wrapf(err, "read snapshot directory %q", snapshotDir)
	}
	for _, file := range files {
		name := file.Name()

		if strings.HasSuffix(name, ".snapshot.tmp") {
			// a temporary snapshot file was found which means that a previous
			// snapshoting process never completed, we can safely remove it.
			err := os.Remove(filepath.Join(snapshotDir, name))
			if err != nil {
				return errors.Wrapf(err, "remove tmp snapshot file %q", name)
			}
		}

		if strings.HasSuffix(name, ".snapshot") {
			tmstr := strings.TrimSuffix(name, ".snapshot")
			i, err := strconv.ParseInt(tmstr, 10, 64)
			if err != nil {
				return errors.Wrapf(err, "parse snapshot time")
			}

			if i < before {
				err := os.Remove(filepath.Join(snapshotDir, name))
				if err != nil {
					return errors.Wrapf(err, "remove snapshot file %q", name)
				}
			}
		}

		if strings.HasSuffix(name, ".snapshot.checkpoints") {
			tmstr := strings.TrimSuffix(name, ".snapshot.checkpoints")
			i, err := strconv.ParseInt(tmstr, 10, 64)
			if err != nil {
				return errors.Wrapf(err, "parse checkpoints time")
			}

			if i < before {
				err := os.Remove(filepath.Join(snapshotDir, name))
				if err != nil {
					return errors.Wrapf(err, "remove checkpoints file %q", name)
				}
			}
		}
	}

	return nil
}

func loadCommitLoggerState(logger logrus.FieldLogger, fileNames []string, state *DeserializationResult, metrics *Metrics) (*DeserializationResult, error) {
	var err error

	fileNames, err = NewCorruptedCommitLogFixer().Do(fileNames)
	if err != nil {
		return nil, errors.Wrap(err, "corrupted commit log fixer")
	}

	for i, fileName := range fileNames {
		beforeIndividual := time.Now()

		err = func() error {
			fd, err := os.Open(fileName)
			if err != nil {
				return errors.Wrapf(err, "open commit log %q for reading", fileName)
			}
			defer fd.Close()

			info, err := fd.Stat()
			if err != nil {
				errors.Wrapf(err, "get commit log %qsize", fileName)
			}
			if info.Size() == 0 {
				// nothing to do
				return nil
			}

			var fdMetered io.Reader = fd
			if metrics != nil {
				fdMetered = diskio.NewMeteredReader(fd,
					metrics.TrackStartupReadCommitlogDiskIO)
			}
			fdBuf := bufio.NewReaderSize(fdMetered, 256*1024)

			var valid int
			state, valid, err = NewDeserializer(logger).Do(fdBuf, state, false)
			if err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
					// we need to check for both EOF or UnexpectedEOF, as we don't know where
					// the commit log got corrupted, a field ending that weset a longer
					// encoding for would return EOF, whereas a field read with binary.Read
					// with a fixed size would return UnexpectedEOF. From our perspective both
					// are unexpected.

					logger.WithField("action", "hnsw_load_commit_log_corruption").
						WithField("path", fileName).
						Error("write-ahead-log ended abruptly, some elements may not have been recovered")

					// we need to truncate the file to its valid length!
					if err := os.Truncate(fileName, int64(valid)); err != nil {
						return errors.Wrapf(err, "truncate corrupt commit log %q", fileName)
					}
				} else {
					// only return an actual error on non-EOF errors, otherwise we'll end
					// up in a startup crashloop
					return errors.Wrapf(err, "deserialize commit log %q", fileName)
				}
			}
			return nil
		}()
		if err != nil {
			return nil, err
		}

		if metrics != nil {
			metrics.StartupProgress(float64(i+1) / float64(len(fileNames)))
			metrics.TrackStartupIndividual(beforeIndividual)
		}
	}

	return state, nil
}

func (l *hnswCommitLogger) writeSnapshot(state *DeserializationResult, filename string) error {
	tmpSnapshotFileName := fmt.Sprintf("%s.tmp", filename)
	checkPointsFileName := fmt.Sprintf("%s.checkpoints", filename)

	snap, err := os.OpenFile(tmpSnapshotFileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o666)
	if err != nil {
		return errors.Wrapf(err, "create snapshot file %q", tmpSnapshotFileName)
	}
	defer snap.Close()

	// compute the checksum of the snapshot file
	w := bufio.NewWriter(snap)

	// write the snapshot to the file
	checkpoints, err := l.writeStateTo(state, w)
	if err != nil {
		return errors.Wrapf(err, "writing snapshot file %q", tmpSnapshotFileName)
	}

	// flush the buffered writer
	err = w.Flush()
	if err != nil {
		return errors.Wrapf(err, "flushing snapshot file %q", tmpSnapshotFileName)
	}

	// sync the file to disk
	err = snap.Sync()
	if err != nil {
		return errors.Wrapf(err, "fsync snapshot file %q", tmpSnapshotFileName)
	}

	// close the file
	err = snap.Close()
	if err != nil {
		return errors.Wrapf(err, "close snapshot file %q", tmpSnapshotFileName)
	}

	// write the checkpoints to a separate file
	err = writeCheckpoints(checkPointsFileName, checkpoints)
	if err != nil {
		return errors.Wrap(err, "write checkpoints file")
	}

	// rename the temporary snapshot file to the final name
	err = os.Rename(tmpSnapshotFileName, filename)
	if err != nil {
		return errors.Wrapf(err, "rename snapshot file %q", tmpSnapshotFileName)
	}

	return nil
}

func (l *hnswCommitLogger) readSnapshot(path string) (*DeserializationResult, error) {
	checkpoints, err := readCheckpoints(path)
	if err != nil {
		// if for any reason the checkpoints file is not found or corrupted
		// we need to remove the snapshot file and create a new one from the commit log.
		_ = os.Remove(path)
		cpPath := path + ".checkpoints"
		_ = os.Remove(cpPath)

		l.logger.WithField("action", "hnsw_remove_corrupt_snapshot").
			WithField("path", path).
			WithError(err).
			Error("checkpoints file not found or corrupted, removing snapshot files")

		return nil, errors.Wrapf(err, "read checkpoints of snapshot '%s'", path)
	}

	state, err := l.readStateFrom(path, checkpoints)
	if err != nil {
		// if for any reason the snapshot file is not found or corrupted
		// we need to remove the snapshot file and create a new one from the commit log.
		_ = os.Remove(path)
		cpPath := path + ".checkpoints"
		_ = os.Remove(cpPath)

		l.logger.WithField("action", "hnsw_remove_corrupt_snapshot").
			WithField("path", path).
			WithError(err).
			Error("snapshot file not found or corrupted, removing snapshot files")
		return nil, errors.Wrapf(err, "read state of snapshot '%s'", path)
	}

	return state, nil
}

// returns checkpoints which can be used as parallelizatio hints
func (l *hnswCommitLogger) writeStateTo(state *DeserializationResult, wr io.Writer) ([]Checkpoint, error) {
	offset, err := l.writeMetadataTo(state, wr)
	if err != nil {
		return nil, err
	}

	var checkpoints []Checkpoint
	// start at the very first node
	checkpoints = append(checkpoints, Checkpoint{NodeID: 0, Offset: uint64(offset)})

	nonNilNodes := 0

	hasher := crc32.NewIEEE()
	w := io.MultiWriter(wr, hasher)

	for i, n := range state.Nodes {
		if n == nil {
			// nil node
			if err := writeByte(w, 0); err != nil {
				return nil, err
			}
			offset += writeByteSize
			continue
		}

		_, hasATombstone := state.Tombstones[n.id]
		_, tombstoneIsCleaned := state.TombstonesDeleted[n.id]

		if hasATombstone && tombstoneIsCleaned {
			// if the node has been deleted but its tombstone has been cleaned up
			// we can write a nil node
			if err := writeByte(w, 0); err != nil {
				return nil, err
			}
			offset += writeByteSize
			continue
		}

		if nonNilNodes%checkpointChunkSize == 0 && nonNilNodes > 0 {
			checkpoints[len(checkpoints)-1].Hash = hasher.Sum32()
			hasher.Reset()
			checkpoints = append(checkpoints, Checkpoint{NodeID: uint64(i), Offset: uint64(offset)})
		}

		if hasATombstone {
			if err := writeByte(w, 1); err != nil {
				return nil, err
			}
		} else {
			if err := writeByte(w, 2); err != nil {
				return nil, err
			}
		}
		offset += writeByteSize

		if err := writeUint32(w, uint32(n.level)); err != nil {
			return nil, err
		}
		offset += writeUint32Size

		if err := writeUint32(w, uint32(len(n.connections))); err != nil {
			return nil, err
		}
		offset += writeUint32Size

		for _, ls := range n.connections {
			if err := writeUint32(w, uint32(len(ls))); err != nil {
				return nil, err
			}
			offset += writeUint32Size

			for _, c := range ls {
				if err := writeUint64(w, c); err != nil {
					return nil, err
				}
				offset += writeUint64Size
			}
		}

		nonNilNodes++
	}

	// compute last checkpoint hash
	checkpoints[len(checkpoints)-1].Hash = hasher.Sum32()

	// add a dummy checkpoint to mark the end of the file
	checkpoints = append(checkpoints, Checkpoint{NodeID: math.MaxInt64, Offset: uint64(offset)})

	return checkpoints, nil
}

// returns checkpoints which can be used as parallelizatio hints
func (l *hnswCommitLogger) writeMetadataTo(state *DeserializationResult, w io.Writer) (offset int, err error) {
	hasher := crc32.NewIEEE()
	w = io.MultiWriter(w, hasher)

	// version
	offset = 0
	if err := writeByte(w, 0); err != nil {
		return 0, err
	}
	offset += writeByteSize

	if err := writeUint64(w, state.Entrypoint); err != nil {
		return 0, err
	}
	offset += writeUint64Size

	if err := writeUint16(w, state.Level); err != nil {
		return 0, err
	}
	offset += writeUint16Size

	isEncoded := state.Compressed || state.MuveraEnabled

	if err := writeBool(w, isEncoded); err != nil {
		return 0, err
	}
	offset += writeByteSize

	if state.Compressed && state.CompressionPQData != nil { // PQ
		// first byte is the compression type
		if err := writeByte(w, byte(SnapshotCompressionTypePQ)); err != nil {
			return 0, err
		}
		offset += writeByteSize

		if err := writeUint16(w, state.CompressionPQData.Dimensions); err != nil {
			return 0, err
		}
		offset += writeUint16Size

		if err := writeUint16(w, state.CompressionPQData.Ks); err != nil {
			return 0, err
		}
		offset += writeUint16Size

		if err := writeUint16(w, state.CompressionPQData.M); err != nil {
			return 0, err
		}
		offset += writeUint16Size

		if err := writeByte(w, byte(state.CompressionPQData.EncoderType)); err != nil {
			return 0, err
		}
		offset += writeByteSize

		if err := writeByte(w, state.CompressionPQData.EncoderDistribution); err != nil {
			return 0, err
		}
		offset += writeByteSize

		if err := writeBool(w, state.CompressionPQData.UseBitsEncoding); err != nil {
			return 0, err
		}
		offset += writeByteSize

		for _, encoder := range state.CompressionPQData.Encoders {
			if n, err := w.Write(encoder.ExposeDataForRestore()); err != nil {
				return 0, err
			} else {
				offset += n
			}
		}
	} else if state.Compressed && state.CompressionSQData != nil { // SQ
		// first byte is the compression type
		if err := writeByte(w, byte(SnapshotCompressionTypeSQ)); err != nil {
			return 0, err
		}
		offset += writeByteSize

		if err := writeUint16(w, state.CompressionSQData.Dimensions); err != nil {
			return 0, err
		}
		offset += writeUint16Size

		if err := writeUint32(w, math.Float32bits(state.CompressionSQData.A)); err != nil {
			return 0, err
		}
		offset += writeUint32Size

		if err := writeUint32(w, math.Float32bits(state.CompressionSQData.B)); err != nil {
			return 0, err
		}
		offset += writeUint32Size
	} else if state.MuveraEnabled && state.EncoderMuvera != nil { // Muvera
		// first byte is the encoder type
		if err := writeByte(w, byte(SnapshotEncoderTypeMuvera)); err != nil {
			return 0, err
		}
		offset += writeByteSize

		if err := writeUint32(w, state.EncoderMuvera.Dimensions); err != nil {
			return 0, err
		}
		offset += writeUint32Size

		if err := writeUint32(w, state.EncoderMuvera.KSim); err != nil {
			return 0, err
		}
		offset += writeUint32Size

		if err := writeUint32(w, state.EncoderMuvera.NumClusters); err != nil {
			return 0, err
		}
		offset += writeUint32Size

		if err := writeUint32(w, state.EncoderMuvera.DProjections); err != nil {
			return 0, err
		}
		offset += writeUint32Size

		if err := writeUint32(w, state.EncoderMuvera.Repetitions); err != nil {
			return 0, err
		}
		offset += writeUint32Size

		for _, gaussian := range state.EncoderMuvera.Gaussians {
			for _, cluster := range gaussian {
				for _, el := range cluster {
					if err := writeUint32(w, math.Float32bits(el)); err != nil {
						return 0, err
					}
					offset += writeUint32Size
				}
			}
		}

		for _, matrix := range state.EncoderMuvera.S {
			for _, vector := range matrix {
				for _, el := range vector {
					if err := writeUint32(w, math.Float32bits(el)); err != nil {
						return 0, err
					}
					offset += writeUint32Size
				}
			}
		}
	}

	if err := writeUint32(w, uint32(len(state.Nodes))); err != nil {
		return 0, err
	}
	offset += writeUint32Size

	// write checksum of the metadata
	if err := binary.Write(w, binary.LittleEndian, hasher.Sum32()); err != nil {
		return 0, err
	}
	offset += writeUint32Size

	return offset, nil
}

func (l *hnswCommitLogger) readStateFrom(filename string, checkpoints []Checkpoint) (*DeserializationResult, error) {
	res := &DeserializationResult{
		NodesDeleted:      make(map[uint64]struct{}),
		Tombstones:        make(map[uint64]struct{}),
		TombstonesDeleted: make(map[uint64]struct{}),
		LinksReplaced:     make(map[uint64]map[uint16]struct{}),
	}

	f, err := os.Open(filename)
	if err != nil {
		return nil, errors.Wrapf(err, "open snapshot file %q", filename)
	}
	defer f.Close()

	hasher := crc32.NewIEEE()
	// start with a single-threaded reader until we make it the nodes section
	r := bufio.NewReader(f)

	var b [8]byte

	_, err = ReadAndHash(r, hasher, b[:1]) // version
	if err != nil {
		return nil, errors.Wrapf(err, "read version")
	}
	if b[0] != 0 {
		return nil, fmt.Errorf("unsupported version %d", b[0])
	}

	_, err = ReadAndHash(r, hasher, b[:8]) // entrypoint
	if err != nil {
		return nil, errors.Wrapf(err, "read entrypoint")
	}
	res.Entrypoint = binary.LittleEndian.Uint64(b[:8])

	_, err = ReadAndHash(r, hasher, b[:2]) // level
	if err != nil {
		return nil, errors.Wrapf(err, "read level")
	}
	res.Level = binary.LittleEndian.Uint16(b[:2])

	_, err = ReadAndHash(r, hasher, b[:1]) // isEncoded
	if err != nil {
		return nil, errors.Wrapf(err, "read compressed")
	}
	isEncoded := b[0] == 1

	// Compressed data
	if isEncoded {
		_, err = ReadAndHash(r, hasher, b[:1]) // encoding type
		if err != nil {
			return nil, errors.Wrapf(err, "read compressed")
		}

		switch b[0] {
		case SnapshotCompressionTypePQ:
			res.Compressed = true
			_, err = ReadAndHash(r, hasher, b[:2]) // PQData.Dimensions
			if err != nil {
				return nil, errors.Wrapf(err, "read PQData.Dimensions")
			}
			dims := binary.LittleEndian.Uint16(b[:2])

			_, err = ReadAndHash(r, hasher, b[:2]) // PQData.Ks
			if err != nil {
				return nil, errors.Wrapf(err, "read PQData.Ks")
			}
			ks := binary.LittleEndian.Uint16(b[:2])

			_, err = ReadAndHash(r, hasher, b[:2]) // PQData.M
			if err != nil {
				return nil, errors.Wrapf(err, "read PQData.M")
			}
			m := binary.LittleEndian.Uint16(b[:2])

			_, err = ReadAndHash(r, hasher, b[:1]) // PQData.EncoderType
			if err != nil {
				return nil, errors.Wrapf(err, "read PQData.EncoderType")
			}
			encoderType := compressionhelpers.Encoder(b[0])

			_, err = ReadAndHash(r, hasher, b[:1]) // PQData.EncoderDistribution
			if err != nil {
				return nil, errors.Wrapf(err, "read PQData.EncoderDistribution")
			}
			dist := b[0]

			_, err = ReadAndHash(r, hasher, b[:1]) // PQData.UseBitsEncoding
			if err != nil {
				return nil, errors.Wrapf(err, "read PQData.UseBitsEncoding")
			}
			useBitsEncoding := b[0] == 1

			encoder := compressionhelpers.Encoder(encoderType)

			res.CompressionPQData = &compressionhelpers.PQData{
				Dimensions:          dims,
				EncoderType:         encoder,
				Ks:                  ks,
				M:                   m,
				EncoderDistribution: dist,
				UseBitsEncoding:     useBitsEncoding,
			}

			var encoderReader func(r io.Reader, res *compressionhelpers.PQData, i uint16) (compressionhelpers.PQEncoder, error)

			switch encoder {
			case compressionhelpers.UseTileEncoder:
				encoderReader = ReadTileEncoder
			case compressionhelpers.UseKMeansEncoder:
				encoderReader = ReadKMeansEncoder
			default:
				return nil, errors.New("unsuported encoder type")
			}

			for i := uint16(0); i < m; i++ {
				encoder, err := encoderReader(io.TeeReader(r, hasher), res.CompressionPQData, i)
				if err != nil {
					return nil, err
				}
				res.CompressionPQData.Encoders = append(res.CompressionPQData.Encoders, encoder)
			}
		case SnapshotCompressionTypeSQ:
			res.Compressed = true
			_, err = ReadAndHash(r, hasher, b[:2]) // SQData.Dimensions
			if err != nil {
				return nil, errors.Wrapf(err, "read SQData.Dimensions")
			}
			dims := binary.LittleEndian.Uint16(b[:2])

			_, err = ReadAndHash(r, hasher, b[:4]) // SQData.A
			if err != nil {
				return nil, errors.Wrapf(err, "read SQData.A")
			}
			a := math.Float32frombits(binary.LittleEndian.Uint32(b[:4]))

			_, err = ReadAndHash(r, hasher, b[:4]) // SQData.B
			if err != nil {
				return nil, errors.Wrapf(err, "read SQData.B")
			}
			b := math.Float32frombits(binary.LittleEndian.Uint32(b[:4]))

			res.CompressionSQData = &compressionhelpers.SQData{
				Dimensions: dims,
				A:          a,
				B:          b,
			}
		case SnapshotEncoderTypeMuvera:
			_, err = ReadAndHash(r, hasher, b[:4]) // Muvera.Dimensions
			if err != nil {
				return nil, errors.Wrapf(err, "read Muvera.Dimensions")
			}
			dims := binary.LittleEndian.Uint32(b[:4])

			_, err = ReadAndHash(r, hasher, b[:4]) // Muvera.KSim
			if err != nil {
				return nil, errors.Wrapf(err, "read Muvera.KSim")
			}
			kSim := binary.LittleEndian.Uint32(b[:4])

			_, err = ReadAndHash(r, hasher, b[:4]) // Muvera.NumClusters
			if err != nil {
				return nil, errors.Wrapf(err, "read Muvera.NumClusters")
			}
			numClusters := binary.LittleEndian.Uint32(b[:4])

			_, err = ReadAndHash(r, hasher, b[:4]) // Muvera.DProjections
			if err != nil {
				return nil, errors.Wrapf(err, "read Muvera.DProjections")
			}
			dProjections := binary.LittleEndian.Uint32(b[:4])

			_, err = ReadAndHash(r, hasher, b[:4]) // Muvera.Repetitions
			if err != nil {
				return nil, errors.Wrapf(err, "read Muvera.Repetitions")
			}
			repetitions := binary.LittleEndian.Uint32(b[:4])

			gaussians := make([][][]float32, repetitions)
			for i := uint32(0); i < repetitions; i++ {
				gaussians[i] = make([][]float32, kSim)
				for j := uint32(0); j < kSim; j++ {
					gaussians[i][j] = make([]float32, dims)
					for k := uint32(0); k < dims; k++ {
						_, err = ReadAndHash(r, hasher, b[:4])
						if err != nil {
							return nil, errors.Wrapf(err, "read Muvera.Gaussians")
						}
						bits := binary.LittleEndian.Uint32(b[:4])
						gaussians[i][j][k] = math.Float32frombits(bits)
					}
				}
			}

			s := make([][][]float32, repetitions)
			for i := uint32(0); i < repetitions; i++ {
				s[i] = make([][]float32, dProjections)
				for j := uint32(0); j < dProjections; j++ {
					s[i][j] = make([]float32, dims)
					for k := uint32(0); k < dims; k++ {
						_, err = ReadAndHash(r, hasher, b[:4])
						if err != nil {
							return nil, errors.Wrapf(err, "read Muvera.Gaussians")
						}
						bits := binary.LittleEndian.Uint32(b[:4])
						s[i][j][k] = math.Float32frombits(bits)
					}
				}
			}

			res.MuveraEnabled = true
			res.EncoderMuvera = &multivector.MuveraData{
				Dimensions:   dims,
				NumClusters:  numClusters,
				KSim:         kSim,
				DProjections: dProjections,
				Repetitions:  repetitions,
				Gaussians:    gaussians,
				S:            s,
			}
		default:
			return nil, fmt.Errorf("unsupported compression type %d", b[0])
		}
	}

	_, err = ReadAndHash(r, hasher, b[:4]) // nodes
	if err != nil {
		return nil, errors.Wrapf(err, "read nodes count")
	}
	nodesCount := int(binary.LittleEndian.Uint32(b[:4]))

	res.Nodes = make([]*vertex, nodesCount)

	// read metadata checksum
	_, err = io.ReadFull(r, b[:4]) // checksum
	if err != nil {
		return nil, errors.Wrapf(err, "read checksum")
	}

	// check checksum
	checksum := binary.LittleEndian.Uint32(b[:4])
	actualChecksum := hasher.Sum32()
	if checksum != actualChecksum {
		return nil, fmt.Errorf("invalid checksum: expected %d, got %d", checksum, actualChecksum)
	}

	var mu sync.Mutex

	eg := enterrors.NewErrorGroupWrapper(l.logger)
	eg.SetLimit(snapshotConcurrency)
	for cpPos, cp := range checkpoints {
		if cpPos == len(checkpoints)-1 {
			// last checkpoint, no need to read
			break
		}

		start := int(cp.Offset)
		end := int(checkpoints[cpPos+1].Offset)

		eg.Go(func() error {
			var b [8]byte
			var read int

			currNodeID := cp.NodeID
			sr := io.NewSectionReader(f, int64(start), int64(end-start))
			hasher := crc32.NewIEEE()
			r := bufio.NewReader(io.TeeReader(sr, hasher))

			for read < end-start {
				n, err := io.ReadFull(r, b[:1]) // node existence
				if err != nil {
					return errors.Wrapf(err, "read node existence")
				}
				read += n
				if b[0] == 0 {
					// nil node
					currNodeID++
					continue
				}

				node := &vertex{id: currNodeID}

				if b[0] == 1 {
					mu.Lock()
					res.Tombstones[node.id] = struct{}{}
					mu.Unlock()
				} else if b[0] != 2 {
					return fmt.Errorf("unsupported node existence state")
				}

				n, err = io.ReadFull(r, b[:4]) // level
				if err != nil {
					return errors.Wrapf(err, "read node level")
				}
				read += n
				node.level = int(binary.LittleEndian.Uint32(b[:4]))

				n, err = io.ReadFull(r, b[:4]) // connections count
				if err != nil {
					return errors.Wrapf(err, "read node connections count")
				}
				read += n
				connCount := int(binary.LittleEndian.Uint32(b[:4]))

				if connCount > 0 {
					node.connections = make([][]uint64, connCount)

					for l := 0; l < connCount; l++ {
						n, err = io.ReadFull(r, b[:4]) // connections count at level
						if err != nil {
							return errors.Wrapf(err, "read node connections count at level")
						}
						read += n
						connCountAtLevel := int(binary.LittleEndian.Uint32(b[:4]))

						if connCountAtLevel > 0 {
							node.connections[l] = make([]uint64, connCountAtLevel)

							for c := 0; c < connCountAtLevel; c++ {
								n, err = io.ReadFull(r, b[:8]) // connection at level
								if err != nil {
									return errors.Wrapf(err, "read node connection at level")
								}
								node.connections[l][c] = binary.LittleEndian.Uint64(b[:8])
								read += n
							}
						}

					}
				}

				mu.Lock()
				res.Nodes[currNodeID] = node
				mu.Unlock()
				currNodeID++
			}

			// check checksum of checkpoint
			if cp.Hash != hasher.Sum32() {
				return fmt.Errorf("invalid checksum for checkpoint %d: expected %d, got %d", cpPos, cp.Hash, hasher.Sum32())
			}

			return nil
		})
	}

	err = eg.Wait()
	if err != nil {
		return nil, err
	}

	return res, nil
}

func ReadAndHash(r io.Reader, hasher hash.Hash, buf []byte) (int, error) {
	n, err := io.ReadFull(r, buf)
	if err != nil {
		return n, err
	}
	_, err = hasher.Write(buf)
	if err != nil {
		return n, err
	}
	return n, nil
}

type Checkpoint struct {
	NodeID uint64
	Offset uint64
	Hash   uint32
}

func writeCheckpoints(fileName string, checkpoints []Checkpoint) error {
	checkpointFile, err := os.OpenFile(fileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o666)
	if err != nil {
		return fmt.Errorf("open new checkpoint file for writing: %w", err)
	}
	defer checkpointFile.Close()

	// 0-4: checksum
	// 4+: checkpoints (20 bytes each)
	buffer := make([]byte, 4+len(checkpoints)*20)
	offset := 4

	for _, cp := range checkpoints {
		binary.LittleEndian.PutUint64(buffer[offset:offset+8], cp.NodeID)
		offset += 8
		binary.LittleEndian.PutUint64(buffer[offset:offset+8], cp.Offset)
		offset += 8
		binary.LittleEndian.PutUint32(buffer[offset:offset+4], cp.Hash)
		offset += 4
	}

	checksum := crc32.ChecksumIEEE(buffer[4:])
	binary.LittleEndian.PutUint32(buffer[:4], checksum)

	_, err = checkpointFile.Write(buffer)
	if err != nil {
		return fmt.Errorf("write checkpoint file: %w", err)
	}

	return checkpointFile.Sync()
}

func readCheckpoints(snapshotFileName string) (checkpoints []Checkpoint, err error) {
	cpfn := snapshotFileName + ".checkpoints"

	cpFile, err := os.Open(cpfn)
	if err != nil {
		return nil, err
	}
	defer cpFile.Close()

	buf, err := io.ReadAll(cpFile)
	if err != nil {
		return nil, err
	}
	if len(buf) < 4 {
		return nil, fmt.Errorf("corrupted checkpoint file %q", cpfn)
	}

	checksum := binary.LittleEndian.Uint32(buf[:4])
	actualChecksum := crc32.ChecksumIEEE(buf[4:])
	if checksum != actualChecksum {
		return nil, fmt.Errorf("corrupted checkpoint file %q, checksum mismatch", cpfn)
	}

	checkpoints = make([]Checkpoint, 0, len(buf[4:])/20)
	for i := 4; i < len(buf); i += 20 {
		id := binary.LittleEndian.Uint64(buf[i : i+8])
		offset := binary.LittleEndian.Uint64(buf[i+8 : i+16])
		hash := binary.LittleEndian.Uint32(buf[i+16 : i+20])
		checkpoints = append(checkpoints, Checkpoint{NodeID: id, Offset: offset, Hash: hash})
	}

	return checkpoints, nil
}
