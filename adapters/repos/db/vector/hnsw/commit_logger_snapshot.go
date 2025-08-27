//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"bufio"
	"bytes"
	"context"
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
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/packedconn"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/multivector"
	"github.com/weaviate/weaviate/entities/diskio"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

const (
	snapshotConcurrency   = 8 // number of goroutines handling snapshot's chunk reading
	snapshotDirSuffix     = ".hnsw.snapshot.d"
	snapshotCheckInterval = 10 * time.Minute
	blockSize             = 4 * 1024 * 1024 // 4MB
)

const (
	SnapshotCompressionTypePQ = iota + 1
	SnapshotCompressionTypeSQ
	SnapshotEncoderTypeMuvera
	SnapshotCompressionTypeRQ
)

// version of the snapshot file format
const (
	// Initial version
	snapshotVersionV1 = 1 //nolint:unused
	// Added packed connections support
	snapshotVersionV2 = 2
	// New snapshot format: organize data in fixed-sized chunks.
	// Metadata header now starts with version|checksum|metadatasize|metadatadata.
	// Body is organized in fixed-sized chunks (4MB).
	// Each chunk has the following format: checksum|startnodeid|data|padding|length.
	// The checkpoints file is removed in this version.
	snapshotVersionV3 = 3
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
	start := time.Now()
	defer func() {
		logger.WithField("commitlog_files", len(fileNames)).
			WithField("took", time.Since(start)).
			Debug("commit log files loaded")
	}()
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
			fdBuf := bufio.NewReaderSize(fdMetered, 512*1024)

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

	snap, err := os.OpenFile(tmpSnapshotFileName, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0o666)
	if err != nil {
		return errors.Wrapf(err, "create snapshot file %q", tmpSnapshotFileName)
	}
	defer snap.Close()

	w := bufio.NewWriter(snap)

	// write the snapshot to the file
	err = l.writeStateTo(state, w)
	if err != nil {
		return errors.Wrapf(err, "writing snapshot file %q", tmpSnapshotFileName)
	}

	// flush the buffer
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

	// rename the temporary snapshot file to the final name
	err = os.Rename(tmpSnapshotFileName, filename)
	if err != nil {
		return errors.Wrapf(err, "rename snapshot file %q", tmpSnapshotFileName)
	}

	return nil
}

func (l *hnswCommitLogger) readSnapshot(path string) (*DeserializationResult, error) {
	start := time.Now()
	defer func() {
		l.logger.WithField("snapshot", path).WithField("took", time.Since(start).String()).Info("snapshot loaded")
	}()

	state, err := l.readStateFrom(path)
	if err != nil {
		// if for any reason the snapshot file is not found or corrupted
		// we need to remove the snapshot file and create a new one from the commit log.
		_ = os.Remove(path)
		cpPath := path + ".checkpoints"
		_ = os.Remove(cpPath)

		l.logger.WithField("action", "hnsw_remove_corrupt_snapshot").
			WithField("path", path).
			WithError(err).
			Error("error while reading snapshot, removing snapshot files")
		return nil, errors.Wrapf(err, "read state of snapshot '%s'", path)
	}

	return state, nil
}

func (l *hnswCommitLogger) writeStateTo(state *DeserializationResult, wr io.Writer) error {
	err := l.writeMetadataTo(state, wr)
	if err != nil {
		return err
	}

	var block bytes.Buffer // fixed-sized block buffer
	var buf bytes.Buffer   // reusable per-node buffer

	hasher := crc32.NewIEEE()
	hw := io.MultiWriter(&block, hasher)

	maxBlockSize := blockSize - 8 // reserve 8 bytes for checksum and actual block length

	// write id of the first node at the start of each block,
	// here 0 for the 1st block
	if err := writeUint64(hw, 0); err != nil {
		return err
	}

	for i, n := range state.Nodes {
		buf.Reset()

		if n != nil {
			_, hasATombstone := state.Tombstones[n.id]
			_, tombstoneIsCleaned := state.TombstonesDeleted[n.id]

			if hasATombstone && tombstoneIsCleaned {
				// if the node has been deleted but its tombstone has been cleaned up
				// we can write a nil node
				if err := writeByte(&buf, 0); err != nil {
					return err
				}
				continue
			}

			if hasATombstone {
				_ = writeByte(&buf, 1)
			} else {
				_ = writeByte(&buf, 2)
			}

			_ = writeUint32(&buf, uint32(n.level))

			connData := n.connections.Data()
			_ = writeUint32(&buf, uint32(len(connData)))

			_, err = buf.Write(connData)
			if err != nil {
				return errors.Wrapf(err, "write connections data for node %d", n.id)
			}
		} else {
			// nil node
			if err := writeByte(&buf, 0); err != nil {
				return err
			}
		}

		// add node data to block if there's enough space, otherwise create a new block
		if buf.Len()+block.Len() < maxBlockSize {
			_, err := hw.Write(buf.Bytes())
			if err != nil {
				return err
			}
			continue
		}

		blockLen := block.Len()

		// new node doesn't fit in block, pad the block and create a new one
		_, err := hw.Write(make([]byte, maxBlockSize-blockLen)) // pad with zeros
		if err != nil {
			return err
		}

		// write block length at the end of the block
		if err := writeUint32(hw, uint32(blockLen)); err != nil {
			return err
		}

		// write block checksum to file
		checksum := hasher.Sum32()
		if err := writeUint32(wr, checksum); err != nil {
			return err
		}

		// write block to file
		_, err = wr.Write(block.Bytes())
		if err != nil {
			return err
		}

		// reset block
		block.Reset()
		hasher.Reset()
		hw = io.MultiWriter(&block, hasher)

		// write next node index at the start of the new block
		if i+1 < len(state.Nodes) {
			if err := writeUint64(hw, uint64(i+1)); err != nil {
				return err
			}
		}
	}

	// handle last block
	if block.Len() > 0 {
		blockLen := block.Len()
		// pad block
		_, err := hw.Write(make([]byte, maxBlockSize-blockLen)) // pad with zeros
		if err != nil {
			return err
		}

		// write block length at the end of the block
		if err := writeUint32(hw, uint32(blockLen)); err != nil {
			return err
		}

		// write block checksum to file
		checksum := hasher.Sum32()
		if err := writeUint32(wr, checksum); err != nil {
			return err
		}

		// write block to file
		_, err = wr.Write(block.Bytes())
		if err != nil {
			return err
		}
	}

	return nil
}

func (l *hnswCommitLogger) writeMetadataTo(state *DeserializationResult, w io.Writer) error {
	var buf bytes.Buffer

	_ = writeUint64(&buf, state.Entrypoint) // entrypoint
	_ = writeUint16(&buf, state.Level)      // level

	isCompressed := state.Compressed
	_ = writeBool(&buf, isCompressed) // isCompressed

	if state.Compressed && state.CompressionPQData != nil { // PQ
		// first byte is the compression type
		_ = writeByte(&buf, byte(SnapshotCompressionTypePQ))
		_ = writeUint16(&buf, state.CompressionPQData.Dimensions)
		_ = writeUint16(&buf, state.CompressionPQData.Ks)
		_ = writeUint16(&buf, state.CompressionPQData.M)
		_ = writeByte(&buf, byte(state.CompressionPQData.EncoderType))
		_ = writeByte(&buf, state.CompressionPQData.EncoderDistribution)
		_ = writeBool(&buf, state.CompressionPQData.UseBitsEncoding)
		for _, encoder := range state.CompressionPQData.Encoders {
			_, _ = buf.Write(encoder.ExposeDataForRestore())
		}
	} else if state.Compressed && state.CompressionSQData != nil { // SQ
		// first byte is the compression type
		_ = writeByte(&buf, byte(SnapshotCompressionTypeSQ))
		_ = writeUint16(&buf, state.CompressionSQData.Dimensions)
		_ = writeUint32(&buf, math.Float32bits(state.CompressionSQData.A))
		_ = writeUint32(&buf, math.Float32bits(state.CompressionSQData.B))
	} else if state.Compressed && state.CompressionRQData != nil { // RQ
		// first byte is the compression type
		_ = writeByte(&buf, byte(SnapshotCompressionTypeRQ))
		_ = writeUint32(&buf, state.CompressionRQData.InputDim)
		_ = writeUint32(&buf, state.CompressionRQData.Bits)
		_ = writeUint32(&buf, state.CompressionRQData.Rotation.OutputDim)
		_ = writeUint32(&buf, state.CompressionRQData.Rotation.Rounds)
		for _, swap := range state.CompressionRQData.Rotation.Swaps {
			for _, dim := range swap {
				_ = writeUint16(&buf, dim.I)
				_ = writeUint16(&buf, dim.J)
			}
		}

		for _, sign := range state.CompressionRQData.Rotation.Signs {
			for _, dim := range sign {
				_ = writeFloat32(&buf, dim)
			}
		}
	}

	isEncoded := state.MuveraEnabled
	_ = writeBool(&buf, isEncoded) // isEncoded

	if state.MuveraEnabled && state.EncoderMuvera != nil { // Muvera
		// first byte is the encoder type
		_ = writeByte(&buf, byte(SnapshotEncoderTypeMuvera))
		_ = writeUint32(&buf, state.EncoderMuvera.Dimensions)
		_ = writeUint32(&buf, state.EncoderMuvera.KSim)
		_ = writeUint32(&buf, state.EncoderMuvera.NumClusters)
		_ = writeUint32(&buf, state.EncoderMuvera.DProjections)
		_ = writeUint32(&buf, state.EncoderMuvera.Repetitions)
		for _, gaussian := range state.EncoderMuvera.Gaussians {
			for _, cluster := range gaussian {
				for _, el := range cluster {
					_ = writeUint32(&buf, math.Float32bits(el))
				}
			}
		}

		for _, matrix := range state.EncoderMuvera.S {
			for _, vector := range matrix {
				for _, el := range vector {
					_ = writeUint32(&buf, math.Float32bits(el))
				}
			}
		}
	}

	_ = writeUint32(&buf, uint32(len(state.Nodes)))

	// compute checksum of the metadata
	metadataSize := uint32(buf.Len())

	hasher := crc32.NewIEEE()
	_ = binary.Write(hasher, binary.LittleEndian, uint8(snapshotVersionV3))
	_ = binary.Write(hasher, binary.LittleEndian, metadataSize)
	_, _ = hasher.Write(buf.Bytes())

	// write everything to the file
	// version
	if err := writeByte(w, snapshotVersionV3); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, hasher.Sum32()); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, metadataSize); err != nil {
		return err
	}

	if _, err := w.Write(buf.Bytes()); err != nil {
		return err
	}

	return nil
}

func (l *hnswCommitLogger) readStateFrom(filename string) (*DeserializationResult, error) {
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

	version, err := l.readMetadata(f, res)
	if err != nil {
		return nil, err
	}

	if version < snapshotVersionV3 {
		err = l.legacyReadSnapshotBody(filename, f, version, res)
	} else {
		err = l.readSnapshotBody(f, res)
	}
	if err != nil {
		return nil, err
	}

	return res, nil
}

// legacyReadSnapshotBody reads the snapshot body from the file for snapshot versions < 3.
func (l *hnswCommitLogger) legacyReadSnapshotBody(filename string, f *os.File, version int, res *DeserializationResult) error {
	checkpoints, err := readCheckpoints(filename)
	if err != nil {
		// if for any reason the checkpoints file is not found or corrupted
		// we need to remove the snapshot file and create a new one from the commit log.
		_ = os.Remove(filename)
		cpPath := filename + ".checkpoints"
		_ = os.Remove(cpPath)

		l.logger.WithField("action", "hnsw_remove_corrupt_snapshot").
			WithField("path", filename).
			WithError(err).
			Error("checkpoints file not found or corrupted, removing snapshot files")

		return errors.Wrapf(err, "read checkpoints of snapshot '%s'", filename)
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
					if version < snapshotVersionV2 {
						pconn, err := packedconn.NewWithMaxLayer(uint8(connCount))
						if err != nil {
							return errors.Wrapf(err, "create packed connections for node %d", node.id)
						}

						for l := uint8(0); l < uint8(connCount); l++ {
							n, err = io.ReadFull(r, b[:4]) // connections count at level
							if err != nil {
								return errors.Wrapf(err, "read node connections count at level")
							}
							read += n
							connCountAtLevel := uint64(binary.LittleEndian.Uint32(b[:4]))

							if connCountAtLevel > 0 {
								for c := uint64(0); c < connCountAtLevel; c++ {
									n, err = io.ReadFull(r, b[:8]) // connection at level
									if err != nil {
										return errors.Wrapf(err, "read node connection at level")
									}
									connID := binary.LittleEndian.Uint64(b[:8])
									pconn.InsertAtLayer(connID, l)
									read += n
								}
							}
						}

						node.connections = pconn
					} else {
						// read the connections data
						connData := make([]byte, connCount)
						n, err = io.ReadFull(r, connData)
						if err != nil {
							return errors.Wrapf(err, "read node connections data")
						}
						read += n

						node.connections = packedconn.NewWithData(connData)
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
		return err
	}

	return nil
}

// readSnapshotBody reads the snapshot body from the file for snapshot versions >= 3.
func (l *hnswCommitLogger) readSnapshotBody(f *os.File, res *DeserializationResult) error {
	var mu sync.Mutex

	finfo, err := f.Stat()
	if err != nil {
		return err
	}
	fsize := finfo.Size()
	seek, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	bodySize := int(fsize - seek)

	eg, ctx := enterrors.NewErrorGroupWithContextWrapper(l.logger, context.Background())
	eg.SetLimit(snapshotConcurrency)

	ch := make(chan int, snapshotConcurrency)

	for i := 0; i < snapshotConcurrency; i++ {
		eg.Go(func() error {
			buf := make([]byte, blockSize)
			var b [8]byte

			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case offset, ok := <-ch:
					if !ok {
						return nil // channel closed, nothing to do
					}

					sr := io.NewSectionReader(f, seek+int64(offset), int64(blockSize))
					n, err := io.ReadFull(sr, buf)
					if err != nil {
						return err
					}
					if n != blockSize {
						return fmt.Errorf("read %d bytes, expected %d bytes at offset %d", n, blockSize, seek+int64(offset))
					}

					hasher := crc32.NewIEEE()
					_, _ = hasher.Write(buf[4:]) // skip the checksum itself
					actualChecksum := hasher.Sum32()

					blockChecksum := binary.LittleEndian.Uint32(buf[:4])
					if actualChecksum != blockChecksum {
						return fmt.Errorf("checksum mismatch for block at offset %d: expected %d, got %d", seek+int64(offset), blockChecksum, actualChecksum)
					}

					block := buf[4:]
					blockLen := binary.LittleEndian.Uint32(block[len(block)-4:])
					block = block[:blockLen]
					r := bytes.NewReader(block)

					_, err = io.ReadFull(r, b[:8]) // start node index
					if err != nil {
						return errors.Wrap(err, "read start node index")
					}
					currNodeID := binary.LittleEndian.Uint64(b[:8])

					for {
						_, err := io.ReadFull(r, b[:1]) // node existence
						if err != nil {
							if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
								break
							}
							return errors.Wrapf(err, "read node existence")
						}
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

						_, err = io.ReadFull(r, b[:4]) // level
						if err != nil {
							return errors.Wrapf(err, "read node level")
						}
						node.level = int(binary.LittleEndian.Uint32(b[:4]))

						_, err = io.ReadFull(r, b[:4]) // connections count
						if err != nil {
							return errors.Wrapf(err, "read node connections count")
						}
						connCount := int(binary.LittleEndian.Uint32(b[:4]))

						if connCount > 0 {
							// read the connections data
							connData := make([]byte, connCount)
							_, err = io.ReadFull(r, connData)
							if err != nil {
								return errors.Wrapf(err, "read node connections data")
							}

							node.connections = packedconn.NewWithData(connData)
						}

						mu.Lock()
						res.Nodes[currNodeID] = node
						mu.Unlock()
						currNodeID++
					}
				}
			}
		})
	}

	for i := 0; i < bodySize; i += blockSize {
		ch <- i
	}
	close(ch)

	return eg.Wait()
}

func (l *hnswCommitLogger) readMetadata(f *os.File, res *DeserializationResult) (int, error) {
	var b [1]byte
	_, err := f.Read(b[:1])
	if err != nil {
		return 0, errors.Wrapf(err, "read version")
	}
	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		return 0, errors.Wrapf(err, "seek to start")
	}

	version := int(b[0])
	if version < 0 || version > snapshotVersionV3 {
		return 0, fmt.Errorf("unsupported snapshot version %d", version)
	}

	if version < snapshotVersionV3 {
		return version, l.readMetadataLegacy(f, res)
	}

	// version >= 3
	return version, l.readAndCheckMetadata(f, res)
}

// this reads the legacy snapshot format for versions v < 3
func (l *hnswCommitLogger) readMetadataLegacy(f *os.File, res *DeserializationResult) error {
	var b [8]byte

	hasher := crc32.NewIEEE()
	r := bufio.NewReader(f)

	_, err := ReadAndHash(r, hasher, b[:1]) // version
	if err != nil {
		return errors.Wrapf(err, "read version")
	}
	version := int(b[0])

	_, err = ReadAndHash(r, hasher, b[:8]) // entrypoint
	if err != nil {
		return errors.Wrapf(err, "read entrypoint")
	}
	res.Entrypoint = binary.LittleEndian.Uint64(b[:8])

	_, err = ReadAndHash(r, hasher, b[:2]) // level
	if err != nil {
		return errors.Wrapf(err, "read level")
	}
	res.Level = binary.LittleEndian.Uint16(b[:2])

	_, err = ReadAndHash(r, hasher, b[:1]) // isEncoded
	if err != nil {
		return errors.Wrapf(err, "read compressed")
	}
	isCompressed := b[0] == 1

	// Compressed data
	if isCompressed {
		_, err = ReadAndHash(r, hasher, b[:1]) // encoding type
		if err != nil {
			return errors.Wrapf(err, "read compressed")
		}

		switch b[0] {
		case SnapshotEncoderTypeMuvera: // legacy Muvera snapshot
			return errors.New("discarding v1 Muvera snapshot")
		case SnapshotCompressionTypePQ:
			res.Compressed = true
			_, err = ReadAndHash(r, hasher, b[:2]) // PQData.Dimensions
			if err != nil {
				return errors.Wrapf(err, "read PQData.Dimensions")
			}
			dims := binary.LittleEndian.Uint16(b[:2])

			_, err = ReadAndHash(r, hasher, b[:2]) // PQData.Ks
			if err != nil {
				return errors.Wrapf(err, "read PQData.Ks")
			}
			ks := binary.LittleEndian.Uint16(b[:2])

			_, err = ReadAndHash(r, hasher, b[:2]) // PQData.M
			if err != nil {
				return errors.Wrapf(err, "read PQData.M")
			}
			m := binary.LittleEndian.Uint16(b[:2])

			_, err = ReadAndHash(r, hasher, b[:1]) // PQData.EncoderType
			if err != nil {
				return errors.Wrapf(err, "read PQData.EncoderType")
			}
			encoderType := compressionhelpers.Encoder(b[0])

			_, err = ReadAndHash(r, hasher, b[:1]) // PQData.EncoderDistribution
			if err != nil {
				return errors.Wrapf(err, "read PQData.EncoderDistribution")
			}
			dist := b[0]

			_, err = ReadAndHash(r, hasher, b[:1]) // PQData.UseBitsEncoding
			if err != nil {
				return errors.Wrapf(err, "read PQData.UseBitsEncoding")
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
				return errors.New("unsuported encoder type")
			}

			for i := uint16(0); i < m; i++ {
				encoder, err := encoderReader(io.TeeReader(r, hasher), res.CompressionPQData, i)
				if err != nil {
					return err
				}
				res.CompressionPQData.Encoders = append(res.CompressionPQData.Encoders, encoder)
			}
		case SnapshotCompressionTypeSQ:
			res.Compressed = true
			_, err = ReadAndHash(r, hasher, b[:2]) // SQData.Dimensions
			if err != nil {
				return errors.Wrapf(err, "read SQData.Dimensions")
			}
			dims := binary.LittleEndian.Uint16(b[:2])

			_, err = ReadAndHash(r, hasher, b[:4]) // SQData.A
			if err != nil {
				return errors.Wrapf(err, "read SQData.A")
			}
			a := math.Float32frombits(binary.LittleEndian.Uint32(b[:4]))

			_, err = ReadAndHash(r, hasher, b[:4]) // SQData.B
			if err != nil {
				return errors.Wrapf(err, "read SQData.B")
			}
			b := math.Float32frombits(binary.LittleEndian.Uint32(b[:4]))

			res.CompressionSQData = &compressionhelpers.SQData{
				Dimensions: dims,
				A:          a,
				B:          b,
			}
		case SnapshotCompressionTypeRQ:
			res.Compressed = true
			_, err = ReadAndHash(r, hasher, b[:4]) // RQData.InputDim
			if err != nil {
				return errors.Wrapf(err, "read RQData.Dimension")
			}
			inputDim := binary.LittleEndian.Uint32(b[:4])

			_, err = ReadAndHash(r, hasher, b[:4]) // RQData.Bits
			if err != nil {
				return errors.Wrapf(err, "read RQData.Bits")
			}
			bits := binary.LittleEndian.Uint32(b[:4])

			_, err = ReadAndHash(r, hasher, b[:4]) // RQData.Rotation.OutputDim
			if err != nil {
				return errors.Wrapf(err, "read RQData.Rotation.OutputDim")
			}
			outputDim := binary.LittleEndian.Uint32(b[:4])

			_, err = ReadAndHash(r, hasher, b[:4]) // RQData.Rotation.Rounds
			if err != nil {
				return errors.Wrapf(err, "read RQData.Rotation.Rounds")
			}
			rounds := binary.LittleEndian.Uint32(b[:4])

			swaps := make([][]compressionhelpers.Swap, rounds)
			for i := uint32(0); i < rounds; i++ {
				swaps[i] = make([]compressionhelpers.Swap, outputDim/2)
				for j := uint32(0); j < outputDim/2; j++ {
					_, err = ReadAndHash(r, hasher, b[:2]) // RQData.Rotation.Swaps[i][j].I
					if err != nil {
						return errors.Wrapf(err, "read RQData.Rotation.Swaps[i][j].I")
					}
					swaps[i][j].I = binary.LittleEndian.Uint16(b[:2])

					_, err = ReadAndHash(r, hasher, b[:2]) // RQData.Rotation.Swaps[i][j].J
					if err != nil {
						return errors.Wrapf(err, "read RQData.Rotation.Swaps[i][j].J")
					}
					swaps[i][j].J = binary.LittleEndian.Uint16(b[:2])
				}
			}

			signs := make([][]float32, rounds)

			for i := uint32(0); i < rounds; i++ {
				signs[i] = make([]float32, outputDim)
				for j := uint32(0); j < outputDim; j++ {
					_, err = ReadAndHash(r, hasher, b[:4]) // RQData.Rotation.Signs[i][j]
					if err != nil {
						return errors.Wrapf(err, "read RQData.Rotation.Signs[i][j]")
					}
					signs[i][j] = math.Float32frombits(binary.LittleEndian.Uint32(b[:4]))
				}
			}

			res.CompressionRQData = &compressionhelpers.RQData{
				InputDim: inputDim,
				Bits:     bits,
				Rotation: compressionhelpers.FastRotation{
					OutputDim: outputDim,
					Rounds:    rounds,
					Swaps:     swaps,
					Signs:     signs,
				},
			}
		default:
			return fmt.Errorf("unsupported compression type %d", b[0])
		}
	}

	isEncoded := false
	if version >= snapshotVersionV2 {
		_, err = ReadAndHash(r, hasher, b[:1]) // isEncoded
		if err != nil {
			return errors.Wrapf(err, "read isEncoded")
		}
		isEncoded = b[0] == 1
	}

	if isEncoded {
		_, err = ReadAndHash(r, hasher, b[:1]) // encoding type
		if err != nil {
			return errors.Wrapf(err, "read encoding type")
		}
		switch b[0] {
		case SnapshotEncoderTypeMuvera:
			_, err = ReadAndHash(r, hasher, b[:4]) // Muvera.Dimensions
			if err != nil {
				return errors.Wrapf(err, "read Muvera.Dimensions")
			}
			dims := binary.LittleEndian.Uint32(b[:4])

			_, err = ReadAndHash(r, hasher, b[:4]) // Muvera.KSim
			if err != nil {
				return errors.Wrapf(err, "read Muvera.KSim")
			}
			kSim := binary.LittleEndian.Uint32(b[:4])

			_, err = ReadAndHash(r, hasher, b[:4]) // Muvera.NumClusters
			if err != nil {
				return errors.Wrapf(err, "read Muvera.NumClusters")
			}
			numClusters := binary.LittleEndian.Uint32(b[:4])

			_, err = ReadAndHash(r, hasher, b[:4]) // Muvera.DProjections
			if err != nil {
				return errors.Wrapf(err, "read Muvera.DProjections")
			}
			dProjections := binary.LittleEndian.Uint32(b[:4])

			_, err = ReadAndHash(r, hasher, b[:4]) // Muvera.Repetitions
			if err != nil {
				return errors.Wrapf(err, "read Muvera.Repetitions")
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
							return errors.Wrapf(err, "read Muvera.Gaussians")
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
							return errors.Wrapf(err, "read Muvera.Gaussians")
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
			return fmt.Errorf("unsupported encoder type %d", b[0])
		}
	}

	_, err = ReadAndHash(r, hasher, b[:4]) // nodes
	if err != nil {
		return errors.Wrapf(err, "read nodes count")
	}
	nodesCount := int(binary.LittleEndian.Uint32(b[:4]))

	res.Nodes = make([]*vertex, nodesCount)

	// read metadata checksum
	_, err = io.ReadFull(r, b[:4]) // checksum
	if err != nil {
		return errors.Wrapf(err, "read checksum")
	}

	// check checksum
	checksum := binary.LittleEndian.Uint32(b[:4])
	actualChecksum := hasher.Sum32()
	if checksum != actualChecksum {
		return fmt.Errorf("invalid checksum: expected %d, got %d", checksum, actualChecksum)
	}

	return nil
}

// this reads the newer snapshot format for versions v >= 3
func (l *hnswCommitLogger) readAndCheckMetadata(f *os.File, res *DeserializationResult) error {
	var b [8]byte
	var read int

	hasher := crc32.NewIEEE()
	r := bufio.NewReader(f)

	n, err := ReadAndHash(r, hasher, b[:1]) // version
	if err != nil {
		return errors.Wrapf(err, "read version")
	}
	read += n

	// read checksum
	n, err = io.ReadFull(r, b[:4])
	if err != nil {
		return errors.Wrapf(err, "read metadata checksum")
	}
	read += n
	expected := binary.LittleEndian.Uint32(b[:4])

	// read metadata size
	n, err = ReadAndHash(r, hasher, b[:4]) // size
	if err != nil {
		return errors.Wrapf(err, "read metadata size")
	}
	read += n
	metadataSize := int(binary.LittleEndian.Uint32(b[:4]))

	// read full metadata
	metadata := make([]byte, metadataSize)
	n, err = ReadAndHash(r, hasher, metadata) // metadata
	if err != nil {
		return errors.Wrapf(err, "read metadata")
	}
	read += n

	actual := hasher.Sum32()
	if actual != expected {
		return fmt.Errorf("invalid metadata checksum: expected %d, got %d", expected, actual)
	}

	mr := bytes.NewReader(metadata)

	_, err = io.ReadFull(mr, b[:8]) // entrypoint
	if err != nil {
		return errors.Wrapf(err, "read entrypoint")
	}
	res.Entrypoint = binary.LittleEndian.Uint64(b[:8])

	_, err = io.ReadFull(mr, b[:2]) // level
	if err != nil {
		return errors.Wrapf(err, "read level")
	}
	res.Level = binary.LittleEndian.Uint16(b[:2])

	_, err = io.ReadFull(mr, b[:1]) // isEncoded
	if err != nil {
		return errors.Wrapf(err, "read compressed")
	}
	isCompressed := b[0] == 1

	// Compressed data
	if isCompressed {
		_, err = io.ReadFull(mr, b[:1]) // encoding type
		if err != nil {
			return errors.Wrapf(err, "read compressed")
		}

		switch b[0] {
		case SnapshotEncoderTypeMuvera: // legacy Muvera snapshot
			return errors.New("discarding v1 Muvera snapshot")
		case SnapshotCompressionTypePQ:
			res.Compressed = true
			_, err = io.ReadFull(mr, b[:2]) // PQData.Dimensions
			if err != nil {
				return errors.Wrapf(err, "read PQData.Dimensions")
			}
			dims := binary.LittleEndian.Uint16(b[:2])

			_, err = io.ReadFull(mr, b[:2]) // PQData.Ks
			if err != nil {
				return errors.Wrapf(err, "read PQData.Ks")
			}
			ks := binary.LittleEndian.Uint16(b[:2])

			_, err = io.ReadFull(mr, b[:2]) // PQData.M
			if err != nil {
				return errors.Wrapf(err, "read PQData.M")
			}
			m := binary.LittleEndian.Uint16(b[:2])

			_, err = io.ReadFull(mr, b[:1]) // PQData.EncoderType
			if err != nil {
				return errors.Wrapf(err, "read PQData.EncoderType")
			}
			encoderType := compressionhelpers.Encoder(b[0])

			_, err = io.ReadFull(mr, b[:1]) // PQData.EncoderDistribution
			if err != nil {
				return errors.Wrapf(err, "read PQData.EncoderDistribution")
			}
			dist := b[0]

			_, err = io.ReadFull(mr, b[:1]) // PQData.UseBitsEncoding
			if err != nil {
				return errors.Wrapf(err, "read PQData.UseBitsEncoding")
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
				return errors.New("unsupported encoder type")
			}

			for i := uint16(0); i < m; i++ {
				encoder, err := encoderReader(mr, res.CompressionPQData, i)
				if err != nil {
					return err
				}
				res.CompressionPQData.Encoders = append(res.CompressionPQData.Encoders, encoder)
			}
		case SnapshotCompressionTypeSQ:
			res.Compressed = true
			_, err = io.ReadFull(mr, b[:2]) // SQData.Dimensions
			if err != nil {
				return errors.Wrapf(err, "read SQData.Dimensions")
			}
			dims := binary.LittleEndian.Uint16(b[:2])

			_, err = io.ReadFull(mr, b[:4]) // SQData.A
			if err != nil {
				return errors.Wrapf(err, "read SQData.A")
			}
			a := math.Float32frombits(binary.LittleEndian.Uint32(b[:4]))

			_, err = io.ReadFull(mr, b[:4]) // SQData.B
			if err != nil {
				return errors.Wrapf(err, "read SQData.B")
			}
			b := math.Float32frombits(binary.LittleEndian.Uint32(b[:4]))

			res.CompressionSQData = &compressionhelpers.SQData{
				Dimensions: dims,
				A:          a,
				B:          b,
			}
		case SnapshotCompressionTypeRQ:
			res.Compressed = true
			_, err = io.ReadFull(mr, b[:4]) // RQData.InputDim
			if err != nil {
				return errors.Wrapf(err, "read RQData.Dimension")
			}
			inputDim := binary.LittleEndian.Uint32(b[:4])

			_, err = io.ReadFull(mr, b[:4]) // RQData.Bits
			if err != nil {
				return errors.Wrapf(err, "read RQData.Bits")
			}
			bits := binary.LittleEndian.Uint32(b[:4])

			_, err = io.ReadFull(mr, b[:4]) // RQData.Rotation.OutputDim
			if err != nil {
				return errors.Wrapf(err, "read RQData.Rotation.OutputDim")
			}
			outputDim := binary.LittleEndian.Uint32(b[:4])

			_, err = io.ReadFull(mr, b[:4]) // RQData.Rotation.Rounds
			if err != nil {
				return errors.Wrapf(err, "read RQData.Rotation.Rounds")
			}
			rounds := binary.LittleEndian.Uint32(b[:4])

			swaps := make([][]compressionhelpers.Swap, rounds)
			for i := uint32(0); i < rounds; i++ {
				swaps[i] = make([]compressionhelpers.Swap, outputDim/2)
				for j := uint32(0); j < outputDim/2; j++ {
					_, err = io.ReadFull(mr, b[:2]) // RQData.Rotation.Swaps[i][j].I
					if err != nil {
						return errors.Wrapf(err, "read RQData.Rotation.Swaps[i][j].I")
					}
					swaps[i][j].I = binary.LittleEndian.Uint16(b[:2])

					_, err = io.ReadFull(mr, b[:2]) // RQData.Rotation.Swaps[i][j].J
					if err != nil {
						return errors.Wrapf(err, "read RQData.Rotation.Swaps[i][j].J")
					}
					swaps[i][j].J = binary.LittleEndian.Uint16(b[:2])
				}
			}

			signs := make([][]float32, rounds)

			for i := uint32(0); i < rounds; i++ {
				signs[i] = make([]float32, outputDim)
				for j := uint32(0); j < outputDim; j++ {
					_, err = io.ReadFull(mr, b[:4]) // RQData.Rotation.Signs[i][j]
					if err != nil {
						return errors.Wrapf(err, "read RQData.Rotation.Signs[i][j]")
					}
					signs[i][j] = math.Float32frombits(binary.LittleEndian.Uint32(b[:4]))
				}
			}

			res.CompressionRQData = &compressionhelpers.RQData{
				InputDim: inputDim,
				Bits:     bits,
				Rotation: compressionhelpers.FastRotation{
					OutputDim: outputDim,
					Rounds:    rounds,
					Swaps:     swaps,
					Signs:     signs,
				},
			}
		default:
			return fmt.Errorf("unsupported compression type %d", b[0])
		}
	}

	_, err = io.ReadFull(mr, b[:1]) // isEncoded
	if err != nil {
		return errors.Wrapf(err, "read isEncoded")
	}
	isEncoded := b[0] == 1

	if isEncoded {
		_, err = io.ReadFull(mr, b[:1]) // encoding type
		if err != nil {
			return errors.Wrapf(err, "read encoding type")
		}
		switch b[0] {
		case SnapshotEncoderTypeMuvera:
			_, err = io.ReadFull(mr, b[:4]) // Muvera.Dimensions
			if err != nil {
				return errors.Wrapf(err, "read Muvera.Dimensions")
			}
			dims := binary.LittleEndian.Uint32(b[:4])

			_, err = io.ReadFull(mr, b[:4]) // Muvera.KSim
			if err != nil {
				return errors.Wrapf(err, "read Muvera.KSim")
			}
			kSim := binary.LittleEndian.Uint32(b[:4])

			_, err = io.ReadFull(mr, b[:4]) // Muvera.NumClusters
			if err != nil {
				return errors.Wrapf(err, "read Muvera.NumClusters")
			}
			numClusters := binary.LittleEndian.Uint32(b[:4])

			_, err = io.ReadFull(mr, b[:4]) // Muvera.DProjections
			if err != nil {
				return errors.Wrapf(err, "read Muvera.DProjections")
			}
			dProjections := binary.LittleEndian.Uint32(b[:4])

			_, err = io.ReadFull(mr, b[:4]) // Muvera.Repetitions
			if err != nil {
				return errors.Wrapf(err, "read Muvera.Repetitions")
			}
			repetitions := binary.LittleEndian.Uint32(b[:4])

			gaussians := make([][][]float32, repetitions)
			for i := uint32(0); i < repetitions; i++ {
				gaussians[i] = make([][]float32, kSim)
				for j := uint32(0); j < kSim; j++ {
					gaussians[i][j] = make([]float32, dims)
					for k := uint32(0); k < dims; k++ {
						_, err = io.ReadFull(mr, b[:4])
						if err != nil {
							return errors.Wrapf(err, "read Muvera.Gaussians")
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
						_, err = io.ReadFull(mr, b[:4])
						if err != nil {
							return errors.Wrapf(err, "read Muvera.Gaussians")
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
			return fmt.Errorf("unsupported encoder type %d", b[0])
		}
	}

	_, err = io.ReadFull(mr, b[:4]) // nodes
	if err != nil {
		return errors.Wrapf(err, "read nodes count")
	}
	nodesCount := int(binary.LittleEndian.Uint32(b[:4]))

	res.Nodes = make([]*vertex, nodesCount)

	// bufio.Reader may have read ahead, so we need to reset the cursor to the start of the body
	_, err = f.Seek(int64(read), io.SeekStart)
	if err != nil {
		return errors.Wrapf(err, "seek to %d", read)
	}

	return nil
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
	checkpointFile, err := os.OpenFile(fileName, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0o666)
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
