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
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	entcfg "github.com/weaviate/weaviate/entities/config"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

const checkpointChunkSize = 100_000

func snapshotsDisabled() bool {
	return entcfg.Enabled(os.Getenv("DISABLE_SNAPSHOTS"))
}

func snapshotTimestamp(path string) (int64, error) {
	return asTimeStamp(strings.TrimSuffix(filepath.Base(path), ".snapshot"))
}

func (l *hnswCommitLogger) CreateSnapshot() (*DeserializationResult, int64, error) {
	return l.createOrLoadSnapshot(false)
}

func (l *hnswCommitLogger) CreateOrLoadSnapshot() (*DeserializationResult, int64, error) {
	return l.createOrLoadSnapshot(true)
}

func (l *hnswCommitLogger) createOrLoadSnapshot(load bool) (*DeserializationResult, int64, error) {
	snapshot, from, immutableFiles, err := l.shouldSnapshot()
	if err != nil {
		return nil, 0, err
	}
	if !load && len(immutableFiles) == 0 {
		// no snapshot needed
		return nil, from, nil
	}

	start := time.Now()

	// only log if we are creating a snapshot
	if len(immutableFiles) > 0 {
		l.logger.WithField("action", "hnsw_create_snapshot").
			Info("creating snapshot")
	}

	// load the last snapshot
	var state *DeserializationResult
	if snapshot != "" {
		state, _, err = readLastSnapshot(l.rootPath, l.id, l.logger)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "read snapshot file %q", snapshot)
		}
	}

	if len(immutableFiles) == 0 {
		// no commit log files to load
		return state, from, nil
	}

	// load the commit log state since the last snapshot
	state, err = loadCommitLoggerState(l.logger, immutableFiles, state)
	if err != nil {
		return nil, 0, err
	}

	// create a new snapshot file
	snapshotFileName := l.snapshotFileName(immutableFiles[len(immutableFiles)-1])
	err = l.writeSnapshot(state, snapshotFileName)
	if err != nil {
		return nil, 0, err
	}

	ts, err := snapshotTimestamp(snapshotFileName)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "get snapshot timestamp")
	}

	err = l.cleanupSnapshots(ts)
	if err != nil {
		l.logger.WithField("action", "hnsw_cleanup_snapshots").
			WithField("path", snapshotFileName).
			WithField("error", err).
			Warn("failed to cleanup snapshots")
	}

	l.logger.WithField("action", "hnsw_create_snapshot").
		WithField("path", snapshotFileName).
		WithField("duration", time.Since(start)).
		Info("snapshot created")

	return state, ts, nil
}

func (l *hnswCommitLogger) shouldSnapshot() (string, int64, []string, error) {
	// check time of last snapshot
	name, err := l.getLastSnapshotName()
	if err != nil {
		return "", 0, nil, errors.Wrapf(err, "get last snapshot name")
	}
	var from int64
	if name != "" {
		from, err = snapshotTimestamp(name)
		if err != nil {
			return "", 0, nil, errors.Wrapf(err, "get last snapshot time")
		}
	}

	// check if commit log contains at least 2 new commit files
	fileNames, err := getCommitFileNames(l.rootPath, l.id, from)
	if err != nil {
		return "", from, nil, err
	}

	if len(fileNames) < 2 {
		// not enough commit log files
		return "", from, nil, nil
	}

	// get a list of all immutable condensed files
	immutable, err := l.getImmutableCondensedFiles(fileNames)
	if err != nil {
		return "", from, nil, err
	}

	return name, from, immutable, nil
}

func (l *hnswCommitLogger) snapshotFileName(commitLogFileName string) string {
	return strings.Replace(commitLogFileName, ".condensed", ".snapshot", 1)
}

// read the directory and find the latest snapshot file
func (l *hnswCommitLogger) getLastSnapshotName() (string, error) {
	commitLogDir := commitLogDirectory(l.rootPath, l.id)

	files, err := os.ReadDir(commitLogDir)
	if err != nil {
		return "", errors.Wrapf(err, "read snapshot directory %q", commitLogDir)
	}

	for i := len(files) - 1; i >= 0; i-- {
		file := files[i]
		if file.IsDir() {
			continue
		}

		name := file.Name()
		if strings.HasSuffix(name, ".snapshot") {
			return filepath.Join(commitLogDir, name), nil
		}
	}

	// no snapshot found
	return "", nil
}

// cleanupSnapshots removes all snapshots, checkpoints and temporary files older than the given timestamp.
func (l *hnswCommitLogger) cleanupSnapshots(before int64) error {
	commitLogDir := commitLogDirectory(l.rootPath, l.id)

	files, err := os.ReadDir(commitLogDir)
	if err != nil {
		return errors.Wrapf(err, "read snapshot directory %q", commitLogDir)
	}
	for _, file := range files {
		name := file.Name()

		if strings.HasSuffix(name, ".snapshot.tmp") {
			// a temporary snapshot file was found which means that a previous
			// snapshoting process never completed, we can safely remove it.
			err := os.Remove(filepath.Join(commitLogDir, name))
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
				err := os.Remove(filepath.Join(commitLogDir, name))
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
				err := os.Remove(filepath.Join(commitLogDir, name))
				if err != nil {
					return errors.Wrapf(err, "remove checkpoints file %q", name)
				}
			}
		}
	}

	return nil
}

func loadCommitLoggerState(logger logrus.FieldLogger, fileNames []string, state *DeserializationResult) (*DeserializationResult, error) {
	var err error

	fileNames, err = NewCorruptedCommitLogFixer().Do(fileNames)
	if err != nil {
		return nil, errors.Wrap(err, "corrupted commit log fixer")
	}

	for _, fileName := range fileNames {
		fd, err := os.Open(fileName)
		if err != nil {
			return nil, errors.Wrapf(err, "open commit log %q for reading", fileName)
		}
		defer fd.Close()

		// metered := diskio.NewMeteredReader(fd,
		// 	l.metrics.TrackStartupReadCommitlogDiskIO)
		fdBuf := bufio.NewReaderSize(fd, 256*1024)

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
					return nil, errors.Wrapf(err, "truncate corrupt commit log %q", fileName)
				}
			} else {
				// only return an actual error on non-EOF errors, otherwise we'll end
				// up in a startup crashloop
				return nil, errors.Wrapf(err, "deserialize commit log %q", fileName)
			}
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

	w := bufio.NewWriter(snap)

	checkpoints, err := writeStateTo(state, w)
	if err != nil {
		return errors.Wrapf(err, "writing snapshot file %q", tmpSnapshotFileName)
	}

	err = w.Flush()
	if err != nil {
		return errors.Wrapf(err, "flushing snapshot file %q", tmpSnapshotFileName)
	}

	err = snap.Sync()
	if err != nil {
		return errors.Wrapf(err, "fsync snapshot file %q", tmpSnapshotFileName)
	}

	err = snap.Close()
	if err != nil {
		return errors.Wrapf(err, "close snapshot file %q", tmpSnapshotFileName)
	}

	err = writeCheckpoints(checkPointsFileName, checkpoints)
	if err != nil {
		return errors.Wrap(err, "write checkpoints file")
	}

	err = os.Rename(tmpSnapshotFileName, filename)
	if err != nil {
		return errors.Wrapf(err, "rename snapshot file %q", tmpSnapshotFileName)
	}

	return nil
}

func (l *hnswCommitLogger) getImmutableCondensedFiles(fileNames []string) ([]string, error) {
	var immutable []string

	threshold := l.logCombiningThreshold()

	for i, fileName := range fileNames {
		if !strings.HasSuffix(fileName, ".condensed") {
			continue
		}

		if i == len(fileNames)-1 {
			// this is the last file, not immutable
			break
		}

		if !strings.HasSuffix(fileNames[i+1], ".condensed") {
			// the next file is not a condensed file, we can stop here
			break
		}

		currentStat, err := os.Stat(fileName)
		if err != nil {
			return nil, errors.Wrapf(err, "stat file %q", fileName)
		}

		if currentStat.Size() > threshold {
			// already above threshold, immutable
			immutable = append(immutable, fileName)
			continue
		}

		nextStat, err := os.Stat(fileNames[i+1])
		if err != nil {
			return nil, errors.Wrapf(err, "stat file %q", fileNames[i+1])
		}

		if currentStat.Size()+nextStat.Size() > threshold {
			// combining those two would exceed threshold, immutable
			immutable = append(immutable, fileName)
			continue
		}
	}

	return immutable, nil
}

func readLastSnapshot(rootPath, name string, logger logrus.FieldLogger) (snap *DeserializationResult, ts int64, err error) {
	dir := commitLogDirectory(rootPath, name)
	err = os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return nil, 0, errors.Wrap(err, "create commit logger directory")
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, 0, errors.Wrap(err, "browse commit logger directory")
	}

	for i := len(files) - 1; i >= 0; i-- {
		info := files[i]
		path := filepath.Join(dir, info.Name())

		if strings.HasSuffix(info.Name(), ".snapshot.tmp") {
			// a temporary snapshot file was found which means that the snapshoting
			// process never completed, this file is thus considered corrupt (too
			// short) and must be deleted. The commit log is never deleted so it's safe to
			// delete this without data loss.
			if err := os.Remove(path); err != nil {
				return nil, 0, errors.Wrap(err, "remove tmp snapshot file")
			}
			// the checkpoints file should also be removed if it exists
			// as it's created right after the temporary snapshot file
			cpfn := path + ".checkpoints"
			if err := os.Remove(cpfn); err != nil {
				if !os.IsNotExist(err) {
					return nil, 0, errors.Wrapf(err, "remove checkpoints file %q", cpfn)
				}
			}

			logger.WithField("action", "hnsw_remove_tmp_snapshot").
				WithField("path", path).
				Warn("remove tmp snapshot file")

			continue
		}

		if !strings.HasSuffix(info.Name(), ".snapshot") {
			// not a snapshot file
			continue
		}

		if snap == nil {
			checkpoints, err := readCheckpoints(path)
			if err != nil {
				// if for any reason the checkpoints file is not found or corrupted
				// we need to remove the snapshot file and create a new one from the commit log.
				if err := os.Remove(path); err != nil {
					return nil, 0, errors.Wrapf(err, "remove snapshot file %q", path)
				}
				cpfn := path + ".checkpoints"
				if err := os.Remove(cpfn); err != nil {
					if !os.IsNotExist(err) {
						return nil, 0, errors.Wrapf(err, "remove checkpoints file %q", cpfn)
					}
				}

				logger.WithField("action", "hnsw_remove_corrupt_snapshot").
					WithField("path", path).
					WithField("checkpoints", checkpoints).
					Warn("checkpoints file not found or corrupted, removing snapshot file")
				return nil, 0, nil
			}

			snap, err = readStateFrom(path, 8, checkpoints, logger)
			if err != nil {
				// if for any reason the snapshot file is not found or corrupted
				// we need to remove the snapshot file and create a new one from the commit log.
				if err := os.Remove(path); err != nil {
					return nil, 0, errors.Wrapf(err, "remove snapshot file %q", path)
				}
				cpfn := path + ".checkpoints"
				if err := os.Remove(cpfn); err != nil {
					if !os.IsNotExist(err) {
						return nil, 0, errors.Wrapf(err, "remove checkpoints file %q", cpfn)
					}
				}

				logger.WithField("action", "hnsw_remove_corrupt_snapshot").
					WithField("path", path).
					WithField("checkpoints", checkpoints).
					Warn("snapshot file not found or corrupted, removing snapshot file")

				return nil, 0, nil
			}

			ts, err = snapshotTimestamp(info.Name())
			if err != nil {
				return nil, 0, errors.Wrapf(err, "read snapshot timestamp %q", path)
			}

			continue
		}

		// remove older snapshots
		if err := os.Remove(path); err != nil {
			return nil, 0, errors.Wrapf(err, "remove snapshot file %q", path)
		}

		cpfn := path + ".checkpoints"
		if err := os.Remove(cpfn); err != nil {
			return nil, 0, errors.Wrapf(err, "remove checkpoints file %q", cpfn)
		}
	}

	return snap, ts, nil
}

// returns checkpoints which can be used as parallelizatio hints
func writeStateTo(state *DeserializationResult, w io.Writer) ([]Checkpoint, error) {
	// version
	offset := 0
	if err := writeByte(w, 0); err != nil {
		return nil, err
	}
	offset += writeByteSize

	if err := writeUint64(w, state.Entrypoint); err != nil {
		return nil, err
	}
	offset += writeUint64Size

	if err := writeUint16(w, state.Level); err != nil {
		return nil, err
	}
	offset += writeUint16Size

	if err := writeBool(w, state.Compressed); err != nil {
		return nil, err
	}
	offset += writeByteSize

	if state.Compressed {
		if err := writeUint16(w, state.CompressionPQData.Ks); err != nil {
			return nil, err
		}
		offset += writeUint16Size

		if err := writeUint16(w, state.CompressionPQData.M); err != nil {
			return nil, err
		}
		offset += writeUint16Size

		if err := writeUint16(w, state.CompressionPQData.Dimensions); err != nil {
			return nil, err
		}
		offset += writeUint16Size

		if err := writeByte(w, byte(state.CompressionPQData.EncoderType)); err != nil {
			return nil, err
		}
		offset += writeByteSize

		if err := writeByte(w, state.CompressionPQData.EncoderDistribution); err != nil {
			return nil, err
		}
		offset += writeByteSize

		if err := writeBool(w, state.CompressionPQData.UseBitsEncoding); err != nil {
			return nil, err
		}
		offset += writeByteSize

		for _, encoder := range state.CompressionPQData.Encoders {
			if n, err := w.Write(encoder.ExposeDataForRestore()); err != nil {
				return nil, err
			} else {
				offset += n
			}
		}
	}

	if err := writeUint32(w, uint32(len(state.Nodes))); err != nil {
		return nil, err
	}
	offset += writeUint32Size

	var checkpoints []Checkpoint
	// start at the very first node
	checkpoints = append(checkpoints, Checkpoint{NodeID: 0, Offset: uint64(offset)})

	nonNilNodes := 0

	for i, n := range state.Nodes {
		if n == nil {
			// nil node
			if err := writeByte(w, 0); err != nil {
				return nil, err
			}
			offset += writeByteSize
			continue
		}

		if nonNilNodes%checkpointChunkSize == 0 && nonNilNodes > 0 {
			checkpoints = append(checkpoints, Checkpoint{NodeID: uint64(i), Offset: uint64(offset)})
		}

		_, hasATombstone := state.Tombstones[n.id]
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

	// note that we are not adding an end checkpoint, so the implicit contract
	// here is that the reader must read from the last checkpoint to EOF.
	return checkpoints, nil
}

func readStateFrom(filename string, concurrency int, checkpoints []Checkpoint,
	logger logrus.FieldLogger,
) (*DeserializationResult, error) {
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

	// start with a single-threaded reader until we make it the nodes section
	r := bufio.NewReader(f)

	var b [8]byte

	_, err = io.ReadFull(r, b[:1]) // version
	if err != nil {
		return nil, errors.Wrapf(err, "read version")
	}
	if b[0] != 0 {
		return nil, fmt.Errorf("unsupported version %d", b[0])
	}

	_, err = io.ReadFull(r, b[:8]) // entrypoint
	if err != nil {
		return nil, errors.Wrapf(err, "read entrypoint")
	}
	res.Entrypoint = binary.LittleEndian.Uint64(b[:8])

	_, err = io.ReadFull(r, b[:2]) // level
	if err != nil {
		return nil, errors.Wrapf(err, "read level")
	}
	res.Level = binary.LittleEndian.Uint16(b[:2])

	_, err = io.ReadFull(r, b[:1]) // compressed
	if err != nil {
		return nil, errors.Wrapf(err, "read compressed")
	}
	res.Compressed = b[0] == 1

	// PQ data
	if res.Compressed {
		_, err = io.ReadFull(r, b[:2]) // PQData.Ks
		if err != nil {
			return nil, errors.Wrapf(err, "read PQData.Ks")
		}
		ks := binary.LittleEndian.Uint16(b[:2])

		_, err = io.ReadFull(r, b[:2]) // PQData.M
		if err != nil {
			return nil, errors.Wrapf(err, "read PQData.M")
		}
		m := binary.LittleEndian.Uint16(b[:2])

		_, err = io.ReadFull(r, b[:2]) // PQData.Dimensions
		if err != nil {
			return nil, errors.Wrapf(err, "read PQData.Dimensions")
		}
		dims := binary.LittleEndian.Uint16(b[:2])

		_, err = io.ReadFull(r, b[:1]) // PQData.EncoderType
		if err != nil {
			return nil, errors.Wrapf(err, "read PQData.EncoderType")
		}
		encoderType := compressionhelpers.Encoder(b[0])

		_, err = io.ReadFull(r, b[:1]) // PQData.EncoderDistribution
		if err != nil {
			return nil, errors.Wrapf(err, "read PQData.EncoderDistribution")
		}
		dist := b[0]

		_, err = io.ReadFull(r, b[:1]) // PQData.UseBitsEncoding
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
			encoder, err := encoderReader(r, res.CompressionPQData, i)
			if err != nil {
				return nil, err
			}
			res.CompressionPQData.Encoders = append(res.CompressionPQData.Encoders, encoder)
		}
	}

	_, err = io.ReadFull(r, b[:4]) // nodes
	if err != nil {
		return nil, errors.Wrapf(err, "read nodes count")
	}
	nodesCount := int(binary.LittleEndian.Uint32(b[:4]))

	res.Nodes = make([]*vertex, nodesCount)

	eg := enterrors.NewErrorGroupWrapper(logger)
	eg.SetLimit(concurrency)
	for cpPos, cp := range checkpoints {
		start := int(cp.Offset)
		var end int
		if cpPos != len(checkpoints)-1 {
			end = int(checkpoints[cpPos+1].Offset)
		} else {
			st, err := f.Stat()
			if err != nil {
				return nil, errors.Wrapf(err, "get file stat")
			}

			end = int(st.Size())
		}

		eg.Go(func() error {
			var b [8]byte
			var read int

			currNodeID := cp.NodeID
			sr := io.NewSectionReader(f, int64(start), int64(end-start))
			r := bufio.NewReader(sr)

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
					res.Tombstones[node.id] = struct{}{}
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

				res.Nodes[currNodeID] = node
				currNodeID++
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

type Checkpoint struct {
	NodeID uint64
	Offset uint64
}

func writeCheckpoints(fileName string, checkpoints []Checkpoint) error {
	checkpointFile, err := os.OpenFile(fileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o666)
	if err != nil {
		return fmt.Errorf("open new checkpoint file for writing: %w", err)
	}
	defer checkpointFile.Close()

	// 0-4: checksum
	// 4+: checkpoints (16 bytes each)
	buffer := make([]byte, 4+len(checkpoints)*16)
	offset := 4

	for _, cp := range checkpoints {
		binary.LittleEndian.PutUint64(buffer[offset:offset+8], cp.NodeID)
		offset += 8
		binary.LittleEndian.PutUint64(buffer[offset:offset+8], cp.Offset)
		offset += 8
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

	checkpoints = make([]Checkpoint, 0, len(buf[4:])/16)
	for i := 4; i < len(buf); i += 16 {
		id := binary.LittleEndian.Uint64(buf[i : i+8])
		offset := binary.LittleEndian.Uint64(buf[i+8 : i+16])
		checkpoints = append(checkpoints, Checkpoint{NodeID: id, Offset: offset})
	}

	return checkpoints, nil
}
