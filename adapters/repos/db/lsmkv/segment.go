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

package lsmkv

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/pkg/errors"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/diskio"
	"github.com/weaviate/weaviate/entities/lsmkv"
	entsentry "github.com/weaviate/weaviate/entities/sentry"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/mmap"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type segment struct {
	path                string
	level               uint16
	secondaryIndexCount uint16
	version             uint16
	segmentStartPos     uint64
	segmentEndPos       uint64
	dataStartPos        uint64
	dataEndPos          uint64
	contents            []byte
	contentFile         *os.File
	strategy            segmentindex.Strategy
	index               diskIndex
	secondaryIndices    []diskIndex
	logger              logrus.FieldLogger
	metrics             *Metrics
	size                int64
	readFromMemory      bool
	unMapContents       bool

	useBloomFilter        bool // see bucket for more datails
	bloomFilter           *bloom.BloomFilter
	secondaryBloomFilters []*bloom.BloomFilter
	bloomFilterMetrics    *bloomFilterMetrics

	// the net addition this segment adds with respect to all previous segments
	calcCountNetAdditions bool // see bucket for more datails
	countNetAdditions     int

	invertedHeader *segmentindex.HeaderInverted
	invertedData   *segmentInvertedData

	observeMetaWrite diskio.MeteredWriterCallback // used for precomputing meta (cna + bloom)
}

type diskIndex interface {
	// Get return lsmkv.NotFound in case no node can be found
	Get(key []byte) (segmentindex.Node, error)

	// Seek returns lsmkv.NotFound in case the seek value is larger than
	// the highest value in the collection, otherwise it returns the next highest
	// value (or the exact value if present)
	Seek(key []byte) (segmentindex.Node, error)

	Next(key []byte) (segmentindex.Node, error)

	// AllKeys in no specific order, e.g. for building a bloom filter
	AllKeys() ([][]byte, error)

	// Size of the index in bytes
	Size() int

	QuantileKeys(q int) [][]byte
}

type segmentConfig struct {
	mmapContents             bool
	useBloomFilter           bool
	calcCountNetAdditions    bool
	overwriteDerived         bool
	enableChecksumValidation bool
	MinMMapSize              int64
	allocChecker             memwatch.AllocChecker
}

// newSegment creates a new segment structure, representing an LSM disk segment.
//
// This function is partially copied by a function called preComputeSegmentMeta.
// Any changes made here should likely be made in preComputeSegmentMeta as well,
// and vice versa. This is absolutely not ideal, but in the short time I was able
// to consider this, I wasn't able to find a way to unify the two -- there are
// subtle differences.
func newSegment(path string, logger logrus.FieldLogger, metrics *Metrics,
	existsLower existsOnLowerSegmentsFn, cfg segmentConfig,
) (_ *segment, rerr error) {
	defer func() {
		p := recover()
		if p == nil {
			return
		}
		entsentry.Recover(p)
		rerr = fmt.Errorf("unexpected error loading segment %q: %v", path, p)
	}()

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}

	// The lifetime of the `file` exceeds this constructor as we store the open file for later use in `contentFile`.
	// invariant: We close **only** if any error happened after successfully opening the file. To avoid leaking open file descriptor.
	// NOTE: This `defer` works even with `err` being shadowed in the whole function because defer checks for named `rerr` return value.
	defer func() {
		if rerr != nil {
			file.Close()
		}
	}()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat file: %w", err)
	}
	size := fileInfo.Size()

	// mmap has some overhead, we can read small files directly to memory
	var contents []byte
	var unMapContents bool
	var allocCheckerErr error

	if size <= cfg.MinMMapSize { // check if it is a candidate for full reading
		if cfg.allocChecker == nil {
			logger.WithFields(logrus.Fields{
				"path":        path,
				"size":        size,
				"minMMapSize": cfg.MinMMapSize,
			}).Info("allocChecker is nil, skipping memory pressure check for new segment")
		} else {
			allocCheckerErr = cfg.allocChecker.CheckAlloc(size) // check if we have enough memory
			if allocCheckerErr != nil {
				logger.Debugf("memory pressure: cannot fully read segment")
			}
		}
	}

	useBloomFilter := cfg.useBloomFilter
	readFromMemory := cfg.mmapContents
	if size > cfg.MinMMapSize || cfg.allocChecker == nil || allocCheckerErr != nil { // mmap the file if it's too large or if we have memory pressure
		contents2, err := mmap.MapRegion(file, int(fileInfo.Size()), mmap.RDONLY, 0, 0)
		if err != nil {
			return nil, fmt.Errorf("mmap file: %w", err)
		}
		contents = contents2
		unMapContents = true
	} else { // read the file into memory if it's small enough and we have enough memory
		meteredF := diskio.NewMeteredReader(file, diskio.MeteredReaderCallback(metrics.ReadObserver("readSegmentFile")))
		bufio.NewReader(meteredF)
		contents, err = io.ReadAll(meteredF)
		if err != nil {
			return nil, fmt.Errorf("read file: %w", err)
		}
		unMapContents = false
		readFromMemory = true
		useBloomFilter = false
	}
	header, err := segmentindex.ParseHeader(contents[:segmentindex.HeaderSize])
	if err != nil {
		return nil, fmt.Errorf("parse header: %w", err)
	}

	if err := segmentindex.CheckExpectedStrategy(header.Strategy); err != nil {
		return nil, fmt.Errorf("unsupported strategy in segment: %w", err)
	}

	logger.WithField("size", fileInfo.Size()).
		Infof("validating checksum segment %q", path)

	start := time.Now()

	file.Seek(0, io.SeekStart)
	segmentFile := segmentindex.NewSegmentFile(segmentindex.WithReader(file))
	headerSize := int64(segmentindex.HeaderSize)
	if header.Strategy == segmentindex.StrategyInverted {
		headerSize += int64(segmentindex.HeaderInvertedSize)
	}
	fileSize := fileInfo.Size()
	if err := segmentFile.ValidateChecksum(fileSize, headerSize); err != nil {
		return nil, fmt.Errorf("validate segment %q: %w", path, err)
	}
	total := time.Since(start) / time.Millisecond
	if total == 0 {
		total = 1 // avoid division by zero in the log
	}

	logger.WithField("duration", total).
		WithField("size", fileInfo.Size()).
		WithField("rate", float64(fileSize)/float64(total)).
		Infof("validated checksum segment %q", path)

	primaryIndex, err := header.PrimaryIndex(contents)
	if err != nil {
		return nil, fmt.Errorf("extract primary index position: %w", err)
	}

	primaryDiskIndex := segmentindex.NewDiskTree(primaryIndex)

	dataStartPos := uint64(segmentindex.HeaderSize)
	dataEndPos := header.IndexStart

	var invertedHeader *segmentindex.HeaderInverted
	if header.Strategy == segmentindex.StrategyInverted {
		invertedHeader, err = segmentindex.LoadHeaderInverted(contents[segmentindex.HeaderSize : segmentindex.HeaderSize+segmentindex.HeaderInvertedSize])
		if err != nil {
			return nil, errors.Wrap(err, "load inverted header")
		}
		dataStartPos = invertedHeader.KeysOffset
		dataEndPos = invertedHeader.TombstoneOffset
	}

	stratLabel := header.Strategy.String()
	observeWrite := monitoring.GetMetrics().FileIOWrites.With(prometheus.Labels{
		"strategy":  stratLabel,
		"operation": "segmentMetadata",
	})

	if unMapContents {
		// a map was created, track it
		monitoring.GetMetrics().MmapOperations.With(prometheus.Labels{
			"operation": "mmap",
			"strategy":  stratLabel,
		}).Inc()
	}

	seg := &segment{
		level:                 header.Level,
		path:                  path,
		contents:              contents,
		version:               header.Version,
		secondaryIndexCount:   header.SecondaryIndices,
		segmentStartPos:       header.IndexStart,
		segmentEndPos:         uint64(size),
		strategy:              header.Strategy,
		dataStartPos:          dataStartPos,
		dataEndPos:            dataEndPos,
		index:                 primaryDiskIndex,
		logger:                logger,
		metrics:               metrics,
		size:                  size,
		readFromMemory:        readFromMemory,
		useBloomFilter:        useBloomFilter,
		calcCountNetAdditions: cfg.calcCountNetAdditions,
		invertedHeader:        invertedHeader,
		invertedData: &segmentInvertedData{
			tombstones: sroar.NewBitmap(),
		},
		unMapContents:    unMapContents,
		observeMetaWrite: func(n int64) { observeWrite.Observe(float64(n)) },
	}

	// Using pread strategy requires file to remain open for segment lifetime
	if seg.readFromMemory {
		defer file.Close()
	} else {
		seg.contentFile = file
	}

	if seg.secondaryIndexCount > 0 {
		seg.secondaryIndices = make([]diskIndex, seg.secondaryIndexCount)
		for i := range seg.secondaryIndices {
			secondary, err := header.SecondaryIndex(contents, uint16(i))
			if err != nil {
				return nil, fmt.Errorf("get position for secondary index at %d: %w", i, err)
			}
			seg.secondaryIndices[i] = segmentindex.NewDiskTree(secondary)
		}
	}

	if seg.useBloomFilter {
		if err := seg.initBloomFilters(metrics, cfg.overwriteDerived); err != nil {
			return nil, err
		}
	}
	if seg.calcCountNetAdditions {
		if err := seg.initCountNetAdditions(existsLower, cfg.overwriteDerived); err != nil {
			return nil, err
		}
	}

	if seg.strategy == segmentindex.StrategyInverted {
		_, err := seg.loadTombstones()
		if err != nil {
			return nil, fmt.Errorf("load tombstones: %w", err)
		}

		_, err = seg.loadPropertyLengths()
		if err != nil {
			return nil, fmt.Errorf("load property lengths: %w", err)
		}

	}

	return seg, nil
}

func (s *segment) close() error {
	var munmapErr, fileCloseErr error
	if s.unMapContents {
		m := mmap.MMap(s.contents)
		munmapErr = m.Unmap()
		stratLabel := s.strategy.String()
		monitoring.GetMetrics().MmapOperations.With(prometheus.Labels{
			"operation": "munmap",
			"strategy":  stratLabel,
		}).Inc()
	}
	if s.contentFile != nil {
		fileCloseErr = s.contentFile.Close()
	}

	if munmapErr != nil || fileCloseErr != nil {
		return fmt.Errorf("close segment: munmap: %w, close contents file: %w", munmapErr, fileCloseErr)
	}

	return nil
}

func (s *segment) dropImmediately() error {
	// support for persisting bloom filters and cnas was added in v1.17,
	// therefore the files may not be present on segments created with previous
	// versions. By using RemoveAll, which does not error on NotExists, these
	// drop calls are backward-compatible:
	if err := os.RemoveAll(s.bloomFilterPath()); err != nil {
		return fmt.Errorf("drop bloom filter: %w", err)
	}

	for i := 0; i < int(s.secondaryIndexCount); i++ {
		if err := os.RemoveAll(s.bloomFilterSecondaryPath(i)); err != nil {
			return fmt.Errorf("drop bloom filter: %w", err)
		}
	}

	if err := os.RemoveAll(s.countNetPath()); err != nil {
		return fmt.Errorf("drop count net additions file: %w", err)
	}

	// for the segment itself, we're not using RemoveAll, but Remove. If there
	// was a NotExists error here, something would be seriously wrong, and we
	// don't want to ignore it.
	if err := os.Remove(s.path); err != nil {
		return fmt.Errorf("drop segment: %w", err)
	}

	return nil
}

func (s *segment) dropMarked() error {
	// support for persisting bloom filters and cnas was added in v1.17,
	// therefore the files may not be present on segments created with previous
	// versions. By using RemoveAll, which does not error on NotExists, these
	// drop calls are backward-compatible:
	if err := os.RemoveAll(s.bloomFilterPath() + DeleteMarkerSuffix); err != nil {
		return fmt.Errorf("drop previously marked bloom filter: %w", err)
	}

	for i := 0; i < int(s.secondaryIndexCount); i++ {
		if err := os.RemoveAll(s.bloomFilterSecondaryPath(i) + DeleteMarkerSuffix); err != nil {
			return fmt.Errorf("drop previously marked secondary bloom filter: %w", err)
		}
	}

	if err := os.RemoveAll(s.countNetPath() + DeleteMarkerSuffix); err != nil {
		return fmt.Errorf("drop previously marked count net additions file: %w", err)
	}

	// for the segment itself, we're not using RemoveAll, but Remove. If there
	// was a NotExists error here, something would be seriously wrong, and we
	// don't want to ignore it.
	if err := os.Remove(s.path + DeleteMarkerSuffix); err != nil {
		return fmt.Errorf("drop previously marked segment: %w", err)
	}

	return nil
}

const DeleteMarkerSuffix = ".deleteme"

func markDeleted(path string) error {
	return os.Rename(path, path+DeleteMarkerSuffix)
}

func (s *segment) markForDeletion() error {
	// support for persisting bloom filters and cnas was added in v1.17,
	// therefore the files may not be present on segments created with previous
	// versions. If we get a not exist error, we ignore it.
	if err := markDeleted(s.bloomFilterPath()); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("mark bloom filter deleted: %w", err)
		}
	}

	for i := 0; i < int(s.secondaryIndexCount); i++ {
		if err := markDeleted(s.bloomFilterSecondaryPath(i)); err != nil {
			if !os.IsNotExist(err) {
				return fmt.Errorf("mark secondary bloom filter deleted: %w", err)
			}
		}
	}

	if err := markDeleted(s.countNetPath()); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("mark count net additions file deleted: %w", err)
		}
	}

	// for the segment itself, we're not accepting a NotExists error. If there
	// was a NotExists error here, something would be seriously wrong, and we
	// don't want to ignore it.
	if err := markDeleted(s.path); err != nil {
		return fmt.Errorf("mark segment deleted: %w", err)
	}

	return nil
}

// Size returns the total size of the segment in bytes, including the header
// and index
func (s *segment) Size() int {
	return int(s.size)
}

// PayloadSize is only the payload of the index, excluding the index
func (s *segment) PayloadSize() int {
	return int(s.dataEndPos)
}

type nodeReader struct {
	r io.Reader
}

func (n *nodeReader) Read(b []byte) (int, error) {
	return n.r.Read(b)
}

type nodeOffset struct {
	start, end uint64
}

func (s *segment) newNodeReader(offset nodeOffset, operation string) (*nodeReader, error) {
	var (
		r   io.Reader
		err error
	)
	if s.readFromMemory {
		contents := s.contents[offset.start:]
		if offset.end != 0 {
			contents = s.contents[offset.start:offset.end]
		}
		r, err = s.bytesReaderFrom(contents)
	} else {
		r, err = s.bufferedReaderAt(offset.start, "ReadFromSegment"+operation)
	}
	if err != nil {
		return nil, fmt.Errorf("new nodeReader: %w", err)
	}
	return &nodeReader{r: r}, nil
}

func (s *segment) copyNode(b []byte, offset nodeOffset) error {
	if s.readFromMemory {
		copy(b, s.contents[offset.start:offset.end])
		return nil
	}
	n, err := s.newNodeReader(offset, "copyNode")
	if err != nil {
		return fmt.Errorf("copy node: %w", err)
	}
	_, err = io.ReadFull(n, b)
	return err
}

func (s *segment) bytesReaderFrom(in []byte) (*bytes.Reader, error) {
	if len(in) == 0 {
		return nil, lsmkv.NotFound
	}
	return bytes.NewReader(in), nil
}

func (s *segment) bufferedReaderAt(offset uint64, operation string) (io.Reader, error) {
	if s.contentFile == nil {
		return nil, fmt.Errorf("nil contentFile for segment at %s", s.path)
	}

	meteredF := diskio.NewMeteredReader(s.contentFile, diskio.MeteredReaderCallback(s.metrics.ReadObserver(operation)))
	r := io.NewSectionReader(meteredF, int64(offset), s.size)

	return bufio.NewReader(r), nil
}
