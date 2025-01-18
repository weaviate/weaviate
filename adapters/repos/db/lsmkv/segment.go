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

	"github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/lsmkv"
	entsentry "github.com/weaviate/weaviate/entities/sentry"
	"github.com/willf/bloom"
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
	mmapContents        bool

	useBloomFilter        bool // see bucket for more datails
	bloomFilter           *bloom.BloomFilter
	secondaryBloomFilters []*bloom.BloomFilter
	bloomFilterMetrics    *bloomFilterMetrics

	// the net addition this segment adds with respect to all previous segments
	calcCountNetAdditions bool // see bucket for more datails
	countNetAdditions     int

	invertedHeader *segmentindex.HeaderInverted
	invertedData   *segmentInvertedData
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
) (_ *segment, err error) {
	defer func() {
		p := recover()
		if p == nil {
			return
		}
		entsentry.Recover(p)
		err = fmt.Errorf("unexpected error loading segment %q: %v", path, p)
	}()

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat file: %w", err)
	}
	size := fileInfo.Size()

	contents, err := mmap.MapRegion(file, int(fileInfo.Size()), mmap.RDONLY, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("mmap file: %w", err)
	}

	header, err := segmentindex.ParseHeader(bytes.NewReader(contents[:segmentindex.HeaderSize]))
	if err != nil {
		return nil, fmt.Errorf("parse header: %w", err)
	}

	if err := segmentindex.CheckExpectedStrategy(header.Strategy); err != nil {
		return nil, fmt.Errorf("unsupported strategy in segment: %w", err)
	}

	if header.Version >= segmentindex.SegmentV1 && cfg.enableChecksumValidation {
		segmentFile := segmentindex.NewSegmentFile(segmentindex.WithReader(file))
		if err := segmentFile.ValidateChecksum(fileInfo); err != nil {
			return nil, fmt.Errorf("validate segment %q: %w", path, err)
		}
	}

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
		mmapContents:          cfg.mmapContents,
		useBloomFilter:        cfg.useBloomFilter,
		calcCountNetAdditions: cfg.calcCountNetAdditions,
		invertedHeader:        invertedHeader,
		invertedData:          &segmentInvertedData{},
	}

	// Using pread strategy requires file to remain open for segment lifetime
	if seg.mmapContents {
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

	return seg, nil
}

func (s *segment) close() error {
	var munmapErr, fileCloseErr error

	m := mmap.MMap(s.contents)
	munmapErr = m.Unmap()
	if s.contentFile != nil {
		fileCloseErr = s.contentFile.Close()
	}

	if munmapErr != nil || fileCloseErr != nil {
		return fmt.Errorf("close segment: munmap: %v, close contents file: %w", munmapErr, fileCloseErr)
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

func (s *segment) newNodeReader(offset nodeOffset) (*nodeReader, error) {
	var (
		r   io.Reader
		err error
	)
	if s.mmapContents {
		contents := s.contents[offset.start:]
		if offset.end != 0 {
			contents = s.contents[offset.start:offset.end]
		}
		r, err = s.bytesReaderFrom(contents)
	} else {
		r, err = s.bufferedReaderAt(offset.start)
	}
	if err != nil {
		return nil, fmt.Errorf("new nodeReader: %w", err)
	}
	return &nodeReader{r: r}, nil
}

func (s *segment) copyNode(b []byte, offset nodeOffset) error {
	if s.mmapContents {
		copy(b, s.contents[offset.start:offset.end])
		return nil
	}
	n, err := s.newNodeReader(offset)
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

func (s *segment) bufferedReaderAt(offset uint64) (*bufio.Reader, error) {
	if s.contentFile == nil {
		return nil, fmt.Errorf("nil contentFile for segment at %s", s.path)
	}

	r := io.NewSectionReader(s.contentFile, int64(offset), s.size)
	return bufio.NewReader(r), nil
}
