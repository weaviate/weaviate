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
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/lsmkv"
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



func (s *segment) close() error {
	var munmapErr, fileCloseErr error

	m := mmap.MMap(s.contents)
	munmapErr = m.Unmap()
	if s.contentFile != nil {
		fileCloseErr = s.contentFile.Close()
	}

	if munmapErr != nil || fileCloseErr != nil {
		return fmt.Errorf("close segment: munmap: %w, close contents file: %w", munmapErr, fileCloseErr)
	}

	return nil
}




const DeleteMarkerSuffix = ".deleteme"



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
