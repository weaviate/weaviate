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

package lsmkv

import (
	"fmt"
	"regexp"
	"strconv"
	"sync"

	"github.com/weaviate/sroar"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringsetrange"
)

type lazySegment struct {
	path        string
	logger      logrus.FieldLogger
	metrics     *Metrics
	existsLower existsOnLowerSegmentsFn
	cfg         segmentConfig

	segment *segment
	mux     sync.Mutex
}

func newLazySegment(path string, logger logrus.FieldLogger, metrics *Metrics,
	existsLower existsOnLowerSegmentsFn, cfg segmentConfig,
) (*lazySegment, error) {
	if metrics != nil && metrics.LazySegmentInit != nil {
		metrics.LazySegmentInit.Inc()
	}

	return &lazySegment{
		path:        path,
		logger:      logger,
		metrics:     metrics,
		existsLower: existsLower,
		cfg:         cfg,
	}, nil
}

func (s *lazySegment) load() error {
	s.mux.Lock()
	defer s.mux.Unlock()

	if s.segment == nil {
		segment, err := newSegment(s.path, s.logger, s.metrics, s.existsLower, s.cfg)
		if err != nil {
			return err
		}
		s.segment = segment
		if s.metrics != nil && s.metrics.LazySegmentLoad != nil {
			s.metrics.LazySegmentLoad.Inc()
		}
	}

	return nil
}

func (s *lazySegment) mustLoad() {
	err := s.load()
	if err != nil {
		panic(fmt.Errorf("error loading segment %q: %w", s.path, err))
	}
}

func (s *lazySegment) getPath() string {
	return s.path
}

func (s *lazySegment) setPath(path string) {
	s.mustLoad()
	s.segment.setPath(path)
}

func (s *lazySegment) getStrategy() segmentindex.Strategy {
	strategy, found := s.numberFromPath("s")
	if found {
		return segmentindex.Strategy(strategy)
	}
	s.mustLoad()
	return s.segment.getStrategy()
}

func (s *lazySegment) getSecondaryIndexCount() uint16 {
	s.mustLoad()
	return s.segment.getSecondaryIndexCount()
}

func (s *lazySegment) getLevel() uint16 {
	level, found := s.numberFromPath("l")
	if found {
		return uint16(level)
	}

	s.mustLoad()
	return s.segment.getLevel()
}

func (s *lazySegment) getSize() int64 {
	s.mustLoad()
	return s.segment.getSize()
}

func (s *lazySegment) setSize(size int64) {
	s.mustLoad()
	s.segment.setSize(size)
}

func (s *lazySegment) PayloadSize() int {
	s.mustLoad()
	return s.segment.PayloadSize()
}

func (s *lazySegment) Size() int {
	s.mustLoad()
	return s.segment.Size()
}

func (s *lazySegment) close() error {
	s.mux.Lock()
	defer s.mux.Unlock()

	if s.metrics != nil && s.metrics.LazySegmentClose != nil {
		s.metrics.LazySegmentClose.Inc()
	}
	if s.segment == nil {
		return nil
	}
	if s.metrics != nil && s.metrics.LazySegmentUnLoad != nil {
		s.metrics.LazySegmentUnLoad.Inc()
	}
	return s.segment.close()
}

func (s *lazySegment) get(key []byte) ([]byte, error) {
	s.mustLoad()
	return s.segment.get(key)
}

func (s *lazySegment) getBySecondaryIntoMemory(pos int, key []byte, buffer []byte) ([]byte, []byte, []byte, error) {
	s.mustLoad()
	return s.segment.getBySecondaryIntoMemory(pos, key, buffer)
}

func (s *lazySegment) getCollection(key []byte) ([]value, error) {
	s.mustLoad()
	return s.segment.getCollection(key)
}

func (s *lazySegment) getInvertedData() *segmentInvertedData {
	s.mustLoad()
	return s.segment.getInvertedData()
}

func (s *lazySegment) getSegment() *segment {
	s.mustLoad()
	return s.segment
}

func (s *lazySegment) isLoaded() bool {
	s.mux.Lock()
	defer s.mux.Unlock()

	return s.segment != nil
}

func (s *lazySegment) markForDeletion() error {
	s.mustLoad()
	return s.segment.markForDeletion()
}

func (s *lazySegment) MergeTombstones(other *sroar.Bitmap) (*sroar.Bitmap, error) {
	s.mustLoad()
	return s.segment.MergeTombstones(other)
}

func (s *lazySegment) newCollectionCursor() *segmentCursorCollection {
	s.mustLoad()
	return s.segment.newCollectionCursor()
}

func (s *lazySegment) newCollectionCursorReusable() *segmentCursorCollectionReusable {
	s.mustLoad()
	return s.segment.newCollectionCursorReusable()
}

func (s *lazySegment) newCursor() *segmentCursorReplace {
	s.mustLoad()
	return s.segment.newCursor()
}

func (s *lazySegment) newCursorWithSecondaryIndex(pos int) *segmentCursorReplace {
	s.mustLoad()
	return s.segment.newCursorWithSecondaryIndex(pos)
}

func (s *lazySegment) newMapCursor() *segmentCursorMap {
	s.mustLoad()
	return s.segment.newMapCursor()
}

func (s *lazySegment) newNodeReader(offset nodeOffset, operation string) (*nodeReader, error) {
	s.mustLoad()
	return s.segment.newNodeReader(offset, operation)
}

func (s *lazySegment) newRoaringSetCursor() *roaringset.SegmentCursor {
	s.mustLoad()
	return s.segment.newRoaringSetCursor()
}

func (s *lazySegment) newRoaringSetRangeCursor() roaringsetrange.SegmentCursor {
	s.mustLoad()
	return s.segment.newRoaringSetRangeCursor()
}

func (s *lazySegment) newRoaringSetRangeReader() *roaringsetrange.SegmentReader {
	s.mustLoad()
	return s.segment.newRoaringSetRangeReader()
}

func (s *lazySegment) quantileKeys(q int) [][]byte {
	s.mustLoad()
	return s.segment.quantileKeys(q)
}

func (s *lazySegment) ReadOnlyTombstones() (*sroar.Bitmap, error) {
	s.mustLoad()
	return s.segment.ReadOnlyTombstones()
}

func (s *lazySegment) replaceStratParseData(in []byte) ([]byte, []byte, error) {
	s.mustLoad()
	return s.segment.replaceStratParseData(in)
}

func (s *lazySegment) roaringSetGet(key []byte) (roaringset.BitmapLayer, error) {
	s.mustLoad()
	return s.segment.roaringSetGet(key)
}

func (s *lazySegment) numberFromPath(str string) (int, bool) {
	template := fmt.Sprintf(`\.%s(\d+)\.`, str)
	re := regexp.MustCompile(template)
	match := re.FindStringSubmatch(s.path)
	if len(match) > 1 {
		num, err := strconv.Atoi(match[1])
		if err == nil {
			return num, true
		}
	}
	return 0, false
}
