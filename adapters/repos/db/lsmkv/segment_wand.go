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
	"encoding/binary"
	"fmt"
	"os"
	"strconv"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

var (
	TOMBSTONE_PATTERN                = []byte{0x01, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	BAD_TOMBSTONE_PATTERN            = []byte{0x00, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	NON_TOMBSTONE_PATTERN            = []byte{0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	BUFFER_SIZE                      = 1024
	FORWARD_JUMP_BUCKET_MINIMUM_SIZE uint64
	FORWARD_JUMP_THRESHOLD           uint64
	FORWARD_JUMP_THRESHOLD_INTERNAL  uint64
	FORWARD_JUMP_UNDERESTIMATE       float64
	FORWARD_JUMP_ENABLED             bool
)

func init() {
	var err error
	FORWARD_JUMP_BUCKET_MINIMUM_SIZE, err = strconv.ParseUint(os.Getenv("FORWARD_JUMP_BUCKET_MINIMUM_SIZE"), 10, 64)
	if err != nil {
		FORWARD_JUMP_BUCKET_MINIMUM_SIZE = 100000
	}

	FORWARD_JUMP_THRESHOLD, err = strconv.ParseUint(os.Getenv("FORWARD_JUMP_THRESHOLD"), 10, 64)
	if err != nil {
		FORWARD_JUMP_THRESHOLD = 1000
	}

	FORWARD_JUMP_THRESHOLD_INTERNAL, err = strconv.ParseUint(os.Getenv("FORWARD_JUMP_THRESHOLD_INTERNAL"), 10, 64)
	if err != nil {
		FORWARD_JUMP_THRESHOLD_INTERNAL = 100
	}

	FORWARD_JUMP_UNDERESTIMATE, err = strconv.ParseFloat(os.Getenv("FORWARD_JUMP_UNDERESTIMATE"), 64)
	if err != nil {
		FORWARD_JUMP_UNDERESTIMATE = 1.0
	}
	FORWARD_JUMP_ENABLED = os.Getenv("FORWARD_JUMP_ENABLED") == "true"
}

func compareByteArrays(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i, val := range a {
		if val != b[i] {
			return false
		}
	}
	return true
}

func (s segment) WandTerm(key []byte, queryTermIndex int, N float64, duplicateTextBoost float64, propertyBoost float64, fullPropertyLen int64, filterDocIds, blockList helpers.AllowList) (terms.Term, error) {
	node, err := s.index.Get(key)
	if err != nil {
		return nil, err
	}
	if s.strategy != segmentindex.StrategyInverted {
		return nil, fmt.Errorf("invalid or unsupported unit")
	}

	term := TermInverted{}
	term.init(N, duplicateTextBoost, s, node.Start, node.End, key, string(key), queryTermIndex, propertyBoost, fullPropertyLen, filterDocIds, blockList)
	return &term, nil
}

func (s segment) Wand2(key []byte) error {
	// node, err := s.index.Get(key)
	node, err := s.index.Get(key)
	if err != nil {
		return err
	}
	actualStart := node.Start + 8
	actualEnd := node.End - 4 - uint64(len(key))
	nodeLen := actualEnd - actualStart
	docCountOffset := nodeOffset{node.Start, node.Start + 8}
	docCountReader, err := s.newNodeReader(docCountOffset)
	if err != nil {
		return err
	}
	var docCount uint64
	if err := binary.Read(docCountReader, binary.LittleEndian, &docCount); err != nil {
		return errors.Wrap(err, "read value length encoding")
	}
	fmt.Println("docCount", docCount)
	likelyNoTombstone := nodeLen%29 == 0
	noTombstone := docCount*29 == nodeLen

	fmt.Println("likelyNoTombstone", likelyNoTombstone)
	fmt.Println("noTombstone", noTombstone)

	offset := nodeOffset{actualStart, actualEnd}

	jumpToDocIdAlignedFound, jumps1, err1 := s.jumpToDocIdAligned(node, key, 74, offset, offset, 0)
	fmt.Println("jumpToDocIdAligned", jumpToDocIdAlignedFound, err1, jumps1)
	jumpToDocIdUnalignedFound, jumps2, err2 := s.jumpToDocIdUnaligned(node, key, 74, offset, offset, 0)
	fmt.Println("jumpToDocIdUnaligned", jumpToDocIdUnalignedFound, err2, jumps2)

	if jumpToDocIdAlignedFound != nil || jumpToDocIdUnalignedFound != nil {
		fmt.Println("found", jumpToDocIdAlignedFound, jumpToDocIdUnalignedFound)
	}
	return nil
}

func (s segment) jumpToDocIdAligned(node segmentindex.Node, key []byte, docId uint64, offset nodeOffset, globalOffset nodeOffset, jumps int) (*nodeOffset, int, error) {
	// splice offset in half
	offsetLen := offset.end - offset.start

	if offsetLen == 0 {
		return nil, jumps, nil
	}

	halfOffsetLen := offsetLen / 2
	// halfOffsetLen should be a multiple of 29
	if halfOffsetLen%29 != 0 {
		halfOffsetLen = halfOffsetLen - (halfOffsetLen % 29)
	}
	// seek to position of docId
	// read docId at halfOffsetLen
	docIdOffsetStart := halfOffsetLen + 11
	// read docId at halfOffsetLen
	docIdOffset := nodeOffset{offset.start + docIdOffsetStart, offset.start + docIdOffsetStart + 8}
	docIdReader, err := s.newNodeReader(docIdOffset)
	if err != nil {
		return nil, jumps, err
	}
	var docIdFound uint64
	if err := binary.Read(docIdReader, binary.BigEndian, &docIdFound); err != nil {
		return nil, jumps, errors.Wrap(err, "read value length encoding")
	}
	fmt.Println("jumped to", docIdFound)
	// if docIdFound is smaller than docId, jump to the right
	if docIdFound < docId {
		offset.start += halfOffsetLen + 29
		return s.jumpToDocIdAligned(node, key, docId, offset, globalOffset, jumps+1)
	}
	// if docIdFound is bigger than docId, jump to the left
	if docIdFound > docId {
		offset.end -= halfOffsetLen + 29
		return s.jumpToDocIdAligned(node, key, docId, offset, globalOffset, jumps+1)
	}
	// if docIdFound is equal to docId, return offset

	return &nodeOffset{offset.start + halfOffsetLen, offset.start + halfOffsetLen + 29}, jumps, nil
}

func (s segment) jumpToDocIdUnaligned(node segmentindex.Node, key []byte, docId uint64, offset nodeOffset, globalOffset nodeOffset, jumps int) (*nodeOffset, int, error) {
	// splice offset in half
	offsetLen := offset.end - offset.start

	if offsetLen == 0 {
		return nil, jumps, nil
	}

	halfOffsetLen := offsetLen
	// if there is a single record in the node, we can't jump to the middle
	if offsetLen > 29 {
		halfOffsetLen = offsetLen / 2
	}
	// halfOffsetLen should be a multiple of 29
	// if halfOffsetLen%29 != 0 {
	//	halfOffsetLen = halfOffsetLen - (halfOffsetLen % 29)
	// }

	startReadingAt := offset.start + halfOffsetLen - 29
	if startReadingAt < globalOffset.start {
		startReadingAt = globalOffset.start
	}
	endReadingAt := startReadingAt + 29*2

	if endReadingAt > globalOffset.end {
		endReadingAt = globalOffset.end
	}

	patternOffset := nodeOffset{startReadingAt, endReadingAt}
	patternReader, err := s.newNodeReader(patternOffset)
	if err != nil {
		return nil, jumps, err
	}
	buffer := make([]byte, 29*2)
	read, err := patternReader.Read(buffer)
	if err != nil {
		return nil, jumps, err
	}

	// find the first 9 bytes in the buffer that match the pattern
	found := false
	isTomstone := false
	for i := 0; i < read-9; i++ {
		if compareByteArrays(buffer[i:i+9], TOMBSTONE_PATTERN) || compareByteArrays(buffer[i:i+9], NON_TOMBSTONE_PATTERN) {
			found = true
			isTomstone = buffer[i] == TOMBSTONE_PATTERN[0]
			halfOffsetLen = startReadingAt + uint64(i) - offset.start
			break
			/*
				if endReadingAt == globalOffset.end {
					break
				}
				jumpOffset := uint64(29) + 1
				if isTomstone {
					jumpOffset = 21 + 1
				}

				if !(compareByteArrays(buffer[jumpOffset:jumpOffset+9], TOMBSTONE_PATTERN) || compareByteArrays(buffer[jumpOffset:jumpOffset+9], NON_TOMBSTONE_PATTERN)) {
					found = false
				} else {
					break
				}
			*/
		}
	}

	if !found {
		return nil, jumps, nil
	}

	docIdOffsetStart := halfOffsetLen + 11
	// read docId at halfOffsetLen
	docIdOffset := nodeOffset{offset.start + docIdOffsetStart, offset.start + docIdOffsetStart + 8}
	docIdReader, err := s.newNodeReader(docIdOffset)
	if err != nil {
		return nil, jumps, err
	}
	var docIdFound uint64
	if err := binary.Read(docIdReader, binary.BigEndian, &docIdFound); err != nil {
		return nil, jumps, errors.Wrap(err, "read value length encoding")
	}
	fmt.Println("jumped to", docIdFound)
	// if docIdFound is smaller than docId, jump to the right
	jumpOffset := uint64(29)
	if isTomstone {
		jumpOffset = 21
	}

	if docIdFound < docId {
		offset.start += halfOffsetLen + jumpOffset
		return s.jumpToDocIdUnaligned(node, key, docId, offset, globalOffset, jumps+1)
	}
	// if docIdFound is bigger than docId, jump to the left
	if docIdFound > docId {
		offset.end -= halfOffsetLen + jumpOffset
		return s.jumpToDocIdUnaligned(node, key, docId, offset, globalOffset, jumps+1)
	}

	// if docIdFound is equal to docId, return offset
	return &nodeOffset{offset.start + halfOffsetLen, offset.start + halfOffsetLen + 29}, jumps, nil
}

func (b *Bucket) GetSegments() []*segment {
	return b.disk.segments
}

func (s *segment) GetTermTombstoneNonTombstone(key []byte) (uint64, uint64, uint64, uint64, error) {
	if s.Closing {
		return 0, 0, 0, 0, fmt.Errorf("segment is closing")
	}
	if true { // s.mmapContents
		node, err := s.index.Get(key)
		if err != nil {
			return 0, 0, 0, 0, err
		}
		count := binary.LittleEndian.Uint64(s.contents[node.Start : node.Start+8])
		if s.strategy == segmentindex.StrategyInverted {
			return count, 0, node.Start, node.End, nil
		}
		byteSize := node.End - node.Start - uint64(8+4+len(key))
		nonTombstones := (byteSize - (21 * count)) / 8
		tombstones := count - nonTombstones
		start := node.Start
		end := node.End
		return nonTombstones, tombstones, start, end, nil
	} else {
		var err error
		node, err := s.index.Get(key)
		if err != nil {
			return 0, 0, 0, 0, err
		}

		values, err := s.getCollection(key)
		if err != nil {
			return 0, 0, 0, 0, err
		}

		docCount := uint64(len(values))
		nonTombstoneCount := uint64(0)
		for _, value := range values {
			if !value.tombstone {
				nonTombstoneCount++
			}
		}
		tombstoneCount := docCount - nonTombstoneCount
		return nonTombstoneCount, tombstoneCount, node.Start, node.End, nil
	}
}

type SegmentTokenization struct {
	Segment      *segment
	Tokenization string
}

func (store *Store) GetSegmentsForTerms(propNamesByTokenization map[string][]string, queryTermsByTokenization map[string][]string) (map[string][]SegmentTokenization, error) {
	segments := make(map[string][]SegmentTokenization, 100)
	for tokenization, propNameTokens := range propNamesByTokenization {
		for _, propName := range propNameTokens {

			bucket := store.Bucket(helpers.BucketSearchableFromPropNameLSM(propName))
			if bucket == nil {
				return nil, fmt.Errorf("could not find bucket for property %v", propName)
			}
			bucketSegments := bucket.GetSegments()
			for _, currentSegment := range bucketSegments {
				hasTerm := false
				if currentSegment == nil || currentSegment.Closing {
					// if there is a segment that is closing, we should wait for the compaction to finish
					currentSegment.CompactionMutex.RLock()
					defer currentSegment.CompactionMutex.RUnlock()
					return store.GetSegmentsForTerms(propNamesByTokenization, queryTermsByTokenization)
				}
				for _, term := range queryTermsByTokenization[tokenization] {
					_, err := currentSegment.index.Get([]byte(term))
					// segment does not contain term
					if err != nil {
						continue
					}
					hasTerm = true
				}
				if hasTerm {
					if _, ok := segments[propName]; !ok {
						segments[propName] = make([]SegmentTokenization, 0, len(bucketSegments))
					}
					segments[propName] = append(segments[propName], SegmentTokenization{Segment: currentSegment, Tokenization: tokenization})
				}
			}
		}
	}
	return segments, nil
}

type MemtableTokenization struct {
	Memtable     *Memtable
	Tokenization string
}

func (store *Store) GetMemtablesForTerms(propNamesByTokenization map[string][]string, queryTermsByTokenization map[string][]string) (map[string][]MemtableTokenization, error) {
	memtables := make(map[string][]MemtableTokenization, len(propNamesByTokenization)*len(queryTermsByTokenization))
	for tokenization, propNameTokens := range propNamesByTokenization {
		for _, propName := range propNameTokens {
			bucket := store.Bucket(helpers.BucketSearchableFromPropNameLSM(propName))
			bucketMemtables := []*Memtable{}
			if bucket.flushing == nil {
				bucketMemtables = append(bucketMemtables, bucket.active)
			} else {
				bucketMemtables = append(bucketMemtables, bucket.flushing, bucket.active)
			}
			for _, memtable := range bucketMemtables {
				if memtable == nil {
					continue
				}
				hasTerm := false
				for _, term := range queryTermsByTokenization[tokenization] {
					_, err := memtable.getMap([]byte(term))
					if err != nil {
						continue
					}
					hasTerm = true
				}
				if hasTerm {
					if _, ok := memtables[propName]; !ok {
						memtables[propName] = make([]MemtableTokenization, 0, len(bucketMemtables))
					}
					memtables[propName] = append(memtables[propName], MemtableTokenization{Memtable: memtable, Tokenization: tokenization})
				}
			}
		}
	}
	return memtables, nil
}

func (store *Store) GetAllSegmentStats(segments map[string][]SegmentTokenization, memTables map[string][]MemtableTokenization, propNamesByTokenization map[string][]string, queryTermsByTokenization map[string][]string) (map[string]map[string]int64, bool, error) {
	propertySizes := make(map[string]map[string]int64, len(propNamesByTokenization)*len(queryTermsByTokenization))
	hasTombstones := false
	for propName := range segments {
		for _, segmentAndTokenization := range segments[propName] {
			segment := segmentAndTokenization.Segment
			tokenization := segmentAndTokenization.Tokenization
			propertySizes[propName] = make(map[string]int64, len(queryTermsByTokenization[tokenization]))
			for _, term := range queryTermsByTokenization[tokenization] {
				nonTombstones, tombstones, _, _, err := segment.GetTermTombstoneNonTombstone([]byte(term))
				// segment does not contain term
				if err != nil {
					continue
				}
				propertySizes[propName][term] += int64(nonTombstones)
				propertySizes[propName][term] -= int64(tombstones)
				if tombstones > 0 {
					hasTombstones = true
				}
			}
		}
	}

	for propName := range memTables {
		for _, memtableAndTokenization := range memTables[propName] {
			memTable := memtableAndTokenization.Memtable
			tokenization := memtableAndTokenization.Tokenization
			if _, ok := propertySizes[propName]; !ok {
				propertySizes[propName] = make(map[string]int64, len(queryTermsByTokenization[tokenization]))
			}
			if memTable != nil && memTable.Size() > 0 {
				for _, term := range queryTermsByTokenization[tokenization] {
					termBytes := []byte(term)
					memTable.RLock()
					_, err := memTable.getMap(termBytes)
					if err != nil {
						memTable.RUnlock()
						continue
					}

					tombstone, nonTombstone, err := memTable.countTombstones(termBytes)
					if err != nil {
						memTable.RUnlock()
						continue
					}

					propertySizes[propName][term] += int64(nonTombstone)
					propertySizes[propName][term] -= int64(tombstone)
					memTable.RUnlock()
				}
			}
		}
	}

	return propertySizes, hasTombstones, nil
}
