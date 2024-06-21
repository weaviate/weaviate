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
	"github.com/weaviate/weaviate/entities/models"
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

func (s segment) WandTerm(key []byte, N float64, duplicateTextBoost float64, propertyBoost float64, fullPropertyLen int64, filterDocIds helpers.AllowList) (terms.Term, error) {
	node, err := s.index.Get(key)
	if err != nil {
		return nil, err
	}
	if s.strategy != segmentindex.StrategyInverted {
		return nil, fmt.Errorf("invalid or unsupported unit")
	}

	term := TermInverted{}
	term.init(N, duplicateTextBoost, s, node.Start, node.End, key, string(key), propertyBoost, fullPropertyLen, filterDocIds)
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

func (store *Store) GetSegmentsForTerms(propNamesByTokenization map[string][]string, queryTermsByTokenization map[string][]string) (map[*segment][]string, error) {
	segments := make(map[*segment][]string)
	for _, propNameTokens := range propNamesByTokenization {
		for _, propName := range propNameTokens {

			bucket := store.Bucket(helpers.BucketSearchableFromPropNameLSM(propName))
			if bucket == nil {
				return nil, fmt.Errorf("could not find bucket for property %v", propName)
			}
			bucketSegments := bucket.GetSegments()
			for _, segment := range bucketSegments {
				hasTerm := false
				if segment == nil || segment.Closing {
					segment.CompactionMutex.RLock()
					return store.GetSegmentsForTerms(propNamesByTokenization, queryTermsByTokenization)
				}
				for _, term := range queryTermsByTokenization[models.PropertyTokenizationWord] {
					_, err := segment.index.Get([]byte(term))
					// segment does not contain term
					if err != nil {
						continue
					}
					hasTerm = true
				}
				if hasTerm {
					if _, ok := segments[segment]; !ok {
						segments[segment] = []string{}
					}
					segments[segment] = append(segments[segment], propName)
				}
			}
		}
	}
	return segments, nil
}

func (store *Store) GetMemtablesForTerms(propNamesByTokenization map[string][]string, queryTermsByTokenization map[string][]string) (map[*Memtable][]string, error) {
	memtables := make(map[*Memtable][]string)
	for _, propNameTokens := range propNamesByTokenization {
		for _, propName := range propNameTokens {
			bucket := store.Bucket(helpers.BucketSearchableFromPropNameLSM(propName))
			bucketMemtables := []*Memtable{bucket.active, bucket.flushing}
			for _, memtable := range bucketMemtables {
				if memtable == nil {
					continue
				}
				hasTerm := false
				for _, term := range queryTermsByTokenization[models.PropertyTokenizationWord] {
					_, err := memtable.getMap([]byte(term))
					if err != nil {
						continue
					}
					hasTerm = true
				}
				if hasTerm {
					if _, ok := memtables[memtable]; !ok {
						memtables[memtable] = []string{}
					}
					memtables[memtable] = append(memtables[memtable], propName)
				}
			}
		}
	}
	return memtables, nil
}

func (store *Store) GetAllSegmentStats(segments map[*segment][]string, memTables map[*Memtable][]string, propNamesByTokenization map[string][]string, queryTermsByTokenization map[string][]string) (map[string]map[string]int64, bool, error) {
	propertySizes := make(map[string]map[string]int64, 100)
	hasTombstones := false
	for segment, propNames := range segments {
		for _, propName := range propNames {
			propertySizes[propName] = make(map[string]int64, len(queryTermsByTokenization[models.PropertyTokenizationWord]))
			for _, term := range queryTermsByTokenization[models.PropertyTokenizationWord] {
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

	for memtable, propNames := range memTables {
		for _, propName := range propNames {
			if _, ok := propertySizes[propName]; !ok {
				propertySizes[propName] = make(map[string]int64, len(queryTermsByTokenization[models.PropertyTokenizationWord]))
			}
			if memtable != nil && memtable.Size() > 0 {
				for _, term := range queryTermsByTokenization[models.PropertyTokenizationWord] {
					termBytes := []byte(term)
					memtable.RLock()
					_, err := memtable.getMap(termBytes)
					if err != nil {
						memtable.RUnlock()
						continue
					}

					tombstone, nonTombstone, err := memtable.countTombstones(termBytes)
					if err != nil {
						memtable.RUnlock()
						continue
					}

					propertySizes[propName][term] += int64(nonTombstone)
					propertySizes[propName][term] -= int64(tombstone)
					memtable.RUnlock()
				}
			}
		}
	}

	return propertySizes, hasTombstones, nil
}
