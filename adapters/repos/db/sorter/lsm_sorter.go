//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package sorter

import (
	"bytes"
	"context"
	"encoding/binary"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/storobj"
)

type docIDAndValue struct {
	docID uint64
	dist  float32
	value interface{}
}

type lsmSorter struct {
	store             *lsmkv.Store
	className         schema.ClassName
	classHelper       *classHelper
	property, order   string
	sorter            *sortBy
	propertyExtractor *lsmPropertyExtractor
}

func newLSMStoreSorter(store *lsmkv.Store, schema schema.Schema, className schema.ClassName, property, order string) *lsmSorter {
	return &lsmSorter{
		store:             store,
		className:         className,
		classHelper:       newClassHelper(schema),
		property:          property,
		order:             order,
		sorter:            newSortBy(newComparator(order)),
		propertyExtractor: newPropertyExtractor(className, newClassHelper(schema), property),
	}
}

func (s *lsmSorter) sort(ctx context.Context, limit int, additional additional.Properties) ([]uint64, error) {
	i := 0
	cursor := s.store.Bucket(helpers.ObjectsBucketLSM).Cursor()
	defer cursor.Close()

	candidates := make([]docIDAndValue, 0, s.getLimit(limit))

	for k, v := cursor.First(); k != nil && i < limit; k, v = cursor.Next() {
		docID, err := storobj.DocIDFromBinary(v)
		if err != nil {
			return nil, errors.Wrapf(err, "unmarhsal doc id of item %d", i)
		}

		value := s.getPropertyValue(v, s.property)

		if s.currentIsBetterThanWorstElement(candidates, limit, value, s.order) {
			candidates = s.addToCandidates(candidates, limit, docIDAndValue{docID: docID, value: value}, s.order)
		}
	}

	docIDs := s.toDocIDs(candidates)

	return docIDs, nil
}

func (s *lsmSorter) sortDocIDs(ctx context.Context, limit int, additional additional.Properties, ids []uint64) ([]uint64, error) {
	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)
	if bucket == nil {
		return nil, errors.Errorf("objects bucket not found")
	}

	candidates := make([]docIDAndValue, 0, s.getLimit(limit))

	for _, id := range ids {
		keyBuf := bytes.NewBuffer(nil)
		binary.Write(keyBuf, binary.LittleEndian, &id)
		docIDBytes := keyBuf.Bytes()
		res, err := bucket.GetBySecondary(0, docIDBytes)
		if err != nil {
			return nil, err
		}

		if res == nil {
			continue
		}

		value := s.getPropertyValue(res, s.property)

		if s.currentIsBetterThanWorstElement(candidates, limit, value, s.order) {
			candidates = s.addToCandidates(candidates, limit, docIDAndValue{docID: id, value: value}, s.order)
		}
	}

	docIDs := s.toDocIDs(candidates)

	return docIDs, nil
}

func (s *lsmSorter) sortDocIDsAndDists(ctx context.Context, limit int, additional additional.Properties, ids []uint64, dists []float32) ([]uint64, []float32, error) {
	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)
	if bucket == nil {
		return nil, nil, errors.Errorf("objects bucket not found")
	}

	candidates := make([]docIDAndValue, 0, s.getLimit(limit))

	for i := range ids {
		keyBuf := bytes.NewBuffer(nil)
		binary.Write(keyBuf, binary.LittleEndian, &ids[i])
		docIDBytes := keyBuf.Bytes()
		res, err := bucket.GetBySecondary(0, docIDBytes)
		if err != nil {
			return nil, nil, err
		}

		if res == nil {
			continue
		}

		value := s.getPropertyValue(res, s.property)

		if s.currentIsBetterThanWorstElement(candidates, limit, value, s.order) {
			candidates = s.addToCandidates(candidates, limit, docIDAndValue{docID: ids[i], dist: dists[i], value: value}, s.order)
		}
	}

	docIDs := s.toDocIDs(candidates)
	distances := s.toDists(candidates)

	return docIDs, distances, nil
}

func (s *lsmSorter) getLimit(limit int) int {
	if limit < 0 {
		return 0
	}
	return limit
}

func (s *lsmSorter) toDocIDs(candidates []docIDAndValue) []uint64 {
	docIDs := make([]uint64, len(candidates))
	for i := range candidates {
		docIDs[i] = candidates[i].docID
	}
	return docIDs
}

func (s *lsmSorter) toDists(candidates []docIDAndValue) []float32 {
	dists := make([]float32, len(candidates))
	for i := range candidates {
		dists[i] = candidates[i].dist
	}
	return dists
}

func (s *lsmSorter) currentIsBetterThanWorstElement(candidates []docIDAndValue,
	limit int, curr interface{}, order string,
) bool {
	if s.getLimit(limit) == 0 || len(candidates) < limit {
		return true
	}
	target := candidates[len(candidates)-1].value
	return s.compareBetterWorse(curr, target)
}

func (s *lsmSorter) addToCandidates(candidates []docIDAndValue,
	limit int, newEntry docIDAndValue, order string,
) []docIDAndValue {
	if len(candidates) == 0 {
		return []docIDAndValue{newEntry}
	}

	pos := -1
	for i := range candidates {
		if s.compareCandidates(newEntry, candidates[i]) {
			pos = i
			break
		}
	}

	if pos == -1 {
		candidates = append(candidates, newEntry)
	} else {
		if s.getLimit(limit) == 0 || len(candidates) < limit {
			candidates = append(candidates[:pos+1], candidates[pos:]...)
		} else {
			candidates = append(candidates[:pos+1], candidates[pos:len(candidates)-1]...)
		}
		candidates[pos] = newEntry
	}

	return candidates
}

func (s *lsmSorter) compareBetterWorse(curr, target interface{}) bool {
	return s.compare(curr, target)
}

func (s *lsmSorter) compareCandidates(newEntry, cand docIDAndValue) bool {
	return s.compare(newEntry.value, cand.value)
}

func (s *lsmSorter) getPropertyValue(v []byte, property string) interface{} {
	return s.propertyExtractor.getProperty(v)
}

func (s *lsmSorter) compare(curr, target interface{}) bool {
	dataType := s.classHelper.getDataType(s.className.String(), s.property)
	if len(dataType) > 0 {
		return s.sorter.compare(curr, target, schema.DataType(dataType[0]))
	}
	return false
}
