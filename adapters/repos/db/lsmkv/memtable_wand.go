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
	"math"
	"sort"

	"github.com/pkg/errors"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/schema"
)

func (m *Memtable) countTombstones(key []byte) (int, int, error) {
	tombstones := 0
	nonTombstones := 0
	if m.strategy != StrategyMapCollection {
		return 0, 0, errors.Errorf("getCollection only possible with strategy %q",
			StrategyMapCollection)
	}

	m.RLock()
	defer m.RUnlock()

	values, err := m.keyMap.get(key)
	if err != nil {
		return 0, 0, err
	}

	for _, v := range values {
		if v.Tombstone {
			tombstones++
		} else {
			nonTombstones++
		}
	}

	return tombstones, nonTombstones, nil
}

func (m *Memtable) CreateTerm(N float64, n float64, filterDocIds helpers.AllowList, query string,
	propName string, propertyBoosts map[string]float32, duplicateTextBoost int,
	additionalExplanations bool,
) (*TermMem, map[uint64]int, error) {
	termResult := &TermMem{queryTerm: query}
	filteredDocIDs := sroar.NewBitmap() // to build the global n if there is a filter

	allMsAndProps := make(AllMapPairsAndPropName, 0, 1)

	preM, err := m.keyMap.get([]byte(query))
	if err != nil {
		return termResult, nil, err
	}

	var postM []MapPair
	if filterDocIds != nil {
		postM := make([]MapPair, 0, len(preM))
		for _, val := range preM {
			docID := binary.BigEndian.Uint64(val.Key)
			if filterDocIds.Contains(docID) {
				postM = append(postM, val)
			} else {
				filteredDocIDs.Set(docID)
			}
		}
	} else {
		postM = preM
	}

	allMsAndProps = append(allMsAndProps, MapPairsAndPropName{MapPairs: postM, propname: propName})

	// sort ascending, this code has two effects
	// 1) We can skip writing the indices from the last property to the map (see next comment). Therefore, having the
	//    biggest property at the end will save us most writes on average
	// 2) For the first property all entries are new, and we can create the map with the respective size. When choosing
	//    the second-biggest entry as the first property we save additional allocations later
	sort.Sort(allMsAndProps)
	if len(allMsAndProps) > 2 {
		allMsAndProps[len(allMsAndProps)-2], allMsAndProps[0] = allMsAndProps[0], allMsAndProps[len(allMsAndProps)-2]
	}

	var docMapPairs []docPointerWithScore = nil
	var docMapPairsIndices map[uint64]int = nil
	for i, mAndProps := range allMsAndProps {
		m := mAndProps.MapPairs
		propName := mAndProps.propname

		// The indices are needed for two things:
		// a) combining the results of different properties
		// b) Retrieve additional information that helps to understand the results when debugging. The retrieval is done
		//    in a later step, after it is clear which objects are the most relevant
		//
		// When b) is not needed the results from the last property do not need to be added to the index-map as there
		// won't be any follow-up combinations.
		includeIndicesForLastElement := false
		if additionalExplanations || i < len(allMsAndProps)-1 {
			includeIndicesForLastElement = true
		}

		// only create maps/slices if we know how many entries there are
		if docMapPairs == nil {
			docMapPairs = make([]docPointerWithScore, 0, len(m))
			docMapPairsIndices = make(map[uint64]int, len(m))
			for k, val := range m {
				if len(val.Value) < 8 {
					// logger.Warnf("Skipping pair in BM25: MapPair.Value should be 8 bytes long, but is %d.", len(val.Value))
					continue
				}
				freqBits := binary.LittleEndian.Uint32(val.Value[0:4])
				propLenBits := binary.LittleEndian.Uint32(val.Value[4:8])
				docMapPairs = append(docMapPairs,
					docPointerWithScore{
						Id:         binary.BigEndian.Uint64(val.Key),
						Frequency:  math.Float32frombits(freqBits) * propertyBoosts[propName],
						PropLength: math.Float32frombits(propLenBits),
					})
				if includeIndicesForLastElement {
					docMapPairsIndices[binary.BigEndian.Uint64(val.Key)] = k
				}
			}
		} else {
			for _, val := range m {
				if len(val.Value) < 8 {
					// b.logger.Warnf("Skipping pair in BM25: MapPair.Value should be 8 bytes long, but is %d.", len(val.Value))
					continue
				}
				key := binary.BigEndian.Uint64(val.Key)
				ind, ok := docMapPairsIndices[key]
				freqBits := binary.LittleEndian.Uint32(val.Value[0:4])
				propLenBits := binary.LittleEndian.Uint32(val.Value[4:8])
				if ok {
					if ind >= len(docMapPairs) {
						// the index is not valid anymore, but the key is still in the map
						// b.logger.Warnf("Skipping pair in BM25: Index %d is out of range for key %d, length %d.", ind, key, len(docMapPairs))
						continue
					}
					if ind < len(docMapPairs) && docMapPairs[ind].Id != key {
						// b.logger.Warnf("Skipping pair in BM25: id at %d in doc map pairs, %d, differs from current key, %d", ind, docMapPairs[ind].id, key)
						continue
					}

					docMapPairs[ind].PropLength += math.Float32frombits(propLenBits)
					docMapPairs[ind].Frequency += math.Float32frombits(freqBits) * propertyBoosts[propName]
				} else {
					docMapPairs = append(docMapPairs,
						docPointerWithScore{
							Frequency:  math.Float32frombits(freqBits) * propertyBoosts[propName],
							PropLength: math.Float32frombits(propLenBits),
						})
					if includeIndicesForLastElement {
						docMapPairsIndices[binary.BigEndian.Uint64(val.Key)] = len(docMapPairs) - 1 // current last entry
					}
				}
			}
		}
	}
	if docMapPairs == nil {
		termResult.exhausted = true
		return termResult, docMapPairsIndices, nil
	}
	termResult.data = docMapPairs

	termResult.idf = math.Log(float64(1)+(N-float64(n)+0.5)/(float64(n)+0.5)) * float64(duplicateTextBoost)

	// catch special case where there are no results and would panic termResult.data[0].id
	// related to #4125
	if len(termResult.data) == 0 {
		termResult.posPointer = 0
		termResult.idPointer = 0
		termResult.exhausted = true
		return termResult, docMapPairsIndices, nil
	}

	termResult.posPointer = 0
	termResult.idPointer = termResult.data[0].Id
	return termResult, docMapPairsIndices, nil
}

type TermMem struct {
	// doubles as max impact (with tf=1, the max impact would be 1*idf), if there
	// is a boost for a queryTerm, simply apply it here once
	idf float64

	idPointer  uint64
	posPointer uint64
	data       []docPointerWithScore
	exhausted  bool
	queryTerm  string
}
type MapPairsAndPropName struct {
	propname string
	MapPairs []MapPair
}

type AllMapPairsAndPropName []MapPairsAndPropName

// provide sort interface
func (m AllMapPairsAndPropName) Len() int {
	return len(m)
}

func (m AllMapPairsAndPropName) Less(i, j int) bool {
	return len(m[i].MapPairs) < len(m[j].MapPairs)
}

func (m AllMapPairsAndPropName) Swap(i, j int) {
	m[i], m[j] = m[j], m[i]
}

func (t *TermMem) ScoreAndAdvance(averagePropLength float64, config schema.BM25Config) (uint64, float64) {
	id := t.idPointer
	pair := t.data[t.posPointer]
	freq := float64(pair.Frequency)
	tf := freq / (freq + config.K1*(1-config.B+config.B*float64(pair.PropLength)/averagePropLength))

	// advance
	t.posPointer++
	if t.posPointer >= uint64(len(t.data)) {
		t.exhausted = true
	} else {
		t.idPointer = t.data[t.posPointer].Id
	}

	return id, tf * t.idf
}

func (t *TermMem) AdvanceAtLeast(minID uint64) {
	for t.idPointer < minID {
		t.posPointer++
		if t.posPointer >= uint64(len(t.data)) {
			t.exhausted = true
			return
		}
		t.idPointer = t.data[t.posPointer].Id
	}
}

func (t *TermMem) IsExhausted() bool {
	return t.exhausted
}

func (t *TermMem) IdPointer() uint64 {
	return t.idPointer
}

func (t *TermMem) QueryTerm() string {
	return t.queryTerm
}

func (t *TermMem) IDF() float64 {
	return t.idf
}

func (t *TermMem) Data() []docPointerWithScore {
	return t.data
}
