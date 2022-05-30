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

package inverted

type scoreMerger struct {
	lists       []docPointersWithScore
	listOffsets []int
	result      docPointersWithScore
}

// newScoreMerge must be initialized with already sorted lists, as the merge
// strategy will not work otherwise
func newScoreMerger(lists []docPointersWithScore) *scoreMerger {
	maxLength := 0
	for _, list := range lists {
		// TODO: this might reserve more memory than we actually need since there
		// will probably be overlap. It might be worth profiling if it's better to
		// reserve all and discard some or start with e.g. half and increase if
		// full. As of the time of building this, no such measurements exist.
		maxLength += len(list.docIDs)
	}

	return &scoreMerger{
		lists:       lists,
		listOffsets: make([]int, len(lists)), // all initialized to 0 which is the correct offset to start with
		result: docPointersWithScore{
			docIDs: make([]docPointerWithScore, maxLength),
		},
	}
}

func (m *scoreMerger) do() docPointersWithScore {
	outputOffset := -1
	lastID := uint64(0)
	for {
		list, id, score, ok := m.currentMin()
		if !ok {
			break
		}

		m.listOffsets[list]++

		if outputOffset > -1 && id == lastID {
			// merge into existing entry
			m.result.docIDs[outputOffset].score = m.result.docIDs[outputOffset].score + score
			// TODO: we currently assume scores will always be added, should there
			// also be other mechanisms, such as multiplication?
		} else {
			// make new entry
			outputOffset++
			m.result.docIDs[outputOffset] = docPointerWithScore{
				id:    id,
				score: score,
			}
		}

		lastID = id

	}

	m.result.docIDs = m.result.docIDs[:outputOffset+1]
	m.result.count = uint64(len(m.result.docIDs))
	return m.result
}

// returns listIndex, docIDValue
func (m *scoreMerger) currentMin() (int, uint64, float64, bool) {
	minlistIndex := -1
	currScore := float64(0)
	minID := uint64(0)
	found := false
	for listIndex := range m.lists {
		if m.listOffsets[listIndex] >= len(m.lists[listIndex].docIDs) {
			continue
		}

		found = true
		candidateValue := m.lists[listIndex].docIDs[m.listOffsets[listIndex]].score
		candidateID := m.lists[listIndex].docIDs[m.listOffsets[listIndex]].id
		if minlistIndex == -1 {
			minlistIndex = listIndex
			currScore = candidateValue
			minID = candidateID
			continue
		}

		if candidateID < minID {
			minlistIndex = listIndex
			currScore = candidateValue
			minID = candidateID
		}
	}

	return minlistIndex, minID, currScore, found
}
