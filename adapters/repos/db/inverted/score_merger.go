package inverted

import "fmt"

type scoreMerger struct {
	lists       []docPointersWithScore
	listOffsets []int
	result      docPointersWithScore
}

// newScoreMerge must be initalized with already sorted lists, as the merge
// strategy will not work otherwise
func newScoreMerger(lists []docPointersWithScore) *scoreMerger {
	maxLength := 0
	for _, list := range lists {
		// TODO: this might reserve more memory than we actually need since there
		// will probably be overlap. It might be worth profiling if it's better to
		// reserve all and discard some or start with e.g. half and increase if
		// full. As of the time of buidling this, no such measurements exist.
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
	for {
		list, id, score, ok := m.currentMin()
		if !ok {
			break
		}

		m.listOffsets[list]++

		fmt.Printf("list %d with doc id %d and score %f\n", list, id, score)

	}
	m.result.docIDs = m.result.docIDs[:0]
	return m.result
}

// returns listIndex, docIDValue
func (m *scoreMerger) currentMin() (int, uint64, float64, bool) {
	minlistIndex := -1
	maxScore := float64(0)
	currID := uint64(0)
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
			maxScore = candidateValue
			currID = candidateID
			continue
		}

		if candidateValue > maxScore {
			minlistIndex = listIndex
			maxScore = candidateValue
			currID = candidateID
		}
	}

	return minlistIndex, currID, maxScore, found
}
