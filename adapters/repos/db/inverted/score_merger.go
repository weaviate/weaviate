package inverted

type scoreMerger struct {
	lists       []docPointersWithScore
	listOffsets []int
	result      docPointersWithScore
}

func newScoreMerger(lists []docPointersWithScore) scoreMerger {
	maxLength := 0
	for _, list := range lists {
		// TODO: this might reserve more memory than we actually need since there
		// will probably be overlap. It might be worth profiling if it's better to
		// reserve all and discard some or start with e.g. half and increase if
		// full. As of the time of buidling this, no such measurements exist.
		maxLength += len(list.docIDs)
	}

	return scoreMerger{
		lists:       lists,
		listOffsets: make([]int, len(lists)), // all initialized to 0 which is the correct offset to start with
		result: docPointersWithScore{
			docIDs: make([]docPointerWithScore, maxLength),
		},
	}
}
