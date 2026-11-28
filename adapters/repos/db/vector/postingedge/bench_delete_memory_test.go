//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build postingedgebench

package postingedge

// memoryCell is what a cell of v6.2 (bench-report.md, "v6.2: reparación en el
// churn") adds to a cell of the deletes: its maintenance passes, one with the
// deletes at once and one a round in the churn, what they cost in all, and
// the edge recall of the sample of its stage when they end.
type memoryCell struct {
	// Deleted is the deleted vectors the costs are divided by.
	Deleted int          `json:"deleted"`
	Passes  []memoryPass `json:"passes"`
	// Rounds are the rounds of the churn, one per pass.
	Rounds []deleteRound `json:"rounds,omitempty"`
	// Repaired counts the lists the first phases built again by search,
	// SearchLoads the postings those searches loaded, SearchRescored the
	// candidates they compared at full precision and InverseDistances the
	// exact distances of the inverse edges of the lists they built.
	Repaired         int `json:"vectorsRepaired"`
	SearchLoads      int `json:"searchLoads"`
	SearchRescored   int `json:"searchRescored"`
	InverseDistances int `json:"inverseDistances"`
	// SecondPhasePostings counts the postings the second phases read and
	// wrote, and Merges those they merged into another.
	SecondPhasePostings int `json:"postingsOfSecondPhase"`
	Merges              int `json:"merges"`
	// Substituted and Dropped count the edges to deleted vectors that were
	// given a substitute and those taken out with none. CodeDistances
	// counts the distances between two codes of the substitution, and
	// RehearsalCodeDistances those of the rehearsals of cell C.
	Substituted            int `json:"edgesSubstituted"`
	Dropped                int `json:"edgesDropped"`
	CodeDistances          int `json:"codeDistances"`
	RehearsalCodeDistances int `json:"rehearsalCodeDistances"`
	// Edges is the edge recall of the sample when the cell ends.
	Edges *memoryEdges `json:"edgeRecall,omitempty"`
}

func (c *memoryCell) add(pass memoryPass) {
	c.Passes = append(c.Passes, pass)
	c.Repaired += pass.Repair.Refreshed
	c.SearchLoads += pass.Repair.Work.SearchLoads
	c.SearchRescored += pass.Repair.Work.SearchRescored
	c.InverseDistances += pass.Repair.Work.InverseDistances
	c.SecondPhasePostings += pass.SecondPhasePostings
	c.Merges += pass.Merges
	c.Substituted += pass.Substitution.Substituted
	c.Dropped += pass.Dropped
	c.CodeDistances += pass.Substitution.CodeDistances
	c.RehearsalCodeDistances += pass.Rehearsal.CodeDistances
}
