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

package postingedge

import "time"

// BuildStats describes one build: how long each stage took and what it
// produced.
type BuildStats struct {
	Vectors  int
	Postings int
	Edges    int
	// CandidatesScored is the number of neighbor candidates the edge builder
	// ranked by code, its dominant cost.
	CandidatesScored int64

	PartitionTime     time.Duration // clustering into postings
	AssignTime        time.Duration // exact nearest-centroid assignment
	EncodeTime        time.Duration // quantizer training and encoding
	EdgeTime          time.Duration // edge construction
	CentroidGraphTime time.Duration // graph over the centroids
	TotalTime         time.Duration
}

// MemoryStats is the memory the built index holds, by component. The numbers
// are exact for the arrays the index owns. The graph over the centroids is
// owned by the hnsw package and is not included, only the centroid vectors it
// indexes are. Full-precision vectors are not held by the index at all: a
// search reads them through Config.TempVectorForIDWithViewThunk.
type MemoryStats struct {
	Vectors  int
	Postings int
	Edges    int

	// PostingBytes covers the doc ids of the posting members, the posting
	// offsets and the slot -> posting directory.
	PostingBytes int64
	// CodeBytes covers the centered 1-bit RQ codes, held once and served to
	// both the posting scans and the sidecar lookups.
	CodeBytes int64
	// EdgeBytes covers the edge endpoints and the per-vector edge offsets.
	EdgeBytes int64
	// CentroidVectorBytes covers the full-precision centroid vectors.
	CentroidVectorBytes int64
}

// Total is the sum of all components.
func (m MemoryStats) Total() int64 {
	return m.PostingBytes + m.CodeBytes + m.EdgeBytes + m.CentroidVectorBytes
}

// SearchStats describes the work of one search.
type SearchStats struct {
	// Rounds is the number of load rounds after the entry postings. With
	// postings on disk every round is one batch of parallel reads, so
	// 1+Rounds is the serial I/O depth of the search.
	Rounds int
	// PostingsLoaded counts the entry postings and the ones loaded by rounds.
	PostingsLoaded int
	// VectorsScanned is the number of posting members scored by code.
	VectorsScanned int
	// ScannedAgain is the number of those that had already been scored as an
	// edge endpoint before their posting was loaded. Such an estimate is
	// only folded into the rank of the posting, not kept, so the scan
	// computes it a second time.
	ScannedAgain int
	// EdgesFollowed is the number of edges leaving the loaded postings. Each
	// one either leads into a posting that is already loaded, or to an
	// endpoint that was scored before, or is scored: see EndpointsInLoaded,
	// EndpointsRepeated and EndpointsScored.
	EdgesFollowed int
	// EndpointsInLoaded is the number of followed edges whose endpoint lives
	// in a posting that was already loaded. They cost a directory lookup.
	EndpointsInLoaded int
	// EndpointsScored is the number of edge endpoints scored by sidecar code:
	// the distinct endpoints outside the loaded postings.
	EndpointsScored int
	// Rescored is the number of candidates compared at full precision.
	Rescored int

	CentroidTime time.Duration // finding the entry postings
	ScanTime     time.Duration // scoring posting members
	ExpandTime   time.Duration // scoring edge endpoints and ranking postings
	SelectTime   time.Duration // picking the postings of each round
	RescoreTime  time.Duration // full-precision rescoring
}

// EndpointsRepeated is the number of followed edges whose endpoint had
// already been scored through another edge. They cost a visited-set lookup.
func (s SearchStats) EndpointsRepeated() int {
	return s.EdgesFollowed - s.EndpointsInLoaded - s.EndpointsScored
}
