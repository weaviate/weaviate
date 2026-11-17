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

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// newStreamTestIndex builds an index over the first n vectors, with the doc
// ids 0..n-1 and a config that serves all of them, and hands it to a stream
// index over all the doc ids. With a count of the centroid graph's distances
// in cfg, the graph of the index counts into it from its build on.
func newStreamTestIndex(t *testing.T, all [][]float32, n int, uc UserConfig, cfg streamConfig) *streamIndex {
	t.Helper()
	ctx := context.Background()
	uc.Distance = "l2-squared"
	config := testConfig(t, all, 0, distancer.NewL2SquaredProvider())
	if cfg.centroidDistances != nil {
		config.centroidProvider = countingProvider{Provider: distancer.NewL2SquaredProvider(), count: cfg.centroidDistances}
	}
	index, err := New(config, uc)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, index.Shutdown(context.Background())) })
	require.NoError(t, index.AddBatch(ctx, sequentialIDs(n, 0), all[:n]))
	require.NoError(t, index.Build(ctx))
	s, err := newStreamIndex(index, len(all), cfg, func(id uint64) []float32 { return all[id] })
	require.NoError(t, err)
	return s
}

// edgesByDoc returns a copy of the edges of every live vector.
func edgesByDoc(s *streamIndex) map[uint64][]uint32 {
	edges := map[uint64][]uint32{}
	for _, l := range s.st.postings.lists {
		for i, docID := range l.docIDs {
			edges[docID] = slices.Clone(l.edgesOf(i))
		}
	}
	return edges
}

// The index searches the same after the conversion to doc id keys as before:
// the same postings, edges, codes and centroid graph, addressed otherwise.
func TestStreamConversionSearchesTheSame(t *testing.T) {
	const k = 10
	ctx := context.Background()
	all := clusteredVectors(2030, 24, 30, 811)
	vectors, queries := all[:2000], all[2000:]
	uc := smallConfig()
	uc.Distance = "l2-squared"
	index := buildTestIndex(t, vectors, distancer.NewL2SquaredProvider(), uc)

	type answer struct {
		ids   []uint64
		dists []float32
		stats SearchStats
	}
	collect := func() []answer {
		var answers []answer
		for _, ef := range []int{10, 40, 160} {
			uc.EF = ef
			require.NoError(t, index.UpdateUserConfig(uc, func() {}))
			for _, query := range queries {
				ids, dists, stats, err := index.SearchByVectorWithStats(ctx, query, k, nil)
				require.NoError(t, err)
				answers = append(answers, answer{ids: ids, dists: dists, stats: withoutTimes(stats)})
			}
		}
		return answers
	}
	before := collect()
	_, err := newStreamIndex(index, len(vectors), streamConfig{threshold: 80}, func(id uint64) []float32 { return vectors[id] })
	require.NoError(t, err)
	require.NotNil(t, index.state.Load().postings.lists)
	assert.Equal(t, before, collect())
}

// Every cell of the experiment keeps the coverage invariant through every
// insert and split, keeps the edges of every vector nearest first and outside
// nothing it should not reach, and does what it says: the principal cell
// leaves old edges alone, inverse edges keep the degree of old vectors, the
// repair searches the edges of split postings again, and the assignment-only
// cell gives the new vectors no edges.
func TestStreamInserts(t *testing.T) {
	const batch, total = 1200, 2400
	ctx := context.Background()
	all := clusteredVectors(total, 24, 30, 77)
	provider := distancer.NewL2SquaredProvider()

	tests := []struct {
		name string
		cfg  streamConfig
	}{
		{name: "principal", cfg: streamConfig{threshold: 60}},
		{name: "inverse edges", cfg: streamConfig{threshold: 60, inverse: true}},
		{name: "edge repair", cfg: streamConfig{threshold: 60, repair: true}},
		{name: "both", cfg: streamConfig{threshold: 60, inverse: true, repair: true}},
		{name: "sibling repair", cfg: streamConfig{threshold: 60, siblings: true}},
		{name: "inverse edges and sibling repair", cfg: streamConfig{threshold: 60, inverse: true, siblings: true}},
		{name: "assignment only", cfg: streamConfig{threshold: 60, assignOnly: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uc := smallConfig()
			s := newStreamTestIndex(t, all, batch, uc, tt.cfg)
			require.NoError(t, s.checkCoverage())
			uc.Distance, uc.EF = "l2-squared", 64
			require.NoError(t, s.ix.UpdateUserConfig(uc, func() {}))
			old := edgesByDoc(s)

			for id := uint64(batch); id < total; id++ {
				require.NoError(t, s.insert(ctx, id, all[id]))
				if id%97 == 0 {
					require.NoError(t, s.checkCoverage(), "after doc %d", id)
				}
			}
			require.NoError(t, s.checkCoverage())
			require.NoError(t, s.cleanCentroidGraph())
			require.NoError(t, s.checkCoverage(), "after the centroid graph was cleaned")

			assert.Equal(t, total, s.liveCount)
			assert.Equal(t, total-batch, s.stats.Inserted)
			assert.Positive(t, s.stats.Splits, "a threshold of 60 over postings of 40 splits")
			sum := 0
			for _, size := range s.postingSizes() {
				sum += size
			}
			assert.Equal(t, total, sum)
			require.Error(t, s.insert(ctx, 0, all[0]), "a doc goes in once")

			// Every edge leads out of its posting when it is made, nearest
			// first, and never to the vector itself.
			for _, l := range s.st.postings.lists {
				for i, docID := range l.docIDs {
					edges := l.edgesOf(i)
					require.LessOrEqual(t, len(edges), s.neighbors)
					require.NotContains(t, edges, uint32(docID))
					prev := float32(-1)
					for _, endpoint := range edges {
						dist, err := provider.SingleDist(all[docID], all[endpoint])
						require.NoError(t, err)
						require.GreaterOrEqual(t, dist, prev, "the edges of doc %d are nearest first", docID)
						prev = dist
					}
				}
			}

			now := edgesByDoc(s)
			switch {
			case tt.cfg.assignOnly:
				for id := uint64(batch); id < total; id++ {
					assert.Empty(t, now[id], "doc %d", id)
				}
				return
			case !tt.cfg.inverse && !tt.cfg.repair && !tt.cfg.siblings:
				for docID, edges := range old {
					require.True(t, slices.Equal(edges, now[docID]),
						"the principal cell leaves the edges of doc %d alone: %v, now %v", docID, edges, now[docID])
				}
			}
			if tt.cfg.inverse {
				assert.Positive(t, s.stats.InverseEdges)
				if !tt.cfg.repair && !tt.cfg.siblings {
					for docID, edges := range old {
						require.Len(t, now[docID], len(edges), "inverse edges keep the degree of doc %d", docID)
					}
				}
				// Every insert is counted once, by the inverse edges it wrote
				// and by the postings they went to.
				for _, hist := range [][]int{s.stats.InverseWrites, s.stats.InversePostings} {
					require.Len(t, hist, s.neighbors+1)
					inserts := 0
					for _, count := range hist {
						inserts += count
					}
					assert.Equal(t, s.stats.Inserted, inserts)
				}
				writes, postings := 0, 0
				for w, count := range s.stats.InverseWrites {
					writes += w * count
				}
				for w, count := range s.stats.InversePostings {
					postings += w * count
				}
				assert.Equal(t, s.stats.InverseEdges, writes)
				assert.LessOrEqual(t, postings, writes, "an insert writes no more postings than lists")
				assert.Positive(t, postings)
				assert.GreaterOrEqual(t, s.stats.InverseChecks, s.stats.InverseEdges, "a list is read before it is written")
				assert.GreaterOrEqual(t, s.stats.InverseDistances, s.stats.InverseChecks, "a list that is read has edges")
			} else {
				assert.Zero(t, s.stats.InverseEdges)
				assert.Zero(t, s.stats.InverseChecks)
				assert.Zero(t, s.stats.InverseDistances)
				assert.Nil(t, s.stats.InverseWrites)
				assert.Nil(t, s.stats.InversePostings)
			}
			if tt.cfg.repair {
				assert.GreaterOrEqual(t, s.stats.Repaired, (tt.cfg.threshold+1)*s.stats.Splits,
					"every split repairs all the members of the posting it splits")
			} else {
				assert.Zero(t, s.stats.Repaired)
			}
			if tt.cfg.siblings {
				assert.Positive(t, s.stats.SiblingEdges)
				assert.Positive(t, s.stats.SiblingLists)
			} else {
				assert.Zero(t, s.stats.SiblingEdges)
				assert.Zero(t, s.stats.SiblingLists)
			}

			// The new vectors are where a search finds them.
			hits := 0
			for id := uint64(batch); id < total; id++ {
				ids, _, err := s.ix.SearchByVector(ctx, all[id], 1, nil)
				require.NoError(t, err)
				if len(ids) == 1 && ids[0] == id {
					hits++
				}
			}
			assert.GreaterOrEqual(t, float64(hits)/float64(total-batch), 0.9)
		})
	}
}

func TestPostingListSetEdges(t *testing.T) {
	tests := []struct {
		name      string
		member    int
		edges     []uint32
		wantEdges []uint32
		wantStart []uint32
	}{
		{name: "same length", member: 1, edges: []uint32{8, 9}, wantEdges: []uint32{1, 2, 8, 9, 5}, wantStart: []uint32{0, 2, 4, 5}},
		{name: "shorter", member: 1, edges: []uint32{8}, wantEdges: []uint32{1, 2, 8, 5}, wantStart: []uint32{0, 2, 3, 4}},
		{name: "longer", member: 0, edges: []uint32{7, 8, 9}, wantEdges: []uint32{7, 8, 9, 3, 4, 5}, wantStart: []uint32{0, 3, 5, 6}},
		{name: "emptied", member: 2, edges: nil, wantEdges: []uint32{1, 2, 3, 4}, wantStart: []uint32{0, 2, 4, 4}},
		{name: "from none", member: 2, edges: []uint32{9}, wantEdges: []uint32{1, 2, 3, 4, 9}, wantStart: []uint32{0, 2, 4, 5}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := newPostingList()
			l.append(10, []uint64{0}, []uint32{1, 2})
			l.append(11, []uint64{0}, []uint32{3, 4})
			l.append(12, []uint64{0}, []uint32{5})
			if tt.name == "from none" {
				l.setEdges(2, nil)
			}
			l.setEdges(tt.member, tt.edges)
			assert.Equal(t, tt.wantEdges, l.edges)
			assert.Equal(t, tt.wantStart, l.edgeStart)
		})
	}
}

// checkCoverage catches every way the invariant can break.
func TestStreamCheckCoverage(t *testing.T) {
	const n = 600
	all := clusteredVectors(n+10, 16, 12, 5)
	tests := []struct {
		name    string
		corrupt func(s *streamIndex, p, q int)
		want    string
	}{
		{name: "intact", corrupt: func(*streamIndex, int, int) {}},
		{
			name: "a live vector in no posting",
			corrupt: func(s *streamIndex, p, q int) {
				l := &s.st.postings.lists[p]
				rest := newPostingList()
				for i := 1; i < len(l.docIDs); i++ {
					rest.append(l.docIDs[i], l.codes[i*s.st.side.codeWords:(i+1)*s.st.side.codeWords], l.edgesOf(i))
				}
				*l = rest
			},
			want: "is in 0 postings",
		},
		{
			name: "a vector in two postings",
			corrupt: func(s *streamIndex, p, q int) {
				from := s.st.postings.lists[p]
				words := s.st.side.codeWords
				s.st.postings.lists[q].append(from.docIDs[0], from.codes[:words], from.edgesOf(0))
			},
			want: "which the map puts in",
		},
		{
			name:    "the map pointing elsewhere",
			corrupt: func(s *streamIndex, p, q int) { s.st.side.postingOf[s.st.postings.lists[p].docIDs[0]] = uint32(q) },
			want:    "which the map puts in",
		},
		{
			name: "a posting holding a vector that is not live",
			corrupt: func(s *streamIndex, p, q int) {
				s.live[s.st.postings.lists[p].docIDs[0]] = false
				s.liveCount--
			},
			want: "which is not live",
		},
		{
			name:    "an edge to a vector that is not live",
			corrupt: func(s *streamIndex, p, q int) { s.st.postings.lists[p].edges[0] = n + 3 },
			want:    "an edge to doc",
		},
		{
			name:    "a retired posting that holds vectors",
			corrupt: func(s *streamIndex, p, q int) { s.retired[p] = true },
			want:    "retired posting",
		},
		{
			name: "an edge into its own posting",
			corrupt: func(s *streamIndex, p, q int) {
				l := &s.st.postings.lists[p]
				l.edges[0] = uint32(l.docIDs[len(l.docIDs)-1])
			},
			want: "an edge into itself",
		},
		{
			name:    "a code that differs from the sidecar's",
			corrupt: func(s *streamIndex, p, q int) { s.st.postings.lists[p].codes[1] ^= 1 },
			want:    "has another code",
		},
		{
			name:    "a map entry for a vector that is not live",
			corrupt: func(s *streamIndex, p, q int) { s.st.side.postingOf[n+5] = uint32(p) },
			want:    "is not live, and the map puts it in",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newStreamTestIndex(t, all, n, smallConfig(), streamConfig{threshold: 80})
			// a posting with edges to break, and another one
			p := slices.IndexFunc(s.st.postings.lists, func(l postingList) bool { return len(l.edges) > 0 })
			require.GreaterOrEqual(t, p, 0, "some posting has edges")
			q := (p + 1) % len(s.st.postings.lists)
			tt.corrupt(s, p, q)
			err := s.checkCoverage()
			if tt.want == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.want)
		})
	}
}

// independentCounts counts one vector in posting p with the given edges, as
// edgeCounts does, by sorting every candidate of live: an independent count.
func independentCounts(t *testing.T, all [][]float32, postingOf []uint32, live func(uint64) bool, neighbors int,
	docID uint64, p uint32, edges []uint32,
) edgeCounts {
	provider := distancer.NewL2SquaredProvider()
	type cand struct {
		id   uint64
		dist float32
	}
	var cands []cand
	for other := range uint64(len(all)) {
		if other == docID || !live(other) {
			continue
		}
		dist, err := provider.SingleDist(all[docID], all[other])
		require.NoError(t, err)
		cands = append(cands, cand{id: other, dist: dist})
	}
	sort.SliceStable(cands, func(i, j int) bool {
		if cands[i].dist != cands[j].dist {
			return cands[i].dist < cands[j].dist
		}
		return cands[i].id < cands[j].id
	})
	c := edgeCounts{Vectors: 1, Edges: len(edges)}
	var targets, covered []uint32
	for _, endpoint := range edges {
		covered = append(covered, postingOf[endpoint])
	}
	for _, cand := range cands[:min(neighbors, len(cands))] {
		target := postingOf[cand.id]
		if target == p {
			continue
		}
		c.Wanted++
		if slices.Contains(edges, uint32(cand.id)) {
			c.Captured++
		}
		if !slices.Contains(targets, target) {
			targets = append(targets, target)
		}
	}
	for _, target := range targets {
		c.PostingsWanted++
		if slices.Contains(covered, target) {
			c.PostingsCaptured++
		}
	}
	return c
}

// edgeRecall counts as the batch diagnostic does, at vector and at posting
// level: an independent count, by sorting every live vector, gives the same.
func TestStreamEdgeRecall(t *testing.T) {
	ctx := context.Background()
	all := clusteredVectors(1600, 16, 20, 31)
	s := newStreamTestIndex(t, all, 1200, smallConfig(), streamConfig{threshold: 60})
	uc := smallConfig()
	uc.Distance, uc.EF = "l2-squared", 64
	require.NoError(t, s.ix.UpdateUserConfig(uc, func() {}))
	for id := uint64(1200); id < 1600; id++ {
		require.NoError(t, s.insert(ctx, id, all[id]))
	}
	rng := rand.New(rand.NewPCG(3, 4))
	sample := make([]uint64, 50)
	for i := range sample {
		sample[i] = uint64(rng.IntN(1600))
	}

	var want edgeCounts
	for _, docID := range sample {
		p := s.st.side.postingOf[docID]
		l := s.st.postings.lists[p]
		want.add(independentCounts(t, all, s.st.side.postingOf, func(uint64) bool { return true }, s.neighbors,
			docID, p, l.edgesOf(l.member(docID))))
	}
	got, err := s.edgeRecall(ctx, testLogger(), sample)
	require.NoError(t, err)
	assert.Equal(t, want, got)
	require.Positive(t, got.Wanted, "the sample has true edges to find")
	require.Positive(t, got.Captured, "and its edges find some of them")
	require.Positive(t, got.PostingsWanted)
	assert.InDelta(t, float64(want.PostingsCaptured)/float64(want.PostingsWanted), got.postingRecall(), 1e-12)
	assert.InDelta(t, float64(want.Captured)/float64(want.Wanted), got.recall(), 1e-12)
	assert.LessOrEqual(t, got.degree(), float64(s.neighbors), "no vector keeps more edges than the neighbors it considers")
}

// The guard of the edge builder is called once per insert, right after the
// vector is written and before anything else changes, and counts it against
// the live vectors of that moment as an independent count does.
func TestStreamEdgeRecallAtInsert(t *testing.T) {
	const batch, total = 1200, 1800
	ctx := context.Background()
	// One blob, where the index stays navigable through the splits. In tight
	// clusters of two postings each, the halves of a split have no edge to
	// each other and no path through other postings: neither the inserts'
	// search nor the repair's can reach the other half.
	all := clusteredVectors(total, 16, 1, 61)
	for _, cfg := range []streamConfig{
		{threshold: 60},
		{threshold: 60, inverse: true, repair: true},
		{threshold: 60, inverse: true, siblings: true},
	} {
		t.Run(fmt.Sprintf("inverse %v repair %v siblings %v", cfg.inverse, cfg.repair, cfg.siblings), func(t *testing.T) {
			s := newStreamTestIndex(t, all, batch, smallConfig(), cfg)
			uc := smallConfig()
			uc.Distance, uc.EF = "l2-squared", 64
			require.NoError(t, s.ix.UpdateUserConfig(uc, func() {}))

			var calls int
			var got, want edgeCounts
			var current uint64
			s.onWrite = func(docID uint64, p uint32, edges []uint32) error {
				calls++
				require.Equal(t, current, docID)
				require.Equal(t, p, s.st.side.postingOf[docID], "the hook sees the map written")
				l := s.st.postings.lists[p]
				require.True(t, slices.Equal(edges, l.edgesOf(l.member(docID))), "and the edges")
				require.False(t, s.retired[p], "before any split")
				c, err := s.edgeRecallAtInsert(ctx, testLogger(), docID, p, edges)
				require.NoError(t, err)
				got.add(c)
				want.add(independentCounts(t, all, s.st.side.postingOf, func(id uint64) bool { return id < docID }, s.neighbors,
					docID, p, edges))
				return nil
			}
			for current = batch; current < total; current++ {
				require.NoError(t, s.insert(ctx, current, all[current]))
			}
			assert.Equal(t, total-batch, calls)
			assert.Equal(t, want, got)
			assert.Positive(t, s.stats.Splits)
			assert.Greater(t, got.recall(), 0.9, "the search that makes the edges finds them")
		})
	}
}

// A repair is what lets the members of a split posting reach each other by
// edge: right after the first split, none of them has an edge into the other
// half without one, since their edges were made while they shared a posting.
// The search repair finds the other half when the index has a path to it, as
// in one blob; the sibling repair finds it by construction, tight clusters
// included, where no path does.
func TestStreamRepairReachesTheOtherHalf(t *testing.T) {
	const batch, total = 1200, 2400
	ctx := context.Background()
	blob := clusteredVectors(total, 24, 1, 404)
	tight := clusteredVectors(total, 64, 20, 61)
	tests := []struct {
		name   string
		all    [][]float32
		cfg    streamConfig
		across bool
	}{
		{name: "no repair, one blob", all: blob, cfg: streamConfig{threshold: 60}},
		{name: "no repair, tight clusters", all: tight, cfg: streamConfig{threshold: 60}},
		{name: "search repair, one blob", all: blob, cfg: streamConfig{threshold: 60, repair: true}, across: true},
		{name: "sibling repair, one blob", all: blob, cfg: streamConfig{threshold: 60, siblings: true}, across: true},
		{name: "sibling repair, tight clusters", all: tight, cfg: streamConfig{threshold: 60, siblings: true}, across: true},
		{
			name: "inverse edges and sibling repair, tight clusters", all: tight,
			cfg: streamConfig{threshold: 60, inverse: true, siblings: true}, across: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uc := smallConfig()
			s := newStreamTestIndex(t, tt.all, batch, uc, tt.cfg)
			uc.Distance, uc.EF = "l2-squared", 64
			require.NoError(t, s.ix.UpdateUserConfig(uc, func() {}))
			for id := uint64(batch); id < total && s.stats.Splits == 0; id++ {
				require.NoError(t, s.insert(ctx, id, tt.all[id]))
			}
			require.Equal(t, 1, s.stats.Splits, "a split happened")
			first := uint32(len(s.st.postings.lists) - 2)
			across := 0
			for half := range uint32(2) {
				l := s.st.postings.lists[first+half]
				sibling := first + 1 - half
				for _, endpoint := range l.edges {
					if s.st.side.postingOf[endpoint] == sibling {
						across++
					}
				}
			}
			members := len(s.st.postings.lists[first].docIDs) + len(s.st.postings.lists[first+1].docIDs)
			if tt.across {
				assert.Positive(t, across)
			} else {
				assert.Zero(t, across)
			}
			if tt.cfg.repair {
				assert.Equal(t, members, s.stats.Repaired)
			}
			if tt.cfg.siblings {
				assert.Equal(t, across, s.stats.SiblingEdges)
			}
			require.NoError(t, s.checkCoverage())
		})
	}
}

// The sibling repair keeps, for every member of either half, the nearest of
// its edges before the split and of the members of the other half, by exact
// distance: an independent count right after the first split gives the same
// edges, and the same number of lists that changed. Inverse edges go into
// lists outside the posting that splits, and change none of this. With two
// edges a vector, some members already have both nearer than anything in the
// other half, and their lists do not change.
func TestStreamSiblingRepairKeepsTheNearest(t *testing.T) {
	const batch, total = 1200, 2400
	ctx := context.Background()
	all := clusteredVectors(total, 24, 1, 909)
	provider := distancer.NewL2SquaredProvider()
	tests := []struct {
		name      string
		cfg       streamConfig
		edges     int
		unchanged bool
	}{
		{name: "sibling repair", cfg: streamConfig{threshold: 60, siblings: true}, edges: DefaultEdges},
		{name: "inverse edges and sibling repair", cfg: streamConfig{threshold: 60, inverse: true, siblings: true}, edges: DefaultEdges},
		{name: "sibling repair, two edges", cfg: streamConfig{threshold: 60, siblings: true}, edges: 2, unchanged: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.cfg
			uc := smallConfig()
			uc.Edges = tt.edges
			s := newStreamTestIndex(t, all, batch, uc, cfg)
			require.Equal(t, tt.edges, s.neighbors)
			uc.Distance, uc.EF = "l2-squared", 64
			require.NoError(t, s.ix.UpdateUserConfig(uc, func() {}))
			// The edges before the split: those of every vector before the
			// insert that splits, and those that insert wrote for its own
			// vector.
			var before map[uint64][]uint32
			s.onWrite = func(docID uint64, _ uint32, edges []uint32) error {
				before[docID] = slices.Clone(edges)
				return nil
			}
			for id := uint64(batch); id < total && s.stats.Splits == 0; id++ {
				before = edgesByDoc(s)
				require.NoError(t, s.insert(ctx, id, all[id]))
			}
			require.Equal(t, 1, s.stats.Splits)
			first := uint32(len(s.st.postings.lists) - 2)
			changed, members := 0, 0
			for half := range uint32(2) {
				l := s.st.postings.lists[first+half]
				other := s.st.postings.lists[first+1-half]
				for i, docID := range l.docIDs {
					type cand struct {
						id   uint64
						dist float32
					}
					var cands []cand
					for _, endpoint := range before[docID] {
						dist, err := provider.SingleDist(all[docID], all[endpoint])
						require.NoError(t, err)
						cands = append(cands, cand{id: uint64(endpoint), dist: dist})
					}
					for _, sibling := range other.docIDs {
						dist, err := provider.SingleDist(all[docID], all[sibling])
						require.NoError(t, err)
						cands = append(cands, cand{id: sibling, dist: dist})
					}
					sort.SliceStable(cands, func(i, j int) bool { return cands[i].dist < cands[j].dist })
					var want []uint32
					for _, c := range cands[:min(s.neighbors, len(cands))] {
						want = append(want, uint32(c.id))
					}
					assert.Equal(t, want, l.edgesOf(i), "doc %d", docID)
					members++
					if !slices.Equal(want, before[docID]) {
						changed++
					}
				}
			}
			assert.Equal(t, changed, s.stats.SiblingLists)
			assert.Positive(t, changed)
			if tt.unchanged {
				assert.Less(t, changed, members, "some lists stay as they were")
			}
		})
	}
}

// The inverse edges of an insert are counted as they are written: a neighbor
// outside the new vector's posting, with edges, takes the new vector when it
// is nearer than the neighbor's worst edge, and an independent count of the
// lists that took it, and of the postings they live in, gives the same
// histograms, insert by insert. A split, with or without the sibling repair,
// changes no list outside the posting it splits.
func TestStreamInverseWritesPerInsert(t *testing.T) {
	const batch, total = 1200, 2400
	ctx := context.Background()
	// Tight clusters of a few postings each: some vectors have every near
	// neighbor in their own posting and no edges, and the neighbors an insert
	// finds outside its posting often share one.
	all := clusteredVectors(total, 64, 20, 61)
	provider := distancer.NewL2SquaredProvider()
	for _, cfg := range []streamConfig{{threshold: 60, inverse: true}, {threshold: 60, inverse: true, siblings: true}} {
		t.Run(fmt.Sprintf("siblings %v", cfg.siblings), func(t *testing.T) {
			uc := smallConfig()
			s := newStreamTestIndex(t, all, batch, uc, cfg)
			uc.Distance, uc.EF = "l2-squared", 64
			require.NoError(t, s.ix.UpdateUserConfig(uc, func() {}))

			listOf := func(docID uint64) []uint32 {
				l := s.st.postings.lists[s.st.side.postingOf[docID]]
				return l.edgesOf(l.member(docID))
			}
			// What the insert found outside its posting, and the lists and
			// postings of those neighbors before any inverse edge.
			var candidates []uint32
			var lists [][]uint32
			var postings []uint32
			s.onWrite = func(_ uint64, _ uint32, edges []uint32) error {
				candidates, lists, postings = slices.Clone(edges), nil, nil
				for _, u := range edges {
					lists = append(lists, slices.Clone(listOf(uint64(u))))
					postings = append(postings, s.st.side.postingOf[u])
				}
				return nil
			}
			wantWrites, wantPostings := make([]int, s.neighbors+1), make([]int, s.neighbors+1)
			checks, distances, empty, shared := 0, 0, 0, 0
			for id := uint64(batch); id < total; id++ {
				require.NoError(t, s.insert(ctx, id, all[id]))
				writes := 0
				var written []uint32
				for c, u := range candidates {
					now := listOf(uint64(u))
					if len(lists[c]) == 0 {
						empty++
						require.Empty(t, now, "doc %d has no edges and takes none", u)
						continue
					}
					checks++
					distances += len(lists[c])
					dist, err := provider.SingleDist(all[u], all[id])
					require.NoError(t, err)
					worst, err := provider.SingleDist(all[u], all[lists[c][len(lists[c])-1]])
					require.NoError(t, err)
					require.Equal(t, dist < worst, slices.Contains(now, uint32(id)),
						"doc %d at %f of doc %d, whose worst edge is at %f", id, dist, u, worst)
					if dist < worst {
						require.Len(t, now, len(lists[c]), "an inverse edge takes the place of the worst edge")
						writes++
						if !slices.Contains(written, postings[c]) {
							written = append(written, postings[c])
						}
					} else {
						require.True(t, slices.Equal(lists[c], now), "doc %d keeps its edges", u)
					}
				}
				wantWrites[writes]++
				wantPostings[len(written)]++
				if len(written) < writes {
					shared++
				}
			}
			assert.Equal(t, wantWrites, s.stats.InverseWrites)
			assert.Equal(t, wantPostings, s.stats.InversePostings)
			assert.Equal(t, checks, s.stats.InverseChecks)
			assert.Equal(t, distances, s.stats.InverseDistances)
			assert.Positive(t, s.stats.Splits)
			// The data has what tells the counts apart: neighbors without
			// edges, inserts that write nothing and inserts whose inverse
			// edges share a posting.
			assert.Positive(t, empty, "some neighbors have no edges")
			assert.Positive(t, wantWrites[0], "some inserts write no inverse edge")
			assert.Positive(t, shared, "some inserts write two lists of one posting")
			// A checkpoint keeps counts of its own.
			kept := s.stats.clone()
			s.stats.InverseWrites[0]++
			s.stats.InversePostings[0]++
			assert.Equal(t, wantWrites, kept.InverseWrites)
			assert.Equal(t, wantPostings, kept.InversePostings)
		})
	}
}

// The work an insert is charged is that of its own edge search: the same
// search run just before it, on the index it finds, does the same work. The
// searches of a repair are not charged to the insert, and a cell that does
// not search charges nothing.
func TestStreamInsertSearchWork(t *testing.T) {
	const batch, total = 1200, 1800
	ctx := context.Background()
	all := clusteredVectors(total, 24, 30, 421)

	tests := []struct {
		name string
		cfg  streamConfig
	}{
		{name: "principal", cfg: streamConfig{threshold: 60}},
		{name: "inverse edges", cfg: streamConfig{threshold: 60, inverse: true}},
		{name: "edge repair", cfg: streamConfig{threshold: 60, repair: true}},
		{name: "inverse edges and sibling repair", cfg: streamConfig{threshold: 60, inverse: true, siblings: true}},
		{name: "assignment only", cfg: streamConfig{threshold: 60, assignOnly: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uc := smallConfig()
			s := newStreamTestIndex(t, all, batch, uc, tt.cfg)
			uc.Distance, uc.EF = "l2-squared", 48
			require.NoError(t, s.ix.UpdateUserConfig(uc, func() {}))

			var want streamStats
			for id := uint64(batch); id < total; id++ {
				if !tt.cfg.assignOnly {
					_, _, work, err := s.ix.SearchByVectorWithStats(ctx, all[id], s.neighbors, nil)
					require.NoError(t, err)
					want.countSearch(work)
				}
				require.NoError(t, s.insert(ctx, id, all[id]))
			}
			require.NoError(t, s.checkCoverage())

			got := s.stats
			assert.Equal(t, want.SearchLoads, got.SearchLoads)
			assert.Equal(t, want.SearchRounds, got.SearchRounds)
			assert.Equal(t, want.SearchScanned, got.SearchScanned)
			assert.Equal(t, want.SearchEndpoints, got.SearchEndpoints)
			assert.Equal(t, want.SearchRescored, got.SearchRescored)
			assert.Equal(t, want.SearchLoadsHist, got.SearchLoadsHist)
			if tt.cfg.assignOnly {
				assert.Zero(t, got.SearchLoads)
				assert.Nil(t, got.SearchLoadsHist)
				return
			}
			if tt.cfg.repair {
				assert.Positive(t, got.Repaired, "the repair searched, and none of it is charged")
			}

			// Every insert is counted once, by the postings its search loaded.
			inserts, loads := 0, 0
			for l, count := range got.SearchLoadsHist {
				inserts += count
				loads += l * count
			}
			assert.Equal(t, got.Inserted, inserts)
			assert.Equal(t, got.SearchLoads, loads)
			assert.GreaterOrEqual(t, got.SearchLoads, got.Inserted, "a search loads its entry posting at least")
			assert.GreaterOrEqual(t, got.SearchScanned, got.SearchRescored, "a candidate is a scanned member")
			assert.Positive(t, got.SearchEndpoints)

			// The counts a checkpoint keeps are its own.
			kept := s.stats.clone()
			s.stats.SearchLoadsHist[0]++
			assert.Equal(t, want.SearchLoadsHist, kept.SearchLoadsHist)
		})
	}
}

// A counting provider computes what the provider it wraps computes, and
// counts one distance for each: by SingleDist and by the distancers it hands
// out, which is how the centroid graph reaches it.
func TestCountingProvider(t *testing.T) {
	plain := distancer.NewL2SquaredProvider()
	var count atomic.Int64
	counting := countingProvider{Provider: plain, count: &count}
	a, b, c := []float32{1, 2, 3}, []float32{4, 6, 8}, []float32{0, 0, 1}

	assert.Equal(t, plain.Type(), counting.Type())
	want, err := plain.SingleDist(a, b)
	require.NoError(t, err)
	got, err := counting.SingleDist(a, b)
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Equal(t, int64(1), count.Load())

	from := counting.New(a)
	for i, to := range [][]float32{b, c, a} {
		want, err := plain.SingleDist(a, to)
		require.NoError(t, err)
		got, err := from.Distance(to)
		require.NoError(t, err)
		assert.Equal(t, want, got)
		assert.Equal(t, int64(2+i), count.Load())
	}

	// A copy counts into the same count.
	copied := counting
	_, err = copied.SingleDist(a, c)
	require.NoError(t, err)
	assert.Equal(t, int64(5), count.Load())
}

// The work of the inserts outside their edge search, in counts: every
// distance the centroid graph computes after its build is charged to the step
// that asked for it, and to one step only; an insert asks the graph once, for
// its posting, and its edge search asks nothing of it; a split is charged its
// 2-means and the distances of its members to the two means; and without a
// count of the graph's distances nothing is charged for the graph.
func TestStreamInsertWorkOutsideTheSearch(t *testing.T) {
	const batch, total, threshold = 1200, 2400, 60
	ctx := context.Background()
	all := clusteredVectors(total, 24, 30, 733)

	tests := []struct {
		name    string
		cfg     streamConfig
		counted bool
	}{
		{name: "inverse edges", cfg: streamConfig{threshold: threshold, inverse: true}, counted: true},
		{name: "principal", cfg: streamConfig{threshold: threshold}, counted: true},
		{name: "assignment only", cfg: streamConfig{threshold: threshold, assignOnly: true}, counted: true},
		{name: "inverse edges, the graph not counted", cfg: streamConfig{threshold: threshold, inverse: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.cfg
			if tt.counted {
				cfg.centroidDistances = new(atomic.Int64)
			}
			s := newStreamTestIndex(t, all, batch, smallConfig(), cfg)
			built := s.centroidCount()
			if tt.counted {
				require.Positive(t, built, "the build of the graph is counted")
			}

			for id := uint64(batch); id < total; id++ {
				require.NoError(t, s.insert(ctx, id, all[id]))
				if id == (batch+total)/2 {
					require.NoError(t, s.cleanCentroidGraph())
				}
			}
			require.NoError(t, s.cleanCentroidGraph())
			require.NoError(t, s.checkCoverage())

			got := s.stats
			splits := int64(got.Splits + got.SplitsFailed)
			require.Positive(t, got.Splits)
			// A split is over more than threshold members: 2m distances
			// to start its 2-means, 2m for each of its one to nine
			// iterations, and 2m to the two means.
			assert.GreaterOrEqual(t, got.SplitDistances, 6*(threshold+1)*splits)

			if !tt.counted {
				assert.Zero(t, got.AssignDistances)
				assert.Zero(t, got.SplitGraphDistances)
				assert.Zero(t, got.CleanDistances)
				return
			}
			// The edge search of an insert computes none: it enters through
			// the posting its insert found, so the sum has no term for it.
			assert.Equal(t, s.centroidCount()-built,
				got.AssignDistances+got.SplitGraphDistances+got.CleanDistances,
				"every distance of the graph is charged, once")
			assert.GreaterOrEqual(t, got.AssignDistances, int64(got.Inserted), "an insert finds its posting in the graph")
			assert.Positive(t, got.SplitGraphDistances, "a split adds two centroids to the graph")
			assert.Positive(t, got.CleanDistances, "a cleanup reconnects the neighbors of the deleted centroids")
		})
	}
}

// The degree by cohort counts every live vector once, in the cohort of the
// place it entered the index at, with the edges it has now.
func TestStreamDegreeByCohort(t *testing.T) {
	const batch, total = 1200, 2400
	ctx := context.Background()
	all := clusteredVectors(total, 24, 30, 947)
	s := newStreamTestIndex(t, all, batch, smallConfig(), streamConfig{threshold: 60, inverse: true})

	// The inserts enter in another order than their doc ids.
	order := make([]uint64, total)
	for i := range order {
		order[i] = uint64(i)
	}
	rand.New(rand.NewPCG(5, 5)).Shuffle(total-batch, func(i, j int) {
		order[batch+i], order[batch+j] = order[batch+j], order[batch+i]
	})
	rank := make([]uint32, total)
	for place, id := range order {
		rank[id] = uint32(place)
	}
	const upto = 2000
	for _, id := range order[batch:upto] {
		require.NoError(t, s.insert(ctx, id, all[id]))
	}
	require.NoError(t, s.checkCoverage())
	edges := edgesByDoc(s)

	tests := []struct {
		name   string
		bounds []int
	}{
		{name: "the batch part and two stretches", bounds: []int{0, batch, 1500, upto}},
		{name: "the batch part and the inserts", bounds: []int{0, batch, upto}},
		{name: "past what is live", bounds: []int{0, batch, upto, total}},
		{name: "the batch part alone", bounds: []int{0, batch}},
		{name: "one stretch in the middle", bounds: []int{1500, 1700}},
		{name: "no cohort", bounds: []int{0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := s.degreeByCohort(rank, tt.bounds)
			if len(tt.bounds) < 2 {
				require.Nil(t, got)
				return
			}
			want := make([]cohortDegree, len(tt.bounds)-1)
			for i := range want {
				want[i].From, want[i].To = tt.bounds[i], tt.bounds[i+1]
				for _, id := range order[want[i].From:min(want[i].To, upto)] {
					want[i].Vectors++
					want[i].Edges += len(edges[id])
				}
			}
			require.Equal(t, want, got)
			if tt.bounds[0] == 0 && tt.bounds[len(tt.bounds)-1] >= upto {
				vectors := 0
				for _, c := range got {
					vectors += c.Vectors
				}
				require.Equal(t, s.liveCount, vectors, "every live vector is in one cohort")
			}
		})
	}
}

// batchRuleEdges returns the edges the batch rule gives every live vector
// over the partition of s as it is, counted on its own by sorting: of the
// neighbors nearest to a vector among the members of its probe nearest live
// postings, with its own posting in the place of the farthest when it is not
// one of them, those outside its posting, nearest first. Every candidate is
// compared exactly, as a build does with a rescue limit above their number.
// With them, the vectors whose posting is not that of their nearest
// centroid, and those whose posting is not among the probe nearest.
func batchRuleEdges(t *testing.T, s *streamIndex, all [][]float32, probe int,
) (edges map[uint64][]uint32, notNearest, outside int) {
	t.Helper()
	provider := distancer.NewL2SquaredProvider()
	type cand struct {
		id   uint64
		dist float32
	}
	byDistance := func(cands []cand) {
		sort.SliceStable(cands, func(i, j int) bool {
			if cands[i].dist != cands[j].dist {
				return cands[i].dist < cands[j].dist
			}
			return cands[i].id < cands[j].id
		})
	}
	var alive []uint32
	for p := range s.st.postings.lists {
		if !s.retired[p] {
			alive = append(alive, uint32(p))
		}
	}
	edges = map[uint64][]uint32{}
	for docID := range uint64(len(all)) {
		if !s.live[docID] {
			continue
		}
		own := s.st.side.postingOf[docID]
		near := make([]cand, 0, len(alive))
		for _, p := range alive {
			dist, err := provider.SingleDist(all[docID], s.st.centroids.vectors[p])
			require.NoError(t, err)
			near = append(near, cand{id: uint64(p), dist: dist})
		}
		byDistance(near)
		if uint32(near[0].id) != own {
			notNearest++
		}
		near = near[:min(probe, len(near))]
		if !slices.ContainsFunc(near, func(c cand) bool { return uint32(c.id) == own }) {
			outside++
			near[len(near)-1].id = uint64(own)
		}
		var cands []cand
		for _, c := range near {
			for _, other := range s.st.postings.lists[c.id].docIDs {
				if other == docID {
					continue
				}
				dist, err := provider.SingleDist(all[docID], all[other])
				require.NoError(t, err)
				cands = append(cands, cand{id: other, dist: dist})
			}
		}
		byDistance(cands)
		list := []uint32{}
		for _, c := range cands[:min(s.neighbors, len(cands))] {
			if s.st.side.postingOf[c.id] != own {
				list = append(list, uint32(c.id))
			}
		}
		edges[docID] = list
	}
	return edges, notNearest, outside
}

// Rebuilding the edges gives every live vector what the batch builder gives
// it over the partition as it is, and touches nothing else. Over the
// partition of a batch build that is the edges the build left. Over one
// grown by inserts it is the batch rule, counted here on its own, whatever
// the inserts and their inverse edges had left there. And the inserts go on
// afterwards.
func TestStreamRebuildEdges(t *testing.T) {
	const batch, total = 1200, 2400
	ctx := context.Background()
	all := clusteredVectors(total, 24, 30, 613)

	tests := []struct {
		name string
		// buildProbe and rescue are the build's, and the rebuild's rescue.
		buildProbe, rescue int
		// upto is how far the inserts go before the rebuild, and probe the
		// probe of the rebuild.
		upto, probe int
		// asBuilt: the edges are those of before the rebuild.
		asBuilt bool
		// noEdges: no vector gets any.
		noEdges bool
	}{
		{name: "a batch build that probed every posting", buildProbe: 64, rescue: 64, upto: batch, probe: 64, asBuilt: true},
		{name: "a batch build that probed some", buildProbe: 16, rescue: 64, upto: batch, probe: 16, asBuilt: true},
		{name: "a batch build, exactly", buildProbe: 16, rescue: total, upto: batch, probe: 16, asBuilt: true},
		{name: "grown by inserts, every posting probed", buildProbe: 16, rescue: total, upto: 2200, probe: 4096},
		{name: "grown by inserts, a few postings probed", buildProbe: 16, rescue: total, upto: 2200, probe: 6},
		{name: "grown by inserts, one posting probed", buildProbe: 16, rescue: total, upto: 2200, probe: 1, noEdges: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uc := smallConfig()
			uc.BuildProbe, uc.RescueLimit = tt.buildProbe, tt.rescue
			s := newStreamTestIndex(t, all, batch, uc, streamConfig{threshold: 60, inverse: true})
			// No empty centroid was dropped: the probes of the build are the
			// nearest live centroids, as those of the rebuild.
			require.Equal(t, batch/uc.TargetPostingSize, s.st.build.Postings)
			uc.Distance, uc.EF = "l2-squared", 64
			require.NoError(t, s.ix.UpdateUserConfig(uc, func() {}))
			for id := uint64(batch); id < uint64(tt.upto); id++ {
				require.NoError(t, s.insert(ctx, id, all[id]))
			}
			require.NoError(t, s.cleanCentroidGraph())
			require.NoError(t, s.checkCoverage())

			before := edgesByDoc(s)
			var members [][]uint64
			var codes [][]uint64
			for _, l := range s.st.postings.lists {
				members = append(members, slices.Clone(l.docIDs))
				codes = append(codes, slices.Clone(l.codes))
			}
			postingOf := slices.Clone(s.st.side.postingOf)
			retired := slices.Clone(s.retired)
			graph, centroids := s.st.centroids, len(s.st.centroids.vectors)
			stats := s.stats.clone()

			want, notNearest, outside := batchRuleEdges(t, s, all, tt.probe)
			out, err := s.rebuildEdges(ctx, testLogger(), tt.probe, tt.rescue)
			require.NoError(t, err)

			// Nothing but the edges changed.
			require.NoError(t, s.checkCoverage())
			for p, l := range s.st.postings.lists {
				require.Equal(t, members[p], l.docIDs, "the members of posting %d", p)
				require.Equal(t, codes[p], l.codes, "the codes of posting %d", p)
			}
			require.Equal(t, postingOf, s.st.side.postingOf)
			require.Equal(t, retired, s.retired)
			require.Same(t, graph, s.st.centroids)
			require.Len(t, s.st.centroids.vectors, centroids)
			require.Equal(t, stats, s.stats, "a rebuild is no insert")

			got := edgesByDoc(s)
			require.Len(t, got, s.liveCount)
			edges := 0
			for docID, list := range got {
				edges += len(list)
				switch {
				case tt.asBuilt:
					require.Truef(t, slices.Equal(before[docID], list), "doc %d: %v before, %v rebuilt", docID, before[docID], list)
				case tt.noEdges:
					require.Empty(t, list, "doc %d", docID)
				}
				if tt.rescue >= total {
					require.Truef(t, slices.Equal(want[docID], list), "doc %d: %v by the rule, %v rebuilt", docID, want[docID], list)
				}
			}

			live := 0
			for p := range s.st.postings.lists {
				if !s.retired[p] {
					live++
				}
			}
			assert.Equal(t, s.liveCount, out.Vectors)
			assert.Equal(t, live, out.Postings)
			assert.Equal(t, edges, out.Edges)
			assert.Equal(t, int64(s.liveCount)*int64(live), out.AssignDistances)
			assert.Equal(t, int64(s.liveCount)*int64(min(tt.probe, live)), out.PostingsProbed)
			assert.Equal(t, notNearest, out.NotNearest)
			assert.Equal(t, outside, out.OutsideProbes)
			if tt.rescue >= total {
				// Every candidate but the vector itself is compared exactly.
				assert.Equal(t, out.CandidatesScored-int64(s.liveCount), out.CandidatesRescored)
			}
			switch {
			case tt.asBuilt:
				assert.Zero(t, out.NotNearest, "a batch build puts every vector with its nearest centroid")
				assert.Positive(t, edges)
			case tt.noEdges:
				assert.Positive(t, out.NotNearest, "inserts and splits leave vectors away from their nearest centroid")
				assert.Equal(t, out.NotNearest, out.OutsideProbes, "with one probe, the nearest centroid is all there is")
				assert.Zero(t, edges, "a vector that sees its own posting alone has no neighbor outside it")
			default:
				assert.Positive(t, out.NotNearest, "inserts and splits leave vectors away from their nearest centroid")
				assert.NotEqual(t, before, got, "the edges of the inserts are not those of the batch rule")
			}
			if tt.probe >= live && tt.rescue >= total {
				// The exact edges of every vector: nothing is missing.
				docIDs := make([]uint64, 0, s.liveCount)
				for docID := range got {
					docIDs = append(docIDs, docID)
				}
				counts, err := s.edgeRecall(ctx, testLogger(), docIDs)
				require.NoError(t, err)
				assert.Equal(t, counts.Wanted, counts.Captured)
				assert.Equal(t, counts.PostingsWanted, counts.PostingsCaptured)
				assert.Equal(t, edges, counts.Edges)
			}

			for id := uint64(tt.upto); id < total; id++ {
				require.NoError(t, s.insert(ctx, id, all[id]))
			}
			require.NoError(t, s.checkCoverage())
			assert.Equal(t, total, s.liveCount)
		})
	}
}

// A rebuilt centroid graph holds the live centroids and only them, under
// their posting ids, finds them, and serves the inserts that follow. The
// postings and their edges are not touched.
func TestStreamRebuildCentroidGraph(t *testing.T) {
	const batch, total = 1200, 2400
	ctx := context.Background()
	all := clusteredVectors(total, 24, 30, 271)
	uc := smallConfig()
	s := newStreamTestIndex(t, all, batch, uc, streamConfig{threshold: 60, inverse: true})
	uc.Distance, uc.EF = "l2-squared", 64
	require.NoError(t, s.ix.UpdateUserConfig(uc, func() {}))
	for id := uint64(batch); id < 2000; id++ {
		require.NoError(t, s.insert(ctx, id, all[id]))
	}
	require.Positive(t, s.stats.Splits)
	// With its deleted centroids still in the graph: the rebuild leaves them
	// out without a cleanup.
	old, edges, postingOf := s.st.centroids, edgesByDoc(s), slices.Clone(s.st.side.postingOf)

	require.NoError(t, s.rebuildCentroidGraph(ctx))
	require.NotSame(t, old, s.st.centroids)
	require.NoError(t, s.checkCoverage(), "the graph holds the live postings and none of the retired ones")
	assert.Equal(t, edges, edgesByDoc(s))
	assert.Equal(t, postingOf, s.st.side.postingOf)

	live, found := 0, 0
	for p := range s.st.postings.lists {
		if s.retired[p] {
			continue
		}
		live++
		ids, err := s.st.centroids.nearest(ctx, s.st.centroids.vectors[p], 1)
		require.NoError(t, err)
		require.Len(t, ids, 1)
		require.False(t, s.retired[ids[0]], "the graph returned posting %d, which is retired", ids[0])
		if ids[0] == uint64(p) {
			found++
		}
	}
	assert.GreaterOrEqual(t, found*10, live*9, "%d of %d centroids find themselves", found, live)

	for id := uint64(2000); id < total; id++ {
		require.NoError(t, s.insert(ctx, id, all[id]))
	}
	require.NoError(t, s.checkCoverage())
	assert.Equal(t, total, s.liveCount)
}

// A refresh repeats the edge step of an insert for a vector that is in the
// index. Its list becomes what the rule of the insert gives over its own
// search, entered through its posting: the nearest neighbors found, itself
// left out, that are outside its posting, with no endpoint twice. It enters
// the lists of those neighbors as a new vector does, and not where it is
// already. Nothing else changes: no other list, nothing of the partition and
// none of the counts of the inserts. Over an exact batch build, with a search
// that compares everything it reaches exactly, every list stays as it was.
func TestStreamRefresh(t *testing.T) {
	const batch, total = 1200, 2400
	ctx := context.Background()
	all := clusteredVectors(total, 24, 30, 947)
	provider := distancer.NewL2SquaredProvider()

	tests := []struct {
		name    string
		inverse bool
		// upto is how far the inserts go before the refresh, and ef and
		// rescore are the search parameters of the inserts and the refresh.
		upto, ef, rescore int
		// exact: the batch build probes every posting and compares every
		// candidate exactly.
		exact bool
	}{
		{name: "an exact batch build, a search that compares all it reaches", inverse: true, upto: batch, ef: total, rescore: total, exact: true},
		{name: "grown by inserts", inverse: true, upto: 2200, ef: 64, rescore: 64},
		{name: "grown by inserts, a narrow search", inverse: true, upto: 2200, ef: 8, rescore: 20},
		{name: "grown by inserts, without inverse edges", upto: 2200, ef: 64, rescore: 64},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uc := smallConfig()
			if tt.exact {
				uc.BuildProbe, uc.RescueLimit = 64, total
			}
			s := newStreamTestIndex(t, all, batch, uc, streamConfig{threshold: 60, inverse: tt.inverse})
			uc.Distance, uc.EF, uc.RescoreLimit = "l2-squared", tt.ef, tt.rescore
			require.NoError(t, s.ix.UpdateUserConfig(uc, func() {}))
			for id := uint64(batch); id < uint64(tt.upto); id++ {
				require.NoError(t, s.insert(ctx, id, all[id]))
			}
			require.NoError(t, s.cleanCentroidGraph())
			require.NoError(t, s.checkCoverage())
			require.Zero(t, s.repeatedEdges(), "the build and the inserts leave no endpoint twice in a list")

			var members [][]uint64
			var codes [][]uint64
			for _, l := range s.st.postings.lists {
				members = append(members, slices.Clone(l.docIDs))
				codes = append(codes, slices.Clone(l.codes))
			}
			postingOf := slices.Clone(s.st.side.postingOf)
			retired := slices.Clone(s.retired)
			graph, centroids := s.st.centroids, len(s.st.centroids.vectors)
			stats := s.stats.clone()
			docIDs := sequentialIDs(tt.upto, 0)
			staleness, err := s.edgeRecall(ctx, testLogger(), docIDs)
			require.NoError(t, err)

			listOf := func(docID uint64) []uint32 {
				l := s.st.postings.lists[s.st.side.postingOf[docID]]
				return l.edgesOf(l.member(docID))
			}
			type outsider struct {
				id   uint64
				dist float32
				list []uint32
			}
			var sum refreshStats
			wantWrites := make([]int, s.neighbors+1)
			already, written := 0, 0
			for _, id := range docIDs {
				p := s.st.side.postingOf[id]
				// The search of the refresh, run just before it.
				ids, dists, err := s.ix.searchFrom(ctx, all[id], s.neighbors+1, nil, []uint64{uint64(p)}, nil)
				require.NoError(t, err)
				at := slices.Index(ids, id)
				require.GreaterOrEqual(t, at, 0, "the search of doc %d finds it", id)
				ids, dists = slices.Delete(ids, at, at+1), slices.Delete(dists, at, at+1)
				require.LessOrEqual(t, len(ids), s.neighbors)
				var want []uint32
				var outsiders []outsider
				for i, u := range ids {
					if s.st.side.postingOf[u] != p {
						want = append(want, uint32(u))
						outsiders = append(outsiders, outsider{id: u, dist: dists[i], list: slices.Clone(listOf(u))})
					}
				}
				old := slices.Clone(listOf(id))
				// For some, every list of the index: only those of the
				// vector and of its neighbors outside its posting may change.
				var everything map[uint64][]uint32
				if id%97 == 0 {
					everything = edgesByDoc(s)
				}

				out, err := s.refreshEdges(ctx, []uint64{id})
				require.NoError(t, err)

				got := listOf(id)
				require.Truef(t, slices.Equal(want, got), "doc %d: %v by the rule over its search, %v refreshed", id, want, got)
				require.NotContains(t, got, uint32(id), "a vector is no edge of its own")
				touched := map[uint64]bool{id: true}
				writes := 0
				for _, o := range outsiders {
					touched[o.id] = true
					now := listOf(o.id)
					if len(o.list) == 0 {
						require.Empty(t, now, "doc %d has no edges and takes none", o.id)
						continue
					}
					if slices.Contains(o.list, uint32(id)) {
						// Only the inverse edges look at the neighbors.
						if tt.inverse {
							already++
						}
						require.Truef(t, slices.Equal(o.list, now), "doc %d had doc %d already and keeps its edges", o.id, id)
						continue
					}
					worst, err := provider.SingleDist(all[o.id], all[o.list[len(o.list)-1]])
					require.NoError(t, err)
					if !tt.inverse || o.dist >= worst {
						require.Truef(t, slices.Equal(o.list, now), "doc %d keeps its edges", o.id)
						continue
					}
					// In the place its distance gives it, and the worst
					// edge gone.
					writes++
					place := slices.Index(now, uint32(id))
					require.GreaterOrEqual(t, place, 0, "doc %d, at %f of doc %d whose worst edge is at %f, is among its edges", id, o.dist, o.id, worst)
					rest := slices.Delete(slices.Clone(now), place, place+1)
					require.Truef(t, slices.Equal(o.list[:len(o.list)-1], rest), "doc %d: %v before, %v now", o.id, o.list, now)
					if place > 0 {
						nearer, err := provider.SingleDist(all[o.id], all[now[place-1]])
						require.NoError(t, err)
						require.LessOrEqual(t, nearer, o.dist)
					}
					if place+1 < len(now) {
						farther, err := provider.SingleDist(all[o.id], all[now[place+1]])
						require.NoError(t, err)
						require.Greater(t, farther, o.dist)
					}
				}
				for docID, list := range everything {
					if !touched[docID] {
						require.Truef(t, slices.Equal(list, listOf(docID)), "doc %d is no neighbor of doc %d and keeps its edges", docID, id)
					}
				}

				kept := 0
				for _, endpoint := range want {
					if slices.Contains(old, endpoint) {
						kept++
					}
				}
				require.Equal(t, 1, out.Refreshed)
				require.Zero(t, out.SelfMissing)
				require.Equal(t, len(old), out.EdgesBefore)
				require.Equal(t, len(want), out.EdgesAfter)
				require.Equal(t, kept, out.EdgesKept)
				require.Equal(t, !slices.Equal(old, want), out.ListsChanged == 1)
				require.Equal(t, writes, out.Work.InverseEdges)
				require.Equal(t, 1, out.Work.SearchLoadsHist[out.Work.SearchLoads], "one search, with the loads it is charged")
				require.Positive(t, out.Work.SearchRescored)
				require.Zero(t, out.Work.Inserted, "a refresh is no insert")
				written += writes
				wantWrites[writes]++
				sum.Refreshed += out.Refreshed
				sum.ListsChanged += out.ListsChanged
				sum.EdgesBefore += out.EdgesBefore
				sum.EdgesAfter += out.EdgesAfter
				sum.EdgesKept += out.EdgesKept
				sum.Work.InverseAlready += out.Work.InverseAlready
				if tt.inverse {
					require.Equal(t, 1, out.Work.InverseWrites[writes], "one refresh, with the inverse edges it wrote")
				} else {
					require.Nil(t, out.Work.InverseWrites)
				}
			}

			// Nothing but edges changed, and the inserts keep their counts.
			require.NoError(t, s.checkCoverage())
			require.Zero(t, s.repeatedEdges(), "a refresh leaves no endpoint twice in a list")
			for p, l := range s.st.postings.lists {
				require.Equal(t, members[p], l.docIDs, "the members of posting %d", p)
				require.Equal(t, codes[p], l.codes, "the codes of posting %d", p)
			}
			require.Equal(t, postingOf, s.st.side.postingOf)
			require.Equal(t, retired, s.retired)
			require.Same(t, graph, s.st.centroids)
			require.Len(t, s.st.centroids.vectors, centroids)
			require.Equal(t, stats, s.stats, "a refresh is no insert")

			assert.Equal(t, tt.upto, sum.Refreshed)
			assert.Equal(t, already, sum.Work.InverseAlready)
			switch {
			case tt.exact:
				// The lists are those of the rule already: none changes, and
				// a vector that beats the worst edge of a neighbor is among
				// its edges.
				assert.Zero(t, sum.ListsChanged)
				assert.Zero(t, written)
				assert.Equal(t, sum.EdgesBefore, sum.EdgesAfter)
				assert.Equal(t, sum.EdgesBefore, sum.EdgesKept)
				assert.Positive(t, already)
			case tt.inverse:
				assert.Positive(t, sum.ListsChanged)
				assert.Positive(t, written)
				assert.Positive(t, already, "some neighbors have the refreshed vector among their edges")
				assert.Positive(t, wantWrites[0], "some refreshes write no inverse edge")
			default:
				assert.Positive(t, sum.ListsChanged)
				assert.Zero(t, written)
				assert.Zero(t, already, "without inverse edges no neighbor is looked at")
			}
			if tt.ef >= 64 && !tt.exact {
				// What the refresh is for: the edges of the vectors against
				// the corpus as it is now.
				fresh, err := s.edgeRecall(ctx, testLogger(), docIDs)
				require.NoError(t, err)
				assert.Greater(t, fresh.recall(), staleness.recall())
			}

			// A vector that is not in the index has nothing to refresh, and
			// the counts of the inserts come back all the same.
			if tt.upto < total {
				_, err := s.refreshEdges(ctx, []uint64{uint64(tt.upto)})
				require.Error(t, err)
				require.Equal(t, stats, s.stats)
			}
			_, err = s.refreshEdges(ctx, []uint64{total})
			require.Error(t, err)

			// The inserts go on.
			for id := uint64(tt.upto); id < total; id++ {
				require.NoError(t, s.insert(ctx, id, all[id]))
			}
			require.NoError(t, s.checkCoverage())
			require.Zero(t, s.repeatedEdges())
			assert.Equal(t, total, s.liveCount)
		})
	}
}

// The stages of a refresh add up: several vectors refreshed in one call are
// counted as each of them alone, in the order given.
func TestStreamRefreshEdgesCounts(t *testing.T) {
	const batch, total = 1200, 2000
	ctx := context.Background()
	all := clusteredVectors(total, 24, 30, 389)
	build := func() *streamIndex {
		uc := smallConfig()
		s := newStreamTestIndex(t, all, batch, uc, streamConfig{threshold: 60, inverse: true})
		uc.Distance, uc.EF = "l2-squared", 64
		require.NoError(t, s.ix.UpdateUserConfig(uc, func() {}))
		for id := uint64(batch); id < total; id++ {
			require.NoError(t, s.insert(ctx, id, all[id]))
		}
		return s
	}
	docIDs := sequentialIDs(300, 100)

	one, each := build(), build()
	require.Equal(t, edgesByDoc(one), edgesByDoc(each), "the two indexes are built the same")
	got, err := one.refreshEdges(ctx, docIDs)
	require.NoError(t, err)

	var want refreshStats
	want.Work.InverseWrites = make([]int, each.neighbors+1)
	want.Work.InversePostings = make([]int, each.neighbors+1)
	for _, id := range docIDs {
		out, err := each.refreshEdges(ctx, []uint64{id})
		require.NoError(t, err)
		want.Refreshed += out.Refreshed
		want.SelfMissing += out.SelfMissing
		want.ListsChanged += out.ListsChanged
		want.EdgesBefore += out.EdgesBefore
		want.EdgesAfter += out.EdgesAfter
		want.EdgesKept += out.EdgesKept
		want.Work.InverseEdges += out.Work.InverseEdges
		want.Work.InverseChecks += out.Work.InverseChecks
		want.Work.InverseDistances += out.Work.InverseDistances
		want.Work.InverseAlready += out.Work.InverseAlready
		want.Work.SearchLoads += out.Work.SearchLoads
		want.Work.SearchRounds += out.Work.SearchRounds
		want.Work.SearchScanned += out.Work.SearchScanned
		want.Work.SearchEndpoints += out.Work.SearchEndpoints
		want.Work.SearchRescored += out.Work.SearchRescored
		for w, count := range out.Work.InverseWrites {
			want.Work.InverseWrites[w] += count
		}
		for w, count := range out.Work.InversePostings {
			want.Work.InversePostings[w] += count
		}
	}
	require.Equal(t, edgesByDoc(each), edgesByDoc(one), "one call or many, the same edges")
	assert.Equal(t, len(docIDs), got.Refreshed)
	assert.Positive(t, got.Time)
	got.Time, got.Work.SearchLoadsHist = 0, nil
	assert.Equal(t, want, got)
	assert.Positive(t, got.Work.InverseChecks)
	assert.Equal(t, got.Work.InverseEdges, func() int {
		sum := 0
		for w, count := range got.Work.InverseWrites {
			sum += w * count
		}
		return sum
	}())
}

// repeatedEdges counts the vectors with an endpoint twice among their edges,
// which checkCoverage lets through.
func TestStreamRepeatedEdges(t *testing.T) {
	all := clusteredVectors(1200, 24, 30, 53)
	s := newStreamTestIndex(t, all, 1200, smallConfig(), streamConfig{threshold: 60, inverse: true})
	require.Zero(t, s.repeatedEdges())

	tests := []struct {
		name    string
		repeats func(edges []uint32) []uint32
		want    int
	}{
		{name: "its edges as they are", repeats: func(edges []uint32) []uint32 { return edges }, want: 0},
		{name: "the first edge twice, together", repeats: func(edges []uint32) []uint32 { return append([]uint32{edges[0]}, edges...) }, want: 1},
		{name: "the first edge twice, apart", repeats: func(edges []uint32) []uint32 { return append(slices.Clone(edges), edges[0]) }, want: 1},
		{name: "two edges twice count once", repeats: func(edges []uint32) []uint32 { return append(slices.Clone(edges), edges[:2]...) }, want: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// A vector with two edges or more, and its list put back after.
			for p := range s.st.postings.lists {
				l := &s.st.postings.lists[p]
				for i := range l.docIDs {
					if edges := slices.Clone(l.edgesOf(i)); len(edges) >= 2 {
						l.setEdges(i, tt.repeats(edges))
						assert.Equal(t, tt.want, s.repeatedEdges())
						assert.NoError(t, s.checkCoverage(), "the coverage does not look for them")
						l.setEdges(i, edges)
						require.Zero(t, s.repeatedEdges())
						return
					}
				}
			}
			t.Fatal("no vector has two edges")
		})
	}
}

// A refresh on doubling takes the vectors whose edges were last built with
// half the vectors the index holds now, or fewer: the batch part once the
// index has doubled, and at each later doubling those refreshed the last time
// and those that entered up to then. An inverse edge that enters a list does
// not make it any younger.
func TestStreamStaleEdges(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name         string
		batch, total int
		inverse      bool
		// rounds are the sizes of the index at which the stale vectors are
		// refreshed, and want how many each round takes.
		rounds, want []int
	}{
		{name: "from a tenth, at each doubling", batch: 150, total: 1500, inverse: true, rounds: []int{300, 600, 1200}, want: []int{150, 300, 600}},
		{name: "the first inserts with the batch part, as in v2.4", batch: 198, total: 2000, inverse: true, rounds: []int{400, 800, 1600}, want: []int{200, 400, 800}},
		{name: "without inverse edges", batch: 150, total: 700, rounds: []int{300, 600}, want: []int{150, 300}},
		{name: "one vector short of the double", batch: 150, total: 400, inverse: true, rounds: []int{299}, want: []int{0}},
		{name: "a round that comes late", batch: 150, total: 800, inverse: true, rounds: []int{700}, want: []int{350}},
		{name: "the same size twice", batch: 150, total: 400, inverse: true, rounds: []int{300, 300}, want: []int{150, 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			all := clusteredVectors(tt.total, 24, 1, 211)
			uc := smallConfig()
			s := newStreamTestIndex(t, all, tt.batch, uc, streamConfig{threshold: 60, inverse: tt.inverse})
			uc.Distance, uc.EF = "l2-squared", 64
			require.NoError(t, s.ix.UpdateUserConfig(uc, func() {}))

			// Kept apart from the index: the size at which the edges of
			// every vector were last built.
			built := make([]int, tt.total)
			for id := range tt.batch {
				built[id] = tt.batch
			}
			check := func() {
				t.Helper()
				for id := range s.liveCount {
					require.Equalf(t, uint32(built[id]), s.builtAt[id], "doc %d", id)
				}
			}
			check()
			next := uint64(tt.batch)
			total := 0
			for i, size := range tt.rounds {
				for ; next < uint64(size); next++ {
					require.NoError(t, s.insert(ctx, next, all[next]))
					built[next] = int(next) + 1
				}
				require.Equal(t, size, s.liveCount)
				check()

				var want []uint64
				for id := range size {
					if 2*built[id] <= size {
						want = append(want, uint64(id))
					}
				}
				stale := s.staleEdges(sequentialIDs(size, 0))
				require.Equal(t, want, stale, "the stale vectors, in the order given")
				require.Len(t, stale, tt.want[i])
				total += len(stale)

				stats, err := s.refreshEdges(ctx, stale)
				require.NoError(t, err)
				require.Equal(t, len(stale), stats.Refreshed)
				if len(stale) > 0 {
					require.Positive(t, stats.EdgesAfter, "the refreshed vectors have neighbors in other postings")
				}
				for _, id := range stale {
					built[id] = size
				}
				check()
				require.Empty(t, s.staleEdges(sequentialIDs(size, 0)), "a refreshed vector is not stale")
			}
			for ; next < uint64(tt.total); next++ {
				require.NoError(t, s.insert(ctx, next, all[next]))
				built[next] = int(next) + 1
			}
			check()
			require.NoError(t, s.cleanCentroidGraph())
			require.NoError(t, s.checkCoverage())
			require.Zero(t, s.repeatedEdges())
			sum := 0
			for _, w := range tt.want {
				sum += w
			}
			require.Equal(t, sum, total)
		})
	}
}

// A snapshot of the edges puts back every list as it was, whatever was done
// to the edges since, and says so when the vectors are not where they were.
func TestStreamSnapshotRestoreEdges(t *testing.T) {
	const batch, upto, total = 1200, 2000, 2400
	ctx := context.Background()
	all := clusteredVectors(total, 24, 6, 397)

	tests := []struct {
		name string
		// change is what happens between the snapshot and its restore.
		change  func(t *testing.T, s *streamIndex)
		wantErr string
	}{
		{name: "nothing", change: func(*testing.T, *streamIndex) {}},
		{name: "a refresh of every vector", change: func(t *testing.T, s *streamIndex) {
			_, err := s.refreshEdges(ctx, sequentialIDs(upto, 0))
			require.NoError(t, err)
		}},
		{name: "a batch rebuild", change: func(t *testing.T, s *streamIndex) {
			_, err := s.rebuildEdges(ctx, testLogger(), 6, 64)
			require.NoError(t, err)
		}},
		{name: "no edges at all", change: func(_ *testing.T, s *streamIndex) {
			s.replaceEdges(func(uint64) []uint32 { return nil })
		}},
		{name: "an insert", wantErr: "snapshot", change: func(t *testing.T, s *streamIndex) {
			require.NoError(t, s.insert(ctx, upto, all[upto]))
		}},
		{name: "inserts that split postings", wantErr: "snapshot", change: func(t *testing.T, s *streamIndex) {
			for id := uint64(upto); id < total; id++ {
				require.NoError(t, s.insert(ctx, id, all[id]))
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uc := smallConfig()
			s := newStreamTestIndex(t, all, batch, uc, streamConfig{threshold: 60, inverse: true})
			uc.Distance, uc.EF = "l2-squared", 64
			require.NoError(t, s.ix.UpdateUserConfig(uc, func() {}))
			for id := uint64(batch); id < upto; id++ {
				require.NoError(t, s.insert(ctx, id, all[id]))
			}
			require.NoError(t, s.cleanCentroidGraph())

			want, wantBuilt, digest := edgesByDoc(s), slices.Clone(s.builtAt), s.edgesDigest()
			edges := 0
			for _, list := range want {
				edges += len(list)
			}
			require.Greater(t, edges, upto, "the vectors have edges to put back")
			snap := s.snapshotEdges()
			require.Equal(t, digest, snap.digest)
			tt.change(t, s)

			err := s.restoreEdges(snap)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, want, edgesByDoc(s))
			require.Equal(t, wantBuilt, s.builtAt)
			require.Equal(t, digest, s.edgesDigest())
			require.NoError(t, s.checkCoverage())

			// The index does not share its lists with the snapshot: it can be
			// restored again.
			_, err = s.refreshEdges(ctx, sequentialIDs(upto, 0))
			require.NoError(t, err)
			require.NoError(t, s.restoreEdges(snap))
			require.Equal(t, want, edgesByDoc(s))
		})
	}
}

// The digest of the edges tells two lists apart by any endpoint and by the
// order of the endpoints.
func TestStreamEdgesDigest(t *testing.T) {
	const batch = 1200
	all := clusteredVectors(batch, 24, 6, 401)
	s := newStreamTestIndex(t, all, batch, smallConfig(), streamConfig{threshold: 60, inverse: true})
	digest := s.edgesDigest()
	require.Equal(t, digest, s.edgesDigest())

	// A vector with two edges or more, and an endpoint its list lacks.
	var l *postingList
	member := -1
	for p := range s.st.postings.lists {
		l = &s.st.postings.lists[p]
		for i := range l.docIDs {
			if len(l.edgesOf(i)) >= 2 {
				member = i
				break
			}
		}
		if member >= 0 {
			break
		}
	}
	require.GreaterOrEqual(t, member, 0)
	edges := slices.Clone(l.edgesOf(member))

	tests := []struct {
		name   string
		change func(edges []uint32) []uint32
	}{
		{name: "two endpoints swapped", change: func(e []uint32) []uint32 { e[0], e[1] = e[1], e[0]; return e }},
		{name: "one endpoint replaced", change: func(e []uint32) []uint32 { e[0] = e[0] ^ 1; return e }},
		{name: "one endpoint dropped", change: func(e []uint32) []uint32 { return e[1:] }},
		{name: "every endpoint dropped", change: func([]uint32) []uint32 { return nil }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l.setEdges(member, tt.change(slices.Clone(edges)))
			require.NotEqual(t, digest, s.edgesDigest())
			l.setEdges(member, edges)
			require.Equal(t, digest, s.edgesDigest())
		})
	}
}

// replaceEdges gives every vector the list it is told, and incomingEdges
// counts the edges that end in every posting.
func TestStreamReplaceAndIncomingEdges(t *testing.T) {
	const batch, upto = 1200, 2200
	ctx := context.Background()
	all := clusteredVectors(upto, 24, 6, 433)
	uc := smallConfig()
	s := newStreamTestIndex(t, all, batch, uc, streamConfig{threshold: 60, inverse: true})
	uc.Distance, uc.EF = "l2-squared", 64
	require.NoError(t, s.ix.UpdateUserConfig(uc, func() {}))
	for id := uint64(batch); id < upto; id++ {
		require.NoError(t, s.insert(ctx, id, all[id]))
	}
	require.NoError(t, s.cleanCentroidGraph())

	count := func() []int {
		incoming := make([]int, len(s.st.postings.lists))
		for _, edges := range edgesByDoc(s) {
			for _, endpoint := range edges {
				incoming[s.st.side.postingOf[endpoint]]++
			}
		}
		return incoming
	}
	check := func() {
		t.Helper()
		incoming := s.incomingEdges()
		require.Equal(t, count(), incoming)
		total, edges := 0, 0
		for p, n := range incoming {
			total += n
			edges += len(s.st.postings.lists[p].edges)
			if s.retired[p] {
				require.Zerof(t, n, "retired posting %d", p)
			}
		}
		require.Equal(t, edges, total, "every edge ends in one posting")
		require.Greater(t, total, upto, "the vectors have edges to count")
	}
	check()

	grown := edgesByDoc(s)
	_, err := s.rebuildEdges(ctx, testLogger(), 6, 64)
	require.NoError(t, err)
	require.NotEqual(t, grown, edgesByDoc(s))
	check()

	s.replaceEdges(func(docID uint64) []uint32 { return grown[docID] })
	got := edgesByDoc(s)
	require.Len(t, got, len(grown))
	for docID, edges := range grown {
		// A vector without edges has none, as a nil list or an empty one.
		require.Truef(t, slices.Equal(edges, got[docID]), "doc %d: %v, not %v", docID, got[docID], edges)
	}
	require.NoError(t, s.checkCoverage())
	check()
}

// A sample followed through the batch rule and through a refresh tells, for
// every neighbor the rule asks for and a list lacks, where it was lost: in a
// posting the rule did not read, or in one it read, behind the candidates it
// compared exactly.
func TestWhySample(t *testing.T) {
	const batch, upto = 1200, 2200
	ctx := context.Background()
	all := clusteredVectors(upto, 24, 6, 457)

	tests := []struct {
		name    string
		inverse bool
		// ef and rescore are the search of the refresh, probe and rescue the
		// batch rule.
		ef, rescore, probe, rescue int
		// exhaustive: the search reads every posting it knows and compares
		// every candidate exactly, and so does the batch rule with those it
		// reads.
		exhaustive bool
	}{
		{name: "a search and a rule that compare all they read", inverse: true, ef: upto, rescore: upto, probe: 6, rescue: upto, exhaustive: true},
		{name: "a rule that reads every posting", inverse: true, ef: upto, rescore: upto, probe: 4096, rescue: upto, exhaustive: true},
		{name: "a narrow search and a narrow rule", inverse: true, ef: 8, rescore: 20, probe: 3, rescue: 20},
		{name: "the search of an insert", inverse: true, ef: 64, rescore: 64, probe: 16, rescue: 64},
		{name: "without inverse edges", ef: 64, rescore: 64, probe: 16, rescue: 64},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uc := smallConfig()
			s := newStreamTestIndex(t, all, batch, uc, streamConfig{threshold: 60, inverse: tt.inverse})
			uc.Distance, uc.EF = "l2-squared", 64
			require.NoError(t, s.ix.UpdateUserConfig(uc, func() {}))
			for id := uint64(batch); id < upto; id++ {
				require.NoError(t, s.insert(ctx, id, all[id]))
			}
			require.NoError(t, s.cleanCentroidGraph())
			require.NoError(t, s.checkCoverage())
			ids := sequentialIDs(upto, 0)
			postingOf := s.st.side.postingOf

			// As the cell does: the batch rule first, then the edges of the
			// cell put back and every vector refreshed.
			snap := s.snapshotEdges()
			incoming := map[string][]int{"final": s.incomingEdges()}
			probes := map[uint64][]uint32{}
			s.onRebuild = func(docID uint64, p []uint32) { probes[docID] = slices.Clone(p) }
			_, err := s.rebuildEdges(ctx, testLogger(), tt.probe, tt.rescue)
			require.NoError(t, err)
			s.onRebuild = nil
			require.Len(t, probes, upto)

			w, err := newWhySample(ctx, testLogger(), s, ids)
			require.NoError(t, err)
			for docID, p := range probes {
				w.probes(docID, p)
			}
			w.keep(func(v *whyVector) *[]uint32 { return &v.rebuilt })
			incoming["rebuilt"] = s.incomingEdges()
			rebuilt, err := s.edgeRecall(ctx, testLogger(), ids)
			require.NoError(t, err)
			require.NoError(t, s.restoreEdges(snap))

			uc.EF, uc.RescoreLimit = tt.ef, tt.rescore
			require.NoError(t, s.ix.UpdateUserConfig(uc, func() {}))
			own := map[uint64][]uint32{}
			s.traceRefresh = w.trace
			s.onRefresh = func(docID uint64, edges []uint32) {
				own[docID] = slices.Clone(edges)
				w.refreshedTo(docID, edges)
			}
			stats, err := s.refreshEdges(ctx, ids)
			require.NoError(t, err)
			s.traceRefresh, s.onRefresh = nil, nil
			require.Equal(t, upto, stats.Refreshed)
			w.keep(func(v *whyVector) *[]uint32 { return &v.refreshed })
			incoming["refreshed"] = s.incomingEdges()
			final := edgesByDoc(s)

			got, err := w.analyze(incoming)
			require.NoError(t, err)

			// Counted apart from the sample, from the lists themselves.
			var wanted, ownCaptured, inverseOut, inverseIn int
			for i := range w.vectors {
				v := &w.vectors[i]
				require.Equal(t, postingOf[v.docID], v.posting)
				require.Equal(t, own[v.docID], v.own, "the list the refresh of doc %d wrote", v.docID)
				require.Equal(t, final[v.docID], v.refreshed)
				require.Len(t, v.refreshed, len(v.own), "an inverse edge takes the place of another")
				for j, nb := range v.nearest {
					require.Equal(t, postingOf[nb.id], v.targets[j])
					bit := uint32(1) << j
					inOwn := slices.Contains(v.own, uint32(nb.id))
					if v.targets[j] == v.posting {
						require.NotZero(t, v.loaded&bit, "the posting of doc %d is where its search enters", v.docID)
						require.False(t, inOwn, "an edge of doc %d inside its posting", v.docID)
						continue
					}
					wanted++
					if inOwn {
						ownCaptured++
						require.NotZero(t, v.loaded&bit, "doc %d has an edge to a posting its search did not read", v.docID)
					}
					if v.loaded&bit != 0 {
						require.NotZero(t, v.known&bit, "the search of doc %d read a posting no edge led it to", v.docID)
					}
					if slices.Contains(v.rebuilt, uint32(nb.id)) {
						require.NotZero(t, v.probed&bit, "the batch rule gave doc %d an edge to a posting it did not read", v.docID)
					}
					if tt.exhaustive {
						// Read and compared exactly is found.
						require.Equal(t, v.loaded&bit != 0, inOwn, "doc %d, neighbor %d", v.docID, nb.id)
						require.Equal(t, v.known&bit != 0, v.loaded&bit != 0, "a search with no stop reads every posting it knows")
						require.Equal(t, v.probed&bit != 0, slices.Contains(v.rebuilt, uint32(nb.id)), "doc %d, neighbor %d", v.docID, nb.id)
					}
				}
				for _, endpoint := range v.own {
					if !slices.Contains(v.refreshed, endpoint) {
						inverseOut++
					}
				}
				for _, endpoint := range v.refreshed {
					if !slices.Contains(v.own, endpoint) {
						inverseIn++
					}
				}
			}

			require.Equal(t, upto, got.Sample)
			require.Equal(t, rebuilt.Wanted, wanted, "the neighbors the rule asks for, as the edge recall counts them")
			require.Equal(t, wanted, got.Wanted)
			require.Equal(t, wanted-ownCaptured, got.Refresh.Missed)
			require.Equal(t, rebuilt.Wanted-rebuilt.Captured, got.Batch.Missed)
			require.Equal(t, got.Refresh.Missed, got.Refresh.Unreached+got.Refresh.KnownNotRead+got.Refresh.Read)
			require.Equal(t, got.Batch.Missed, got.Batch.Unreached+got.Batch.Read)
			require.Zero(t, got.Batch.KnownNotRead)
			require.LessOrEqual(t, got.BothMissed, min(got.Refresh.Missed, got.Batch.Missed))
			require.Equal(t, inverseOut, got.InverseOut)
			require.Equal(t, inverseIn, got.InverseIn)
			require.Equal(t, got.InverseOut, got.InverseIn)
			require.LessOrEqual(t, got.InverseOutExact, got.InverseOut)
			require.LessOrEqual(t, got.InverseInExact, got.InverseIn)
			require.Positive(t, got.Wanted, "the vectors have neighbors in other postings")
			switch {
			case tt.inverse && !tt.exhaustive:
				// A vector whose search missed a neighbor gets it when that
				// neighbor is refreshed after it.
				require.Positive(t, stats.Work.InverseEdges)
				require.Positive(t, got.InverseIn, "inverse edges change lists their own refresh wrote")
			case tt.inverse:
				require.Positive(t, stats.Work.InverseEdges)
			default:
				require.Zero(t, got.InverseIn)
				refreshed, err := s.edgeRecall(ctx, testLogger(), ids)
				require.NoError(t, err)
				require.Equal(t, refreshed.Wanted-refreshed.Captured, got.Refresh.Missed)
				require.Zero(t, stats.Work.InverseEdges)
			}
			if tt.exhaustive {
				require.Zero(t, got.Refresh.Read)
				require.Zero(t, got.Refresh.KnownNotRead)
				require.Zero(t, got.Batch.Read)
			} else {
				require.Positive(t, got.Refresh.Read+got.Refresh.KnownNotRead+got.Refresh.Unreached)
				require.Positive(t, got.Batch.Read)
			}
			if tt.probe < 16 {
				require.Positive(t, got.Batch.Unreached, "a rule that reads %d postings", tt.probe)
			}
			if tt.probe >= len(s.st.postings.lists) {
				require.Zero(t, got.Batch.Missed, "a rule that reads everything and compares it exactly")
			}

			live := 0
			for p := range s.st.postings.lists {
				if !s.retired[p] {
					live++
				}
			}
			for _, name := range []string{"final", "rebuilt", "refreshed"} {
				edges := 0
				for _, n := range incoming[name] {
					edges += n
				}
				require.InDelta(t, float64(edges)/float64(live), got.Incoming[name].Mean, 1e-6)
				for _, key := range []string{name, name + "PerMember"} {
					fifths := got.Fifths[key]
					require.Len(t, fifths, 5, key)
					var sum whyFifth
					for i, f := range fifths {
						require.LessOrEqual(t, f.From, f.To)
						if i > 0 {
							require.LessOrEqual(t, fifths[i-1].To, f.From)
						}
						sum.Wanted += f.Wanted
						sum.RefreshMissed += f.RefreshMissed
						sum.BatchMissed += f.BatchMissed
					}
					require.Equal(t, got.Wanted, sum.Wanted)
					require.Equal(t, got.Refresh.Missed, sum.RefreshMissed)
					require.Equal(t, got.Batch.Missed, sum.BatchMissed)
				}
			}
		})
	}
}

// A sample is read once every vector of it went through both rules.
func TestWhySampleErrors(t *testing.T) {
	const batch = 1200
	ctx := context.Background()
	all := clusteredVectors(batch+1, 24, 6, 461)
	s := newStreamTestIndex(t, all, batch, smallConfig(), streamConfig{threshold: 60, inverse: true})

	_, err := newWhySample(ctx, testLogger(), s, []uint64{3, 7, 3})
	require.ErrorContains(t, err, "doc 3 is in the sample twice")
	_, err = newWhySample(ctx, testLogger(), s, []uint64{batch})
	require.ErrorContains(t, err, "is not live")

	w, err := newWhySample(ctx, testLogger(), s, []uint64{3, 7})
	require.NoError(t, err)
	require.Nil(t, w.trace(5), "a vector outside the sample is not traced")
	_, err = w.analyze(nil)
	require.ErrorContains(t, err, "was not followed")
	for _, docID := range []uint64{3, 7} {
		w.probes(docID, nil)
		require.NotNil(t, w.trace(docID))
	}
	_, err = w.analyze(map[string][]int{"final": {1, 2}})
	require.ErrorContains(t, err, "incoming edges")
	got, err := w.analyze(map[string][]int{"final": s.incomingEdges()})
	require.NoError(t, err)
	require.Equal(t, 2, got.Sample)
	// Followed through nothing: every neighbor the rule asks for is missed,
	// in a posting no rule read.
	require.Equal(t, got.Wanted, got.Refresh.Missed)
	require.Equal(t, got.Wanted, got.Refresh.Unreached)
	require.Equal(t, got.Wanted, got.Batch.Unreached)
	require.Equal(t, got.Wanted, got.BothMissed)
}

func TestFifthsBy(t *testing.T) {
	tests := []struct {
		name           string
		keys           []float64
		refresh, batch []bool
		want           []whyFifth
	}{
		{
			name:    "ten neighbors, out of order",
			keys:    []float64{9, 1, 5, 3, 7, 2, 10, 4, 8, 6},
			refresh: []bool{false, true, false, true, false, true, false, false, false, false},
			batch:   []bool{true, false, false, false, false, false, true, false, false, true},
			want: []whyFifth{
				{From: 1, To: 2, Wanted: 2, RefreshMissed: 2},
				{From: 3, To: 4, Wanted: 2, RefreshMissed: 1},
				{From: 5, To: 6, Wanted: 2, BatchMissed: 1},
				{From: 7, To: 8, Wanted: 2},
				{From: 9, To: 10, Wanted: 2, BatchMissed: 2},
			},
		},
		{
			name:    "equal keys stay in the order given",
			keys:    []float64{4, 4, 4, 4, 4},
			refresh: []bool{true, false, false, false, true},
			batch:   []bool{false, true, false, false, false},
			want: []whyFifth{
				{From: 4, To: 4, Wanted: 1, RefreshMissed: 1},
				{From: 4, To: 4, Wanted: 1, BatchMissed: 1},
				{From: 4, To: 4, Wanted: 1},
				{From: 4, To: 4, Wanted: 1},
				{From: 4, To: 4, Wanted: 1, RefreshMissed: 1},
			},
		},
		{
			name:    "fewer neighbors than parts",
			keys:    []float64{2, 1, 3},
			refresh: []bool{true, true, false},
			batch:   []bool{false, false, false},
			want: []whyFifth{
				{},
				{From: 1, To: 1, Wanted: 1, RefreshMissed: 1},
				{},
				{From: 2, To: 2, Wanted: 1, RefreshMissed: 1},
				{From: 3, To: 3, Wanted: 1},
			},
		},
		{name: "none", want: make([]whyFifth, 5)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, fifthsBy(tt.keys, tt.refresh, tt.batch))
		})
	}
}

func TestSpreadOf(t *testing.T) {
	hundred := make([]float64, 100)
	for i := range hundred {
		hundred[i] = float64(100 - i)
	}
	tests := []struct {
		name   string
		values []float64
		want   whySpread
	}{
		{name: "none"},
		{name: "one", values: []float64{7}, want: whySpread{Min: 7, P1: 7, P5: 7, P10: 7, P25: 7, P50: 7, P75: 7, P90: 7, P99: 7, Max: 7, Mean: 7}},
		{
			name: "one to a hundred, out of order", values: hundred,
			want: whySpread{Min: 1, P1: 2, P5: 6, P10: 11, P25: 26, P50: 51, P75: 76, P90: 91, P99: 100, Max: 100, Mean: 50.5},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, spreadOf(tt.values))
		})
	}
}
