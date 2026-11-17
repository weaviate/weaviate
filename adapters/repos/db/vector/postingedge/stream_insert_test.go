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
