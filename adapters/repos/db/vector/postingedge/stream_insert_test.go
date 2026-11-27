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

// The tests here are the small tests of the streamIndex simulator
// (stream_test.go). The tests of the streaming experiments it ran, which need
// thousands of vectors, are in stream_insert_harness_test.go (build tag
// postingedgebench).
//
// They run the simulator at half the posting size of the experiments:
// postings of 20 that split past 30, where those have postings of 40 that
// split past 60, and half their edges. A hundred vectors then make several
// postings and their splits.
const streamPostingSize, streamThreshold, streamEdges = 20, 30, 8

// streamEF is the ef of the searches of the inserts, where the experiments
// have 64 over corpora ten times as large.
const streamEF = 24

// streamTestConfig is smallConfig with postings of streamPostingSize and
// streamEdges edges a vector.
func streamTestConfig() UserConfig {
	uc := smallConfig()
	uc.TargetPostingSize = streamPostingSize
	uc.Edges = streamEdges
	return uc
}

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
	// Every search rescores on one goroutine. The fan-out of the rescore
	// starts up to twice GOMAXPROCS goroutines per search, which under -race
	// costs more than the search itself, and these tests are about what the
	// inserts write. TestRescoreLeavesOutVectorsThatAreGone covers the
	// fan-out.
	index.rescoreConcurrency = 1
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

// Every cell of the experiment keeps the coverage invariant through every
// insert and split, keeps the edges of every vector nearest first and outside
// nothing it should not reach, and does what it says: the principal cell
// leaves old edges alone, inverse edges keep the degree of old vectors, the
// repair searches the edges of split postings again, and the assignment-only
// cell gives the new vectors no edges.
func TestStreamInserts(t *testing.T) {
	t.Parallel()
	const batch, total = 60, 120
	ctx := context.Background()
	all := clusteredVectors(total, 16, 2, 77)
	provider := distancer.NewL2SquaredProvider()

	tests := []struct {
		name string
		cfg  streamConfig
	}{
		{name: "principal", cfg: streamConfig{threshold: streamThreshold}},
		{name: "inverse edges", cfg: streamConfig{threshold: streamThreshold, inverse: true}},
		{name: "edge repair", cfg: streamConfig{threshold: streamThreshold, repair: true}},
		{name: "assignment only", cfg: streamConfig{threshold: streamThreshold, assignOnly: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			uc := streamTestConfig()
			s := newStreamTestIndex(t, all, batch, uc, tt.cfg)
			require.NoError(t, s.checkCoverage())
			uc.Distance, uc.EF = "l2-squared", streamEF
			require.NoError(t, s.ix.UpdateUserConfig(uc, func() {}))
			old := edgesByDoc(s)

			for id := uint64(batch); id < total; id++ {
				require.NoError(t, s.insert(ctx, id, all[id]))
				if id%23 == 0 {
					require.NoError(t, s.checkCoverage(), "after doc %d", id)
				}
			}
			require.NoError(t, s.checkCoverage())
			require.NoError(t, s.cleanCentroidGraph())
			require.NoError(t, s.checkCoverage(), "after the centroid graph was cleaned")

			assert.Equal(t, total, s.liveCount)
			assert.Equal(t, total-batch, s.stats.Inserted)
			assert.Positive(t, s.stats.Splits, "a threshold of 30 over postings of 20 splits")
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
	t.Parallel()
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
	t.Parallel()
	const n = 60
	all := clusteredVectors(n+10, 16, 1, 5)
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
			t.Parallel()
			s := newStreamTestIndex(t, all, n, streamTestConfig(), streamConfig{threshold: 2 * streamPostingSize})
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

// The guard of the edge builder is called once per insert, right after the
// vector is written and before anything else changes, and counts it against
// the live vectors of that moment as an independent count does.
func TestStreamEdgeRecallAtInsert(t *testing.T) {
	t.Parallel()
	const batch, total = 60, 120
	ctx := context.Background()
	// One blob, where the index stays navigable through the splits. In tight
	// clusters of two postings each, the halves of a split have no edge to
	// each other and no path through other postings: neither the inserts'
	// search nor the repair's can reach the other half.
	all := clusteredVectors(total, 16, 1, 61)
	for _, cfg := range []streamConfig{
		{threshold: streamThreshold, inverse: true, siblings: true},
	} {
		t.Run(fmt.Sprintf("inverse %v repair %v siblings %v", cfg.inverse, cfg.repair, cfg.siblings), func(t *testing.T) {
			t.Parallel()
			s := newStreamTestIndex(t, all, batch, streamTestConfig(), cfg)
			uc := streamTestConfig()
			uc.Distance, uc.EF = "l2-squared", streamEF
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

// A counting provider computes what the provider it wraps computes, and
// counts one distance for each: by SingleDist and by the distancers it hands
// out, which is how the centroid graph reaches it.
func TestCountingProvider(t *testing.T) {
	t.Parallel()
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

// repeatedEdges counts the vectors with an endpoint twice among their edges,
// which checkCoverage lets through.
func TestStreamRepeatedEdges(t *testing.T) {
	t.Parallel()
	all := clusteredVectors(60, 16, 1, 53)
	s := newStreamTestIndex(t, all, len(all), streamTestConfig(), streamConfig{threshold: streamThreshold, inverse: true})
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

// The digest of the edges tells two lists apart by any endpoint and by the
// order of the endpoints.
func TestStreamEdgesDigest(t *testing.T) {
	t.Parallel()
	const batch = 60
	all := clusteredVectors(batch, 16, 1, 401)
	s := newStreamTestIndex(t, all, batch, streamTestConfig(), streamConfig{threshold: streamThreshold, inverse: true})
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

// A sample is read once every vector of it went through both rules.
func TestWhySampleErrors(t *testing.T) {
	t.Parallel()
	const batch = 60
	ctx := context.Background()
	all := clusteredVectors(batch+1, 16, 1, 461)
	s := newStreamTestIndex(t, all, batch, streamTestConfig(), streamConfig{threshold: streamThreshold, inverse: true})

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

// The degree by cohort counts every live vector once, in the cohort of the
// place it entered the index at, with the edges it has now.
func TestStreamDegreeByCohort(t *testing.T) {
	t.Parallel()
	const batch, total = 60, 120
	ctx := context.Background()
	all := clusteredVectors(total, 16, 2, 947)
	s := newStreamTestIndex(t, all, batch, streamTestConfig(), streamConfig{threshold: streamThreshold, inverse: true})

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
	const upto = 100
	for _, id := range order[batch:upto] {
		require.NoError(t, s.insert(ctx, id, all[id]))
	}
	require.NoError(t, s.checkCoverage())
	edges := edgesByDoc(s)

	tests := []struct {
		name   string
		bounds []int
	}{
		{name: "the batch part and two stretches", bounds: []int{0, batch, 80, upto}},
		{name: "the batch part and the inserts", bounds: []int{0, batch, upto}},
		{name: "past what is live", bounds: []int{0, batch, upto, total}},
		{name: "the batch part alone", bounds: []int{0, batch}},
		{name: "one stretch in the middle", bounds: []int{70, 90}},
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

// A rebuilt centroid graph holds the live centroids and only them, under
// their posting ids, finds them, and serves the inserts that follow. The
// postings and their edges are not touched.
func TestStreamRebuildCentroidGraph(t *testing.T) {
	t.Parallel()
	const batch, upto, total = 60, 100, 120
	ctx := context.Background()
	all := clusteredVectors(total, 16, 2, 271)
	uc := streamTestConfig()
	s := newStreamTestIndex(t, all, batch, uc, streamConfig{threshold: streamThreshold, inverse: true})
	uc.Distance, uc.EF = "l2-squared", streamEF
	require.NoError(t, s.ix.UpdateUserConfig(uc, func() {}))
	for id := uint64(batch); id < upto; id++ {
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

	for id := uint64(upto); id < total; id++ {
		require.NoError(t, s.insert(ctx, id, all[id]))
	}
	require.NoError(t, s.checkCoverage())
	assert.Equal(t, total, s.liveCount)
}

// replaceEdges gives every vector the list it is told, and incomingEdges
// counts the edges that end in every posting.
func TestStreamReplaceAndIncomingEdges(t *testing.T) {
	t.Parallel()
	const batch, upto = 60, 120
	ctx := context.Background()
	all := clusteredVectors(upto, 16, 2, 433)
	uc := streamTestConfig()
	s := newStreamTestIndex(t, all, batch, uc, streamConfig{threshold: streamThreshold, inverse: true})
	uc.Distance, uc.EF = "l2-squared", streamEF
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
	t.Parallel()
	const batch, upto = 80, 140
	ctx := context.Background()
	all := clusteredVectors(upto, 16, 2, 457)

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
		// The cell that reaches every count; the others are in
		// TestWhySampleAtScale (stream_insert_harness_test.go).
		{name: "a narrow search and a narrow rule", inverse: true, ef: 8, rescore: 20, probe: 2, rescue: 20},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			uc := streamTestConfig()
			s := newStreamTestIndex(t, all, batch, uc, streamConfig{threshold: streamThreshold, inverse: tt.inverse})
			uc.Distance, uc.EF = "l2-squared", streamEF
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
			if tt.probe < 6 {
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

func TestFifthsBy(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
