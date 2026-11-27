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
	"cmp"
	"context"
	"slices"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// The informative part of v2.4 (bench-report.md, "v2.4: refresco al doblar,
// en línea"): why the edges of the batch rule search better than those of a
// refresh. A sample of vectors is followed through both: its exact nearest
// neighbors, the list its refresh writes with what the search of that refresh
// read, and the list the batch rule gives it with the postings that rule
// read. A neighbor the rule asks for and a list lacks is a miss, and the
// three hypotheses are read on where the misses are.

// whyVector is one sampled vector.
type whyVector struct {
	docID   uint64
	posting uint32
	// nearest are its exact nearest neighbors among the live vectors, itself
	// left out, nearest first, and targets the postings they are in. Those
	// outside its own posting are the edges the rule asks for.
	nearest []scored
	targets []uint32
	// loaded and known have bit j set when the search of its refresh read
	// the posting of nearest[j], and when an edge that search followed led
	// to that posting. probed has it set when the batch rule read it.
	loaded, known, probed uint32
	// own is the list its refresh wrote, refreshed its list once every
	// vector was refreshed, and rebuilt the list of the batch rule.
	own, refreshed, rebuilt []uint32
	traced, probedSet       bool
}

// whySample follows a sample of the live vectors of an index.
type whySample struct {
	s       *streamIndex
	vectors []whyVector
	at      map[uint64]int
	// mask has, by posting, the bits of the neighbors of the vector being
	// traced that are in it; active is that vector, -1 for none.
	mask   []uint32
	active int
}

// newWhySample takes the exact nearest neighbors of the given live vectors,
// by brute force. The vectors of the index must stay where they are from
// here on: no insert and no split.
func newWhySample(ctx context.Context, logger logrus.FieldLogger, s *streamIndex, docIDs []uint64) (*whySample, error) {
	if s.neighbors > 32 {
		return nil, errors.Errorf("a sample follows up to 32 neighbors per vector, the index keeps %d", s.neighbors)
	}
	w := &whySample{
		s: s, vectors: make([]whyVector, len(docIDs)), at: make(map[uint64]int, len(docIDs)),
		mask: make([]uint32, len(s.st.postings.lists)), active: -1,
	}
	for i, docID := range docIDs {
		if _, twice := w.at[docID]; twice {
			return nil, errors.Errorf("doc %d is in the sample twice", docID)
		}
		w.at[docID] = i
	}
	postingOf := s.st.side.postingOf
	err := parallelChunks(ctx, logger, len(docIDs), 1, func(_, start, end int) error {
		var nearest topK
		for i := start; i < end; i++ {
			docID := docIDs[i]
			if docID >= uint64(len(s.live)) || !s.live[docID] {
				return errors.Errorf("doc %d is not live", docID)
			}
			found, err := s.nearestLive(docID, &nearest)
			if err != nil {
				return err
			}
			v := &w.vectors[i]
			v.docID, v.posting = docID, postingOf[docID]
			v.nearest = slices.Clone(found)
			v.targets = make([]uint32, len(found))
			for j, nb := range found {
				v.targets[j] = postingOf[nb.id]
			}
		}
		return nil
	})
	return w, err
}

// trace is streamIndex.traceRefresh: for a sampled vector, the hook that
// notes which postings of its neighbors the search of its refresh reads, and
// to which of them an edge it follows leads.
func (w *whySample) trace(docID uint64) func(sc *searchScratch, post *posting, cut float32) {
	i, sampled := w.at[docID]
	if !sampled {
		return nil
	}
	if w.active >= 0 {
		for _, q := range w.vectors[w.active].targets {
			w.mask[q] = 0
		}
	}
	w.active = i
	v := &w.vectors[i]
	v.loaded, v.known, v.traced = 0, 0, true
	for j, q := range v.targets {
		w.mask[q] |= 1 << j
	}
	postingOf := w.s.st.side.postingOf
	return func(sc *searchScratch, post *posting, cut float32) {
		// Every posting of a round is marked before any of it is loaded.
		for _, p := range sc.touched {
			v.loaded |= w.mask[p]
		}
		// The sources, as collectEndpoints takes them.
		for m, dist := range sc.memberDist {
			if dist > cut {
				continue
			}
			for _, endpoint := range post.edgesOf(m) {
				v.known |= w.mask[postingOf[endpoint]]
			}
		}
	}
}

// refreshedTo is streamIndex.onRefresh: it keeps the list the refresh of a
// sampled vector wrote.
func (w *whySample) refreshedTo(docID uint64, edges []uint32) {
	if i, sampled := w.at[docID]; sampled {
		w.vectors[i].own = slices.Clone(edges)
	}
}

// probes is streamIndex.onRebuild: it notes which postings of the neighbors
// of a sampled vector the batch rule reads.
func (w *whySample) probes(docID uint64, probes []uint32) {
	i, sampled := w.at[docID]
	if !sampled {
		return
	}
	v := &w.vectors[i]
	v.probed, v.probedSet = 0, true
	for j, q := range v.targets {
		if slices.Contains(probes, q) {
			v.probed |= 1 << j
		}
	}
}

// keep copies the list every sampled vector has now to where into points.
func (w *whySample) keep(into func(v *whyVector) *[]uint32) {
	for i := range w.vectors {
		v := &w.vectors[i]
		l := &w.s.st.postings.lists[v.posting]
		*into(v) = slices.Clone(l.edgesOf(l.member(v.docID)))
	}
}

// whyMisses parts the neighbors a rule asks for and its list lacks by where
// the rule lost them.
type whyMisses struct {
	Missed int `json:"missed"`
	// Unreached: in a posting the rule had no way to. For a refresh, one no
	// edge its search followed led to. For the batch rule, one outside the
	// postings it reads.
	Unreached int `json:"unreached"`
	// KnownNotRead: for a refresh, in a posting an edge its search followed
	// led to, and that the search stopped before reading. The batch rule has
	// none.
	KnownNotRead int `json:"knownNotRead"`
	// Read: in a posting the rule read. The neighbor was ranked by its code
	// and left out of the candidates compared exactly.
	Read int `json:"read"`
}

// whyFifth is one of the five parts of the neighbors the rule asks for,
// ordered by a count of the posting they are in.
type whyFifth struct {
	// From and To are the least and the greatest count in the part.
	From          float64 `json:"from"`
	To            float64 `json:"to"`
	Wanted        int     `json:"wanted"`
	RefreshMissed int     `json:"refreshMissed"`
	BatchMissed   int     `json:"batchMissed"`
}

// whySpread is how a count spreads over the live postings.
type whySpread struct {
	Min  float64 `json:"min"`
	P1   float64 `json:"p1"`
	P5   float64 `json:"p5"`
	P10  float64 `json:"p10"`
	P25  float64 `json:"p25"`
	P50  float64 `json:"p50"`
	P75  float64 `json:"p75"`
	P90  float64 `json:"p90"`
	P99  float64 `json:"p99"`
	Max  float64 `json:"max"`
	Mean float64 `json:"mean"`
}

// whyAnalysis is what the sample tells about the three hypotheses.
type whyAnalysis struct {
	Sample int `json:"sample"`
	// Wanted counts the neighbors the rule asks for over the sample: of the
	// exact nearest ones, those outside the vector's posting.
	Wanted int `json:"wanted"`
	// Refresh and Batch are the misses of the list each refresh wrote and of
	// the list of the batch rule, by cause (hypothesis 1). BothMissed counts
	// the neighbors both lack.
	Refresh    whyMisses `json:"refresh"`
	Batch      whyMisses `json:"batch"`
	BothMissed int       `json:"bothMissed"`
	// What the inverse edges of the other refreshes did to the lists of the
	// sample afterwards (hypothesis 2): the edges they took out and those
	// they put in, and how many of each are exact nearest neighbors.
	InverseOut      int `json:"inverseOut"`
	InverseOutExact int `json:"inverseOutExact"`
	InverseIn       int `json:"inverseIn"`
	InverseInExact  int `json:"inverseInExact"`
	// Fifths parts the wanted neighbors in five by the incoming edges of
	// their posting (hypothesis 3), counted in the index named by the key,
	// and "<key>PerMember" by the incoming edges per member. Incoming is how
	// those counts spread over the postings.
	Fifths   map[string][]whyFifth `json:"fifths"`
	Incoming map[string]whySpread  `json:"incoming"`
}

// spreadOf summarizes the values, which it sorts.
func spreadOf(values []float64) whySpread {
	if len(values) == 0 {
		return whySpread{}
	}
	slices.Sort(values)
	at := func(q float64) float64 {
		return values[min(len(values)-1, int(q*float64(len(values))))]
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return whySpread{
		Min: values[0], P1: at(0.01), P5: at(0.05), P10: at(0.10), P25: at(0.25), P50: at(0.50),
		P75: at(0.75), P90: at(0.90), P99: at(0.99), Max: values[len(values)-1], Mean: sum / float64(len(values)),
	}
}

// fifthsBy orders the wanted neighbors by their key, least first, parts them
// in five of equal size, and counts in each part the neighbors each rule
// missed. Equal keys stay in the order given.
func fifthsBy(keys []float64, refreshMissed, batchMissed []bool) []whyFifth {
	order := make([]int, len(keys))
	for i := range order {
		order[i] = i
	}
	slices.SortStableFunc(order, func(a, b int) int { return cmp.Compare(keys[a], keys[b]) })
	const parts = 5
	out := make([]whyFifth, parts)
	for part := range out {
		from, to := part*len(order)/parts, (part+1)*len(order)/parts
		f := &out[part]
		for at, i := range order[from:to] {
			if at == 0 {
				f.From = keys[i]
			}
			f.To = keys[i]
			f.Wanted++
			if refreshMissed[i] {
				f.RefreshMissed++
			}
			if batchMissed[i] {
				f.BatchMissed++
			}
		}
	}
	return out
}

// analyze reads the sample once every vector has been traced through its
// refresh and through the batch rule, and its three lists are kept. incoming
// holds, by a name, the incoming edges of every posting in one state of the
// index.
func (w *whySample) analyze(incoming map[string][]int) (*whyAnalysis, error) {
	s := w.s
	out := &whyAnalysis{
		Sample: len(w.vectors), Fifths: map[string][]whyFifth{}, Incoming: map[string]whySpread{},
	}
	// One entry per wanted neighbor, over the whole sample.
	var target []uint32
	var refreshMissed, batchMissed []bool
	for i := range w.vectors {
		v := &w.vectors[i]
		if !v.traced || !v.probedSet {
			return nil, errors.Errorf("doc %d was not followed through its refresh and through the batch rule", v.docID)
		}
		exact := func(endpoint uint32) bool {
			return slices.ContainsFunc(v.nearest, func(nb scored) bool { return nb.id == uint64(endpoint) })
		}
		for j, nb := range v.nearest {
			if v.targets[j] == v.posting {
				continue
			}
			out.Wanted++
			bit := uint32(1) << j
			byRefresh := !slices.Contains(v.own, uint32(nb.id))
			byBatch := !slices.Contains(v.rebuilt, uint32(nb.id))
			target = append(target, v.targets[j])
			refreshMissed = append(refreshMissed, byRefresh)
			batchMissed = append(batchMissed, byBatch)
			if byRefresh {
				out.Refresh.Missed++
				switch {
				case v.loaded&bit != 0:
					out.Refresh.Read++
				case v.known&bit != 0:
					out.Refresh.KnownNotRead++
				default:
					out.Refresh.Unreached++
				}
			}
			if byBatch {
				out.Batch.Missed++
				if v.probed&bit != 0 {
					out.Batch.Read++
				} else {
					out.Batch.Unreached++
				}
			}
			if byRefresh && byBatch {
				out.BothMissed++
			}
		}
		for _, endpoint := range v.own {
			if !slices.Contains(v.refreshed, endpoint) {
				out.InverseOut++
				if exact(endpoint) {
					out.InverseOutExact++
				}
			}
		}
		for _, endpoint := range v.refreshed {
			if !slices.Contains(v.own, endpoint) {
				out.InverseIn++
				if exact(endpoint) {
					out.InverseInExact++
				}
			}
		}
	}

	lists := s.st.postings.lists
	for name, counts := range incoming {
		if len(counts) != len(lists) {
			return nil, errors.Errorf("the incoming edges %q are of %d postings, the index holds %d", name, len(counts), len(lists))
		}
		total, perMember := make([]float64, len(target)), make([]float64, len(target))
		for i, q := range target {
			total[i] = float64(counts[q])
			perMember[i] = float64(counts[q]) / float64(max(len(lists[q].docIDs), 1))
		}
		out.Fifths[name] = fifthsBy(total, refreshMissed, batchMissed)
		out.Fifths[name+"PerMember"] = fifthsBy(perMember, refreshMissed, batchMissed)

		var all, allPerMember []float64
		for p := range lists {
			if s.retired[p] {
				continue
			}
			all = append(all, float64(counts[p]))
			allPerMember = append(allPerMember, float64(counts[p])/float64(max(len(lists[p].docIDs), 1)))
		}
		out.Incoming[name] = spreadOf(all)
		out.Incoming[name+"PerMember"] = spreadOf(allPerMember)
	}
	return out, nil
}
