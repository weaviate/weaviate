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

package hnsw

import (
	"context"
	"math/bits"
	"sort"
	"time"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
)

// FilteredScanConfig parameterizes the three-stage filtered prefix scan.
type FilteredScanConfig struct {
	// PrefixWords is the number of 64-bit code words stage 1 reads per
	// allowlist member. The default 7 makes header word + prefix exactly one
	// 64-byte cache line (8 + 7*8 = 64 B): 448 prefix bits at 768 dims.
	PrefixWords int
	// Budget1 is the stage-1 survivor budget (bucketed best-B selection by
	// prefix Hamming distance). The default 4096 tracks the wiki-dpr rank
	// curve's p95 at 448 bits (~6.1k corpus-wide is an upper bound for every
	// allowlist; 4096 is the round number below it — validated, not
	// trusted, by the measurement harness).
	Budget1 int
	// Budget2 is the stage-2 survivor budget after full-width corrected
	// re-ranking. Default 700 ≈ the full-width rank curve's p95 (712).
	Budget2 int
	// FloatsForID returns the full-precision vector for stage-3 exact
	// rescoring, typically a view into an mmapped corpus. The scan does not
	// use the index's own float path: this round measures the scan against
	// disk-resident floats without a full import.
	FloatsForID func(id uint64) []float32
}

func (c *FilteredScanConfig) withDefaults() FilteredScanConfig {
	out := *c
	if out.PrefixWords <= 0 {
		out.PrefixWords = 7
	}
	if out.Budget1 <= 0 {
		out.Budget1 = 4096
	}
	if out.Budget2 <= 0 {
		out.Budget2 = 700
	}
	return out
}

// FilteredScanStats reports per-stage cost of one scan. Bytes are counted
// at 64-byte cache-line granularity (the x86 line; halve line counts for
// Apple's 128-byte lines — at 768 dims the full record is one 128 B line,
// so stages 1 and 2 differ in compute only on M-series).
type FilteredScanStats struct {
	Members    int // allowlist size
	Survivors1 int
	Survivors2 int

	AllowIter time.Duration // cost of iterating the allowlist alone
	Stage1    time.Duration
	Stage2    time.Duration
	Stage3    time.Duration

	Stage1Bytes int64 // one line per member
	Stage2Bytes int64 // full record lines per survivor
	Stage3Bytes int64 // float row bytes per rescored candidate
}

// FilteredScanScratch holds the reusable buffers of the scan. One scratch
// per goroutine; the scan is single-threaded this round.
type FilteredScanScratch struct {
	buckets  [][]uint32 // per-Hamming-distance survivor buckets
	surv     []uint64
	stage2   []scanCand
	topK     []scanCand
	initOnce bool
}

type scanCand struct {
	id   uint64
	dist float32
}

// NewFilteredScanScratch allocates scratch for the given config.
func NewFilteredScanScratch(cfg FilteredScanConfig) *FilteredScanScratch {
	c := cfg.withDefaults()
	maxD := c.PrefixWords*64 + 1
	s := &FilteredScanScratch{
		buckets: make([][]uint32, maxD),
		surv:    make([]uint64, 0, c.Budget1+64),
		stage2:  make([]scanCand, 0, c.Budget1+64),
		topK:    make([]scanCand, 0, c.Budget2+1),
	}
	for i := range s.buckets {
		s.buckets[i] = make([]uint32, 0, 32)
	}
	s.initOnce = true
	return s
}

// FilteredPrefixScan runs the three-stage filtered scan over the members of
// allow:
//
//	stage 1: prefix Hamming over the first PrefixWords code words (one cache
//	         line per member including the header), bucketed best-Budget1
//	         selection. The statistic is the plain sign-bit Hamming distance
//	         of the CENTERED codes — the same statistic the rank curves
//	         that size Budget1 were measured with; the per-record
//	         corrections (step, ⟨μ,x⟩) enter in stage 2.
//	stage 2: full-width corrected multi-bit distance (the centered rq1
//	         estimator) over survivors, best-Budget2.
//	stage 3: exact float rescore of the Budget2 finalists via
//	         cfg.FloatsForID, returning the top k ids and distances.
//
// Requires the index to be compressed with the centered 1-bit quantizer.
func (h *hnsw) FilteredPrefixScan(ctx context.Context, query []float32, k int,
	allow helpers.AllowList, cfg FilteredScanConfig, scratch *FilteredScanScratch,
) ([]uint64, []float32, FilteredScanStats, error) {
	var stats FilteredScanStats
	c := cfg.withDefaults()
	if scratch == nil || !scratch.initOnce {
		scratch = NewFilteredScanScratch(c)
	}
	if c.FloatsForID == nil {
		return nil, nil, stats, errors.New("filtered scan requires cfg.FloatsForID for stage-3 rescoring")
	}
	if !h.compressed.Load() {
		return nil, nil, stats, errors.New("filtered scan requires a compressed index")
	}
	source, ok := h.compressor.(compressionhelpers.WordCodeSource)
	if !ok {
		return nil, nil, stats, errors.Errorf("compressor %T does not expose word codes", h.compressor)
	}
	rq, ok := source.ScanQuantizer().(*compressionhelpers.CenteredBinaryRotationalQuantizer)
	if !ok {
		return nil, nil, stats, errors.Errorf("filtered scan requires the centered 1-bit quantizer, got %T", source.ScanQuantizer())
	}

	if h.distancerProvider.Type() == "cosine-dot" {
		query = h.normalizeVec(query)
	}

	// Stage-1 buckets store ids as uint32 to halve their memory; scan
	// corpora stay well below that, enforced once here.
	if !allow.IsEmpty() && allow.Max() > 1<<32-1 {
		return nil, nil, stats, errors.New("filtered scan supports ids < 2^32")
	}

	// The allowlist iteration cost is measured on its own: at millions of
	// members the bitmap walk itself is a real term.
	iterStart := time.Now()
	it := allow.Iterator()
	for _, ok := it.Next(); ok; _, ok = it.Next() {
	}
	it.Stop()
	stats.AllowIter = time.Since(iterStart)
	stats.Members = allow.Len()

	// Stage 1: prefix Hamming, bucketed best-B1 with an adapting threshold.
	queryCode := rq.Encode(query) // sign words of the centered query
	if avail := len(queryCode) - 1; c.PrefixWords > avail {
		// narrow codes (low dims) have fewer bit words than the default
		// one-line prefix; the prefix can never exceed the code
		c.PrefixWords = avail
	}
	prefixQ := queryCode[1 : 1+c.PrefixWords]
	maxD := c.PrefixWords * 64
	if len(scratch.buckets) < maxD+1 {
		// scratch was sized for a narrower prefix; rebuild
		scratch = NewFilteredScanScratch(c)
	}
	buckets := scratch.buckets
	for i := 0; i <= maxD; i++ {
		buckets[i] = buckets[i][:0]
	}
	worst := maxD // highest bucket currently admissible
	count := 0

	s1Start := time.Now()
	it = allow.Iterator()
	for id, ok := it.Next(); ok; id, ok = it.Next() {
		code, err := source.WordCode(ctx, id)
		if err != nil {
			// ids in the allowlist but not in the vector index (deleted or
			// never vectorized) are skipped, matching filtered graph search
			continue
		}
		var d int
		words := code[1 : 1+c.PrefixWords]
		for w := 0; w < c.PrefixWords; w++ {
			d += bits.OnesCount64(words[w] ^ prefixQ[w])
		}
		if d > worst {
			continue
		}
		buckets[d] = append(buckets[d], uint32(id))
		count++
		if count > c.Budget1 {
			// evict one from the worst bucket; tighten the threshold when
			// it drains
			b := buckets[worst]
			for len(b) == 0 {
				worst--
				b = buckets[worst]
			}
			buckets[worst] = b[:len(b)-1]
			count--
			for len(buckets[worst]) == 0 && worst > 0 {
				worst--
			}
		}
	}
	it.Stop()
	stats.Stage1 = time.Since(s1Start)
	stats.Stage1Bytes = int64(stats.Members) * 64

	surv := scratch.surv[:0]
	for d := 0; d <= worst && len(surv) < count; d++ {
		for _, id := range buckets[d] {
			surv = append(surv, uint64(id))
		}
	}
	stats.Survivors1 = len(surv)

	// Stage 2: full-width corrected distance over survivors.
	s2Start := time.Now()
	distancer := rq.NewDistancer(query)
	stage2 := scratch.stage2[:0]
	var recordWords int
	for _, id := range surv {
		code, err := source.WordCode(ctx, id)
		if err != nil {
			continue
		}
		recordWords = len(code)
		dist, err := distancer.Distance(code)
		if err != nil {
			return nil, nil, stats, err
		}
		stage2 = append(stage2, scanCand{id: id, dist: dist})
	}
	b2 := c.Budget2
	if b2 > len(stage2) {
		b2 = len(stage2)
	}
	// B1 is a few thousand: a full sort is cheap and branch-predictable.
	sort.Slice(stage2, func(i, j int) bool { return stage2[i].dist < stage2[j].dist })
	stage2 = stage2[:b2]
	stats.Stage2 = time.Since(s2Start)
	recordLines := int64((recordWords*8 + 63) / 64)
	stats.Stage2Bytes = int64(stats.Survivors1) * recordLines * 64
	stats.Survivors2 = len(stage2)

	// Stage 3: exact rescore from disk-resident floats.
	s3Start := time.Now()
	topK := scratch.topK[:0]
	for _, cand := range stage2 {
		vec := c.FloatsForID(cand.id)
		if vec == nil {
			continue
		}
		exact, err := h.distancerProvider.SingleDist(query, vec)
		if err != nil {
			return nil, nil, stats, err
		}
		topK = append(topK, scanCand{id: cand.id, dist: exact})
	}
	sort.Slice(topK, func(i, j int) bool { return topK[i].dist < topK[j].dist })
	if k > len(topK) {
		k = len(topK)
	}
	topK = topK[:k]
	stats.Stage3 = time.Since(s3Start)
	stats.Stage3Bytes = int64(stats.Survivors2) * int64(len(query)) * 4

	ids := make([]uint64, k)
	dists := make([]float32, k)
	for i, cand := range topK {
		ids[i] = cand.id
		dists[i] = cand.dist
	}
	scratch.surv = surv[:0]
	scratch.stage2 = stage2[:0]
	scratch.topK = topK[:0]
	return ids, dists, stats, nil
}
